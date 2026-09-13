//! Shared PostgreSQL relation-tree extraction for query offload.

mod costing;
mod equivalence;
mod node;
mod selected_path;
mod subplan;

use std::ptr;

use lagodb_core::query_contract::ScanId;
use pgrx::pg_sys;

use super::candidate::{ScannableRelation, SingleRelationCandidate};
use super::expression::{ExpressionSourceCatalog, QueryExpressionPlanner};
pub(super) use node::PlannedRelationInput;
use node::RelationNode;

pub(super) struct PlannedRelationTree {
    input: PlannedRelationInput,
    expressions: QueryExpressionPlanner,
}

impl PlannedRelationTree {
    pub(super) fn contains_join(&self) -> bool {
        self.input.root.contains_join()
    }

    pub(super) fn into_parts(self) -> (PlannedRelationInput, QueryExpressionPlanner) {
        (self.input, self.expressions)
    }
}

/// One PostgreSQL PlannerInfo namespace. RTIs are dense inside this scope but
/// can repeat in another scope belonging to a lifted SubPlan.
struct PlannerRelationScope {
    root: *mut pg_sys::PlannerInfo,
    relations_by_rti: Box<[Option<RelationBinding>]>,
}

/// Origin needed for planner-scope-local equivalence-class inspection.
#[derive(Clone, Copy)]
struct ScanOrigin {
    root: *mut pg_sys::PlannerInfo,
    rti: pg_sys::Index,
}

/// Reconstructs PostgreSQL relation trees into the provider-neutral form used
/// by ordinary Join and Aggregate planning. ScanIds are global to the complete
/// tree; RTIs are resolved only together with their PlannerInfo scope.
pub(super) struct RelationTreePlanner {
    scopes: Vec<PlannerRelationScope>,
    scan_origins: Vec<ScanOrigin>,
    expressions: QueryExpressionPlanner,
}

#[derive(Clone, Copy)]
pub(super) struct RelationBinding {
    pub(super) scan: ScanId,
    pub(super) relation: ScannableRelation,
}

impl RelationTreePlanner {
    pub(super) unsafe fn build_base(
        root: *mut pg_sys::PlannerInfo,
        relation: *mut pg_sys::RelOptInfo,
        rti: pg_sys::Index,
        allow_mark: bool,
    ) -> Option<PlannedRelationTree> {
        let mut planner = unsafe {
            Self::for_rtis(root, vec![rti], (*relation).relids, ptr::null_mut())
        }?;
        let relation_root =
            unsafe { planner.build_base_subtree(root, relation, rti, allow_mark) }?;
        Some(planner.finish(relation_root))
    }

    pub(super) unsafe fn build_upper_input(
        root: *mut pg_sys::PlannerInfo,
        input_rel: *mut pg_sys::RelOptInfo,
    ) -> Option<PlannedRelationTree> {
        let planner = unsafe { &*root };
        let input = unsafe { &*input_rel };
        if !input.lateral_relids.is_null()
            || unsafe { !pg_sys::bms_equal(input.relids, planner.all_query_rels) }
        {
            return None;
        }
        let rtis = unsafe { Self::collect_base_relids(root, input.relids) }?;
        if unsafe { pg_sys::bms_num_members(planner.all_baserels) } as usize
            != rtis.len()
        {
            return None;
        }
        let mut planner =
            unsafe { Self::for_rtis(root, rtis, input.relids, ptr::null_mut()) }?;
        let relation_root = match input.reloptkind {
            pg_sys::RelOptKind::RELOPT_BASEREL => unsafe {
                planner.build_base_subtree(root, input_rel, input.relid, false)
            }?,
            pg_sys::RelOptKind::RELOPT_JOINREL => {
                unsafe { planner.build_selected_subtree(root, input_rel, false) }?
            }
            _ => return None,
        };
        Some(planner.finish(relation_root))
    }

    unsafe fn for_rtis(
        root: *mut pg_sys::PlannerInfo,
        rtis: Vec<pg_sys::Index>,
        local_relids: *mut pg_sys::Bitmapset,
        runtime_outer_relids: *mut pg_sys::Bitmapset,
    ) -> Option<Self> {
        let mut mappings = Vec::with_capacity(rtis.len());
        let scope = unsafe {
            Self::build_scope(
                root,
                rtis,
                local_relids,
                runtime_outer_relids,
                0,
                &mut mappings,
            )
        }?;
        let scan_origins = mappings
            .iter()
            .map(|&(rti, _)| ScanOrigin { root, rti })
            .collect();
        let sources = ExpressionSourceCatalog::for_relations(
            root,
            &mappings,
            runtime_outer_relids,
        )?;
        Some(Self {
            scopes: vec![scope],
            scan_origins,
            expressions: QueryExpressionPlanner::new(sources),
        })
    }

    unsafe fn add_scope(
        &mut self,
        root: *mut pg_sys::PlannerInfo,
        rtis: Vec<pg_sys::Index>,
    ) -> Option<()> {
        if self.scopes.iter().any(|scope| scope.root == root) {
            return None;
        }
        let start = self.scan_origins.len();
        let mut mappings = Vec::with_capacity(rtis.len());
        let scope = unsafe {
            Self::build_scope(
                root,
                rtis,
                (*root).all_baserels,
                ptr::null_mut(),
                start,
                &mut mappings,
            )
        }?;
        self.expressions.add_source_relations(root, &mappings)?;
        self.scan_origins
            .extend(mappings.iter().map(|&(rti, _)| ScanOrigin { root, rti }));
        self.scopes.push(scope);
        Some(())
    }

    unsafe fn build_scope(
        root: *mut pg_sys::PlannerInfo,
        rtis: Vec<pg_sys::Index>,
        local_relids: *mut pg_sys::Bitmapset,
        runtime_outer_relids: *mut pg_sys::Bitmapset,
        scan_start: usize,
        mappings: &mut Vec<(pg_sys::Index, ScanId)>,
    ) -> Option<PlannerRelationScope> {
        let planner = unsafe { &*root };
        let mut relations_by_rti = vec![None; planner.simple_rel_array_size as usize];
        for (offset, rti) in rtis.into_iter().enumerate() {
            let relation = unsafe { *planner.simple_rel_array.add(rti as usize) };
            let rte = unsafe { *planner.simple_rte_array.add(rti as usize) };
            if relation.is_null()
                || rte.is_null()
                || !unsafe {
                    SingleRelationCandidate::restrictions_are_safe(relation)
                }
            {
                return None;
            }
            let relation = unsafe {
                ScannableRelation::inspect(
                    relation,
                    rti,
                    rte,
                    local_relids,
                    runtime_outer_relids,
                )
            }?;
            let scan = ScanId::from_index(scan_start.checked_add(offset)?);
            relations_by_rti[rti as usize] = Some(RelationBinding { scan, relation });
            mappings.push((rti, scan));
        }
        Some(PlannerRelationScope {
            root,
            relations_by_rti: relations_by_rti.into_boxed_slice(),
        })
    }

    fn finish(self, root: RelationNode) -> PlannedRelationTree {
        PlannedRelationTree {
            input: PlannedRelationInput {
                root,
                scan_count: self.scan_origins.len(),
            },
            expressions: self.expressions,
        }
    }

    fn binding(
        &self,
        root: *mut pg_sys::PlannerInfo,
        rti: pg_sys::Index,
    ) -> Option<RelationBinding> {
        self.scopes
            .iter()
            .find(|scope| scope.root == root)?
            .relations_by_rti
            .get(rti as usize)
            .copied()
            .flatten()
    }

    fn scan_origin(&self, scan: ScanId) -> Option<ScanOrigin> {
        self.scan_origins.get(scan.index()).copied()
    }

    unsafe fn build_base_subtree(
        &mut self,
        root: *mut pg_sys::PlannerInfo,
        relation: *mut pg_sys::RelOptInfo,
        rti: pg_sys::Index,
        allow_mark: bool,
    ) -> Option<RelationNode> {
        let binding = self.binding(root, rti)?;
        if binding.relation.input_rel != relation {
            return None;
        }
        unsafe { self.build_scan_with_restrictions(root, binding, allow_mark) }
    }
}
