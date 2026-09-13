//! Base-restriction classification and exact SubPlan lifting.

mod extraction;

use std::ptr;

use lagodb_query::plan::JoinType;
use pgrx::pg_sys;

use super::node::{RelationScan, RelationScanFilter};
use super::{RelationBinding, RelationNode, RelationTreePlanner};
use crate::query_host::planning::candidate::SingleRelationCandidate;
use crate::query_host::planning::expression::{ExpressionScope, PredicateDomain};

#[derive(Clone, Copy)]
struct LiftedSubPlan {
    restriction: *mut pg_sys::RestrictInfo,
    subplan: *mut pg_sys::SubPlan,
    inner_root: *mut pg_sys::PlannerInfo,
    anti: bool,
}

#[derive(Clone, Copy)]
struct LiftedMarkSubPlan {
    restriction: *mut pg_sys::RestrictInfo,
    subplan: *mut pg_sys::SubPlan,
    inner_root: *mut pg_sys::PlannerInfo,
    null_varno: pg_sys::Index,
    null_attno: pg_sys::AttrNumber,
    anti: bool,
}

struct ClassifiedRestrictions {
    ordinary_clauses: *mut pg_sys::List,
    ordinary_restrictions: *mut pg_sys::List,
    top_level: Vec<LiftedSubPlan>,
    mark: Vec<LiftedMarkSubPlan>,
}

impl ClassifiedRestrictions {
    fn new() -> Self {
        Self {
            ordinary_clauses: ptr::null_mut(),
            ordinary_restrictions: ptr::null_mut(),
            top_level: Vec::new(),
            mark: Vec::new(),
        }
    }
}

impl RelationTreePlanner {
    pub(super) unsafe fn build_scan_with_restrictions(
        &mut self,
        root: *mut pg_sys::PlannerInfo,
        binding: RelationBinding,
        allow_mark: bool,
    ) -> Option<RelationNode> {
        let restrictions = unsafe {
            Self::classify_restrictions(
                root,
                (*binding.relation.input_rel).baserestrictinfo,
            )
        }?;
        if !allow_mark && !restrictions.mark.is_empty() {
            return None;
        }

        let has_lifted =
            !restrictions.top_level.is_empty() || !restrictions.mark.is_empty();
        let scan_rows = if has_lifted {
            unsafe {
                Self::estimate_scan_rows(
                    root,
                    binding,
                    restrictions.ordinary_restrictions,
                )
            }
        } else {
            unsafe { (*binding.relation.input_rel).rows }
        };
        let filter = if restrictions.ordinary_clauses.is_null() {
            None
        } else {
            let exact = unsafe {
                self.expressions.lower(
                    restrictions.ordinary_clauses.cast(),
                    ExpressionScope::predicate(root, PredicateDomain::Exact),
                )
            }
            .ok()?;
            Some(RelationScanFilter {
                exact,
                source: restrictions.ordinary_clauses.cast(),
            })
        };
        let mut current = RelationNode::Scan(RelationScan {
            root,
            scan: binding.scan,
            relation: binding.relation,
            filter,
            estimated_rows: scan_rows,
        });

        for lifted in restrictions.top_level {
            current = unsafe { self.wrap_top_level_subplan(root, current, lifted) }?;
        }
        for lifted in restrictions.mark {
            current = unsafe { self.wrap_mark_subplan(root, current, lifted) }?;
        }
        Some(current)
    }

    unsafe fn classify_restrictions(
        root: *mut pg_sys::PlannerInfo,
        restrictions: *mut pg_sys::List,
    ) -> Option<ClassifiedRestrictions> {
        let mut classified = ClassifiedRestrictions::new();
        let count = unsafe { pg_sys::list_length(restrictions) };
        for index in 0..count {
            let restriction = unsafe { pg_sys::list_nth(restrictions, index) }
                .cast::<pg_sys::RestrictInfo>();
            let clause = unsafe { (*restriction).clause.cast::<pg_sys::Node>() };
            if let Some((subplan, anti, inner_root)) =
                unsafe { Self::extract_subplan(root, clause) }
            {
                classified.top_level.push(LiftedSubPlan {
                    restriction,
                    subplan,
                    inner_root,
                    anti,
                });
                continue;
            }
            let or_clause = if unsafe { (*restriction).orclause.is_null() } {
                clause
            } else {
                unsafe { (*restriction).orclause.cast() }
            };
            if let Some(mark) =
                unsafe { Self::extract_mark_subplan(root, restriction, or_clause) }
            {
                classified.mark.push(mark);
                continue;
            }
            // An unrecognized SubPlan cannot be left for PostgreSQL after the
            // containing base relation has been replaced by this CustomScan.
            if unsafe { pg_sys::contain_subplans(clause) } {
                return None;
            }
            if !unsafe { SingleRelationCandidate::expression_is_safe(root, clause) } {
                return None;
            }
            classified.ordinary_clauses = unsafe {
                pg_sys::lappend(classified.ordinary_clauses, clause.cast())
            };
            classified.ordinary_restrictions = unsafe {
                pg_sys::lappend(classified.ordinary_restrictions, restriction.cast())
            };
        }
        Some(classified)
    }

    unsafe fn wrap_top_level_subplan(
        &mut self,
        outer_root: *mut pg_sys::PlannerInfo,
        left: RelationNode,
        lifted: LiftedSubPlan,
    ) -> Option<RelationNode> {
        let (right, inner_output) = unsafe {
            self.build_subplan_input(lifted.subplan, lifted.inner_root, false)
        }?;
        let test_expression = unsafe { (*lifted.subplan).testexpr };
        let is_in = !test_expression.is_null();
        let mut keys = Vec::new();
        if is_in {
            let inner_output = inner_output?;
            let key = unsafe {
                self.expressions.lower_subplan_join_key(
                    outer_root,
                    lifted.inner_root,
                    lifted.subplan,
                    inner_output,
                )
            }
            .ok()?;
            let key = unsafe { self.orient_join_key(&left, &right, key) }?;
            keys.push(key);
        }
        let join_type = if lifted.anti {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };
        let null_aware = lifted.anti && is_in;
        if null_aware && keys.len() != 1 {
            return None;
        }
        let input_rows = left.estimated_rows();
        let estimated_rows = unsafe {
            Self::estimate_restriction_rows(
                outer_root,
                lifted.restriction,
                input_rows,
            )
        };
        let estimated_key_rows =
            unsafe { pg_sys::clamp_row_est(input_rows * right.estimated_rows()) };
        Some(RelationNode::Join {
            join_type,
            left: Box::new(left),
            right: Box::new(right),
            keys,
            on_filters: Vec::new(),
            null_aware,
            mark_filter: None,
            estimated_key_rows,
            estimated_rows,
        })
    }

    unsafe fn wrap_mark_subplan(
        &mut self,
        outer_root: *mut pg_sys::PlannerInfo,
        left: RelationNode,
        lifted: LiftedMarkSubPlan,
    ) -> Option<RelationNode> {
        let (right, inner_output) = unsafe {
            self.build_subplan_input(lifted.subplan, lifted.inner_root, true)
        }?;
        let key = unsafe {
            self.expressions.lower_subplan_join_key(
                outer_root,
                lifted.inner_root,
                lifted.subplan,
                inner_output?,
            )
        }
        .ok()?;
        let key = unsafe { self.orient_join_key(&left, &right, key) }?;
        if key.left().scan != self.binding(outer_root, lifted.null_varno)?.scan
            || key.left().attno != lifted.null_attno
        {
            return None;
        }
        let input_rows = left.estimated_rows();
        let estimated_rows = unsafe {
            Self::estimate_restriction_rows(
                outer_root,
                lifted.restriction,
                input_rows,
            )
        };
        let estimated_key_rows =
            unsafe { pg_sys::clamp_row_est(input_rows * right.estimated_rows()) };
        Some(RelationNode::Join {
            join_type: JoinType::LeftMark,
            left: Box::new(left),
            right: Box::new(right),
            keys: vec![key],
            on_filters: Vec::new(),
            null_aware: false,
            mark_filter: Some(super::node::RelationMarkFilter {
                null_test: key.left(),
                anti: lifted.anti,
            }),
            estimated_key_rows,
            estimated_rows,
        })
    }

    unsafe fn build_subplan_input(
        &mut self,
        subplan: *mut pg_sys::SubPlan,
        inner_root: *mut pg_sys::PlannerInfo,
        allow_mark: bool,
    ) -> Option<(RelationNode, Option<*mut pg_sys::Expr>)> {
        if subplan.is_null()
            || inner_root.is_null()
            || unsafe { !(*subplan).parParam.is_null() }
            || !unsafe { Self::subquery_shape_is_supported(inner_root) }
        {
            return None;
        }
        let inner_relation = unsafe { Self::find_final_relation(inner_root) }?;
        let rtis = unsafe {
            Self::collect_base_relids(inner_root, (*inner_root).all_query_rels)
        }?;
        if rtis.is_empty() {
            return None;
        }
        unsafe { self.add_scope(inner_root, rtis) }?;
        let inner = unsafe {
            self.build_selected_subtree(inner_root, inner_relation, allow_mark)
        }?;
        let output = if unsafe { (*subplan).testexpr.is_null() } {
            None
        } else {
            Some(unsafe { Self::subplan_output(inner_root) }?)
        };
        Some((inner, output))
    }

    unsafe fn estimate_scan_rows(
        root: *mut pg_sys::PlannerInfo,
        binding: RelationBinding,
        ordinary: *mut pg_sys::List,
    ) -> f64 {
        let source_rows = unsafe { (*binding.relation.input_rel).tuples };
        if ordinary.is_null() {
            return unsafe { pg_sys::clamp_row_est(source_rows) };
        }
        let selectivity = unsafe {
            pg_sys::clauselist_selectivity(
                root,
                ordinary,
                binding.relation.range_table_index as i32,
                pg_sys::JoinType::JOIN_INNER,
                ptr::null_mut(),
            )
        };
        unsafe { pg_sys::clamp_row_est(source_rows * selectivity) }
    }

    unsafe fn estimate_restriction_rows(
        root: *mut pg_sys::PlannerInfo,
        restriction: *mut pg_sys::RestrictInfo,
        input_rows: f64,
    ) -> f64 {
        let selectivity = unsafe {
            pg_sys::clause_selectivity(
                root,
                restriction.cast(),
                0,
                pg_sys::JoinType::JOIN_INNER,
                ptr::null_mut(),
            )
        };
        unsafe { pg_sys::clamp_row_est(input_rows * selectivity) }
    }
}
