//! PostgreSQL join candidate recognition and path installation.

use std::ptr;

use lagodb_query::plan::JoinType;
use pgrx::pg_sys;

use super::candidate::SingleRelationCandidate;
use super::path_installation::QueryPathInstallation;
use super::query_shape::QueryShape;
use super::relation_plan::RelationPlanBuilder;
use super::relation_tree::RelationTreePlanner;
use crate::gucs::{QueryOffloadMode, query_offload_mode};
use crate::query_host::error::QueryHostError;

pub(super) struct JoinCandidate {
    root: *mut pg_sys::PlannerInfo,
    join_rel: *mut pg_sys::RelOptInfo,
    outer_rel: *mut pg_sys::RelOptInfo,
    inner_rel: *mut pg_sys::RelOptInfo,
    path_target: *mut pg_sys::PathTarget,
    join_type: JoinType,
    restrict_list: *mut pg_sys::List,
    parameter_info: *mut pg_sys::ParamPathInfo,
    rows: f64,
}

impl JoinCandidate {
    pub(super) unsafe fn inspect(
        root: *mut pg_sys::PlannerInfo,
        join_rel: *mut pg_sys::RelOptInfo,
        outer_rel: *mut pg_sys::RelOptInfo,
        inner_rel: *mut pg_sys::RelOptInfo,
        join_type: pg_sys::JoinType::Type,
        extra: *mut pg_sys::JoinPathExtraData,
    ) -> Option<Self> {
        if query_offload_mode() == QueryOffloadMode::Off {
            return None;
        }
        // Uncorrelated nested scopes are imported into the outer relation tree,
        // so their standard paths must remain visible. A correlated/LATERAL
        // scope cannot be lifted; it instead owns this serial Join CustomPath
        // and rebinds PARAM_EXEC values on each relevant PostgreSQL rescan.
        if unsafe { !(*root).parent_root.is_null() && (*root).plan_params.is_null() }
        {
            return None;
        }
        let join_type = match join_type {
            pg_sys::JoinType::JOIN_INNER => JoinType::Inner,
            pg_sys::JoinType::JOIN_LEFT => JoinType::Left,
            pg_sys::JoinType::JOIN_RIGHT => JoinType::Right,
            pg_sys::JoinType::JOIN_FULL => JoinType::Full,
            pg_sys::JoinType::JOIN_SEMI => JoinType::LeftSemi,
            pg_sys::JoinType::JOIN_ANTI => JoinType::LeftAnti,
            // PostgreSQL also presents right-oriented physical alternatives;
            // their canonical left-oriented candidate is planned separately.
            _ => return None,
        };
        let planner = unsafe { &*root };
        let parse = unsafe { &*planner.parse };
        let relation = unsafe { &*join_rel };
        if !unsafe { Self::covers_complete_query(planner, relation) }
            || !unsafe {
                Self::target_references_only_relation(
                    root,
                    relation.reltarget,
                    relation.relids,
                )
            }
        {
            return None;
        }
        let (parameter_info, rows) = if relation.lateral_relids.is_null() {
            (ptr::null_mut(), relation.rows)
        } else {
            let parameter_info = unsafe {
                Self::minimum_parameterization(join_rel, relation.lateral_relids)
            }?;
            (parameter_info, unsafe { (*parameter_info).ppi_rows })
        };
        if !QueryShape::new(parse).supports_join()
            // Publish one path for the complete relation subtree. Installing
            // Force-cost paths on intermediate JOINRELs can make add_path()
            // discard the standard child JoinPaths needed for recursive
            // reconstruction at the final level. The sole exception is a
            // complete parameterized LATERAL subtree: its own relids plus its
            // mandatory outer relids must cover the query exactly.
            || !unsafe {
                SingleRelationCandidate::expression_list_is_safe(
                    root,
                    (*(*join_rel).reltarget).exprs,
                )
            }
        {
            return None;
        }
        let restrict_list = unsafe { (*extra).restrictlist };
        let count = unsafe { pg_sys::list_length(restrict_list) };
        for index in 0..count {
            let restriction = unsafe { pg_sys::list_nth(restrict_list, index) }
                .cast::<pg_sys::RestrictInfo>();
            if unsafe { (*restriction).pseudoconstant }
                || !unsafe {
                    SingleRelationCandidate::expression_is_safe(
                        root,
                        (*restriction).clause.cast(),
                    )
                }
            {
                return None;
            }
        }
        Some(Self {
            root,
            join_rel,
            outer_rel,
            inner_rel,
            path_target: unsafe { (*join_rel).reltarget },
            join_type,
            restrict_list,
            parameter_info,
            rows,
        })
    }

    pub(super) unsafe fn plan(self) -> Result<(), QueryHostError> {
        let Some(relation_tree) = (unsafe {
            RelationTreePlanner::build_join(
                self.root,
                self.join_rel,
                self.outer_rel,
                self.inner_rel,
                self.join_type,
                self.restrict_list,
                self.rows,
            )
        }) else {
            return Ok(());
        };
        let mut builder =
            RelationPlanBuilder::new(self.root, self.path_target, relation_tree);
        let Some(planned) = (unsafe { builder.build() }) else {
            return Ok(());
        };
        QueryPathInstallation::new(
            self.join_rel,
            self.path_target,
            planned,
            self.rows,
        )
        .with_parameter_info(self.parameter_info)
        .install()
    }

    unsafe fn covers_complete_query(
        planner: &pg_sys::PlannerInfo,
        relation: &pg_sys::RelOptInfo,
    ) -> bool {
        if !unsafe { pg_sys::bms_is_subset(relation.relids, planner.all_query_rels) }
            || !unsafe {
                pg_sys::bms_is_subset(relation.lateral_relids, planner.all_query_rels)
            }
        {
            return false;
        }
        let mut member = -1;
        loop {
            member =
                unsafe { pg_sys::bms_next_member(planner.all_query_rels, member) };
            if member < 0 {
                return true;
            }
            if !unsafe { pg_sys::bms_is_member(member, relation.relids) }
                && !unsafe { pg_sys::bms_is_member(member, relation.lateral_relids) }
            {
                return false;
            }
        }
    }

    unsafe fn target_references_only_relation(
        root: *mut pg_sys::PlannerInfo,
        target: *mut pg_sys::PathTarget,
        relids: *mut pg_sys::Bitmapset,
    ) -> bool {
        let expressions = unsafe { (*target).exprs };
        let count = unsafe { pg_sys::list_length(expressions) };
        for index in 0..count {
            let expression = unsafe { pg_sys::list_nth(expressions, index) };
            let referenced = unsafe { pg_sys::pull_varnos(root, expression.cast()) };
            let valid = unsafe { pg_sys::bms_is_subset(referenced, relids) };
            unsafe { pg_sys::bms_free(referenced) };
            if !valid {
                // create_customscan_plan intentionally does not replace outer
                // Vars in custom_scan_tlist, so such a projection cannot use
                // the parameterized CustomScan contract.
                return false;
            }
        }
        true
    }

    unsafe fn minimum_parameterization(
        relation: *mut pg_sys::RelOptInfo,
        required_outer: *mut pg_sys::Bitmapset,
    ) -> Option<*mut pg_sys::ParamPathInfo> {
        let paths = unsafe { (*relation).pathlist };
        let count = unsafe { pg_sys::list_length(paths) };
        let mut selected = None;
        for index in 0..count {
            let path =
                unsafe { pg_sys::list_nth(paths, index) }.cast::<pg_sys::Path>();
            let parameter_info = unsafe { (*path).param_info };
            if parameter_info.is_null()
                || !unsafe {
                    pg_sys::bms_equal((*parameter_info).ppi_req_outer, required_outer)
                }
            {
                continue;
            }
            let total_cost = unsafe { (*path).total_cost };
            if selected.is_none_or(|(_, best_cost)| total_cost < best_cost) {
                selected = Some((parameter_info, total_cost));
            }
        }
        selected.map(|(parameter_info, _)| parameter_info)
    }
}
