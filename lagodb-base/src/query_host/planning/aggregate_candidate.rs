//! Unified Aggregate/Having candidate over a shared relation tree.

use std::ptr;

use pgrx::pg_sys;

use super::aggregate_plan::AggregatePlanBuilder;
use super::candidate::SingleRelationCandidate;
use super::path_installation::QueryPathInstallation;
use super::query_shape::QueryShape;
use super::relation_tree::RelationTreePlanner;
use crate::gucs::{QueryOffloadMode, query_offload_mode};
use crate::query_host::error::QueryHostError;

pub(super) struct AggregateCandidate;

impl AggregateCandidate {
    pub(super) unsafe fn install(
        root: *mut pg_sys::PlannerInfo,
        stage: pg_sys::UpperRelationKind::Type,
        input_rel: *mut pg_sys::RelOptInfo,
        output_rel: *mut pg_sys::RelOptInfo,
    ) -> Result<bool, QueryHostError> {
        if query_offload_mode() == QueryOffloadMode::Off
            || stage != pg_sys::UpperRelationKind::UPPERREL_GROUP_AGG
        {
            return Ok(false);
        }
        let parse = unsafe { &*(*root).parse };
        if !QueryShape::new(parse).supports_aggregate()
            || !unsafe {
                SingleRelationCandidate::target_list_is_safe(root, parse.targetList)
            }
            || !unsafe {
                SingleRelationCandidate::expression_list_is_safe(
                    root,
                    (*(*output_rel).reltarget).exprs,
                )
            }
            || !unsafe {
                SingleRelationCandidate::expression_is_safe(root, parse.havingQual)
            }
        {
            return Ok(false);
        }
        let Some(relation_tree) =
            (unsafe { RelationTreePlanner::build_upper_input(root, input_rel) })
        else {
            return Ok(false);
        };
        let input_rows = unsafe { (*input_rel).rows };
        let aggregate_rows = unsafe { Self::estimate_group_rows(root, input_rows) };
        let output_rows = unsafe { Self::first_path_rows(output_rel) };
        let path_target = unsafe { (*output_rel).reltarget };
        // Never rewrite an aggregate over an outer join into separate
        // per-source aggregates. NULL extension is produced by the shared
        // JoinNode first, so COUNT(*) and COUNT(non_preserved.column) retain
        // their distinct PostgreSQL meanings.
        let mut builder = AggregatePlanBuilder::over_relation_tree(
            root,
            path_target,
            relation_tree,
            aggregate_rows,
            output_rows,
        );
        let Some(planned) = (unsafe { builder.build() }) else {
            return Ok(false);
        };
        QueryPathInstallation::new(output_rel, path_target, planned, output_rows)
            .install()?;
        Ok(true)
    }

    unsafe fn estimate_group_rows(root: *mut pg_sys::PlannerInfo, rows: f64) -> f64 {
        if unsafe { (*root).processed_groupClause.is_null() } {
            return 1.0;
        }
        let groups = unsafe {
            pg_sys::get_sortgrouplist_exprs(
                (*root).processed_groupClause,
                (*(*root).parse).targetList,
            )
        };
        unsafe {
            pg_sys::estimate_num_groups(
                root,
                groups,
                rows,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        }
    }

    unsafe fn first_path_rows(relation: *mut pg_sys::RelOptInfo) -> f64 {
        let path = unsafe { pg_sys::list_nth((*relation).pathlist, 0) }
            .cast::<pg_sys::Path>();
        unsafe { (*path).rows }
    }
}
