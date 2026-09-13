//! Query-offload recognition and CustomPath materialization.

use std::ffi::c_void;

mod aggregate_candidate;
mod aggregate_plan;
mod candidate;
mod distinct_plan;
mod expression;
mod join_candidate;
mod materialize;
mod path_installation;
mod planned_query;
mod provider_scan_planner;
mod query_shape;
mod relation_plan;
mod relation_tree;
mod subplan_candidate;
mod table_scan_filter;
mod upper_plan;

use pgrx::pg_sys;

use super::error::QueryHostError;

pub(super) use materialize::plan_custom_path;

pub(super) unsafe fn set_rel_pathlist(
    root: *mut pg_sys::PlannerInfo,
    relation: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) -> Result<(), QueryHostError> {
    let Some(candidate) = (unsafe {
        subplan_candidate::SubPlanCandidate::inspect(root, relation, rti, rte)
    }) else {
        return Ok(());
    };
    unsafe { candidate.plan() }
}

pub(super) unsafe fn set_join_pathlist(
    root: *mut pg_sys::PlannerInfo,
    join_rel: *mut pg_sys::RelOptInfo,
    outer_rel: *mut pg_sys::RelOptInfo,
    inner_rel: *mut pg_sys::RelOptInfo,
    join_type: pg_sys::JoinType::Type,
    extra: *mut pg_sys::JoinPathExtraData,
) -> Result<(), QueryHostError> {
    let Some(candidate) = (unsafe {
        join_candidate::JoinCandidate::inspect(
            root, join_rel, outer_rel, inner_rel, join_type, extra,
        )
    }) else {
        return Ok(());
    };
    unsafe { candidate.plan() }
}

pub(super) unsafe fn create_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
) -> Result<(), QueryHostError> {
    if unsafe {
        aggregate_candidate::AggregateCandidate::install(
            root, stage, input_rel, output_rel,
        )
    }? {
        return Ok(());
    }
    if unsafe {
        distinct_plan::DistinctCandidate::install(root, stage, input_rel, output_rel)
    }? {
        return Ok(());
    }
    let _ = unsafe {
        upper_plan::UpperPlanCandidate::install(root, stage, output_rel, extra)
    }?;
    Ok(())
}
