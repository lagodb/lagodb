//! Query-offload recognition and Aggregate CustomPath materialization.

mod aggregate_plan;
mod candidate;
mod distinct_plan;
mod expression;
mod materialize;
mod path_installation;
mod table_scan_filter;

use pgrx::pg_sys;

use super::error::QueryHostError;

pub(super) use materialize::plan_custom_path;

pub(super) unsafe fn create_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
) -> Result<(), QueryHostError> {
    if let Some(candidate) = unsafe {
        candidate::SingleRelationCandidate::inspect_aggregate(
            root, stage, input_rel, output_rel,
        )
    } {
        return unsafe { candidate.plan_aggregate() };
    }
    if let Some(candidate) = unsafe {
        candidate::SingleRelationCandidate::inspect_distinct(
            root, stage, input_rel, output_rel,
        )
    } {
        return unsafe { candidate.plan_distinct() };
    }
    Ok(())
}
