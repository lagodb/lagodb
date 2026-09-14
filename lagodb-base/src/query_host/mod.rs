//! Sole PostgreSQL host for provider-neutral query offload.

use std::ffi::c_void;

mod error;
mod execution;
mod explain;
mod methods;
mod planning;

use lagodb_core::diag::PgReportError;
use pgrx::pg_sys;

pub(crate) fn init() {
    methods::register();
}

pub(crate) unsafe fn create_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
) -> Result<(), PgReportError> {
    unsafe { planning::create_upper_paths(root, stage, input_rel, output_rel, extra) }
        .map_err(error::QueryHostError::into_report)
}

pub(crate) unsafe fn set_join_pathlist(
    root: *mut pg_sys::PlannerInfo,
    join_rel: *mut pg_sys::RelOptInfo,
    outer_rel: *mut pg_sys::RelOptInfo,
    inner_rel: *mut pg_sys::RelOptInfo,
    join_type: pg_sys::JoinType::Type,
    extra: *mut pg_sys::JoinPathExtraData,
) -> Result<(), PgReportError> {
    unsafe {
        planning::set_join_pathlist(
            root, join_rel, outer_rel, inner_rel, join_type, extra,
        )
    }
    .map_err(error::QueryHostError::into_report)
}

pub(crate) unsafe fn set_rel_pathlist(
    root: *mut pg_sys::PlannerInfo,
    relation: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) -> Result<(), PgReportError> {
    unsafe { planning::set_rel_pathlist(root, relation, rti, rte) }
        .map_err(error::QueryHostError::into_report)
}
