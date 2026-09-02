//! Sole PostgreSQL host for provider-neutral query offload.

mod error;
mod execution;
mod methods;
mod metrics;
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
) -> Result<(), PgReportError> {
    unsafe { planning::create_upper_paths(root, stage, input_rel, output_rel) }
        .map_err(error::QueryHostError::into_report)
}
