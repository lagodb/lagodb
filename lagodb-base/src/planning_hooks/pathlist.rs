//! Previous-first relation and upper pathlist routing.

use std::ffi::c_void;

use lagodb_core::diag::{PgReportError, ReportableError};
use lagodb_core::runtime_api::FfiErrorRecord;
use pgrx::{pg_guard, pg_sys};

use super::{
    PREV_CREATE_UPPER_PATHS, PREV_SET_REL_PATHLIST, callback_result, directory,
};

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn set_rel_pathlist(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) {
    if let Some(Some(previous)) = PREV_SET_REL_PATHLIST.get() {
        // SAFETY: this is the PostgreSQL-provided predecessor hook and the
        // current hook arguments remain live for the duration of the call.
        unsafe { previous(root, rel, rti, rte) };
    }
    directory::relation_scan_snapshot()
        .try_for_each(|descriptor| {
            let mut error = FfiErrorRecord::default();
            // SAFETY: registration validated this exact-build callback, and
            // all PostgreSQL pointers are forwarded only synchronously.
            let status = unsafe {
                (descriptor.plan_relation)(
                    descriptor.context,
                    root,
                    rel,
                    rti,
                    rte,
                    &mut error,
                )
            };
            callback_result(status, &error, "relation planning callback")
        })
        .report_unwrap();
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn create_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
) {
    if let Some(Some(previous)) = PREV_CREATE_UPPER_PATHS.get() {
        // SAFETY: this is the PostgreSQL-provided predecessor hook and the
        // current hook arguments remain live for the duration of the call.
        unsafe { previous(root, stage, input_rel, output_rel, extra) };
    }
    unsafe { route_upper_paths(root, stage, input_rel, output_rel, extra) }
        .report_unwrap();
}

unsafe fn route_upper_paths(
    root: *mut pg_sys::PlannerInfo,
    stage: pg_sys::UpperRelationKind::Type,
    input_rel: *mut pg_sys::RelOptInfo,
    output_rel: *mut pg_sys::RelOptInfo,
    extra: *mut c_void,
) -> Result<(), PgReportError> {
    directory::modify_snapshot().try_for_each(|descriptor| {
        let mut error = FfiErrorRecord::default();
        // SAFETY: registration validated this exact-build callback, and
        // all PostgreSQL pointers are forwarded only synchronously.
        let status = unsafe {
            (descriptor.create_upper_paths)(
                descriptor.context,
                root,
                stage,
                input_rel,
                output_rel,
                extra,
                &mut error,
            )
        };
        callback_result(status, &error, "modify upper-path callback")
    })?;
    unsafe {
        crate::query_host::create_upper_paths(root, stage, input_rel, output_rel)
    }
}
