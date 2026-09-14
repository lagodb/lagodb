//! ModifyTable-only PlannerPre -> previous/standard planner -> PlannerPost routing.
//!
//! Query offload does not use this hook. It contributes an upper CustomPath
//! through `set_join_pathlist_hook`/`create_upper_paths_hook` and then uses
//! PostgreSQL CustomScan method tables. The planner hook remains required by
//! the pre-existing custom-modify
//! contract, which injects whole-row inputs before planning and fixes the
//! resulting ModifyTable plan afterward.

use std::ffi::c_char;

use lagodb_core::diag::{PgReportError, ReportableError};
use lagodb_core::runtime_api::CallbackErrorReport;
use pgrx::{pg_guard, pg_sys};

use super::{PREV_PLANNER, callback_result, directory};

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn planner(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    // SAFETY: PostgreSQL supplies all planner-hook arguments and keeps them
    // live until this guarded callback returns.
    unsafe { route(parse, query_string, cursor_options, bound_params) }
        .report_unwrap()
}

unsafe fn route(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> Result<*mut pg_sys::PlannedStmt, PgReportError> {
    let snapshot = directory::modify_snapshot();
    snapshot.try_for_each(|descriptor| {
        let mut error = CallbackErrorReport::default();
        // SAFETY: registration validated this exact-build callback; `parse`
        // and the stack error record remain live for this synchronous call.
        let status = unsafe {
            (descriptor.planner_pre)(descriptor.context, parse, &mut error)
        };
        callback_result(status, &error, "modify planner pre callback")
    })?;

    let planned = if let Some(Some(previous)) = PREV_PLANNER.get() {
        // SAFETY: this is the PostgreSQL-provided predecessor planner and the
        // original hook arguments remain live.
        unsafe { previous(parse, query_string, cursor_options, bound_params) }
    } else {
        // SAFETY: these are the unmodified arguments supplied by PostgreSQL's
        // planner hook contract.
        unsafe {
            pg_sys::standard_planner(
                parse,
                query_string,
                cursor_options,
                bound_params,
            )
        }
    };
    snapshot.try_for_each(|descriptor| {
        let mut error = CallbackErrorReport::default();
        // SAFETY: registration validated this exact-build callback and the
        // returned `PlannedStmt` remains live in its planner context.
        let status = unsafe {
            (descriptor.planner_post)(descriptor.context, planned, &mut error)
        };
        callback_result(status, &error, "modify planner post callback")
    })?;
    Ok(planned)
}
