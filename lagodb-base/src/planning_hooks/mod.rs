//! Sole owner and router of PostgreSQL planning hook pointers.

mod directory;

use std::ffi::{c_char, c_void};
use std::sync::OnceLock;

use lagodb_core::diag::{PgReportError, ReportableError};
use lagodb_core::runtime_api::{
    PLANNING_CALLBACK_FAILED, PLANNING_CALLBACK_OK, PlanErrorRecord,
};
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{pg_guard, pg_sys};

pub(crate) use directory::{PreparedPlanningHooks, commit, prepare};

static PREV_PLANNER: OnceLock<pg_sys::planner_hook_type> = OnceLock::new();
static PREV_SET_REL_PATHLIST: OnceLock<pg_sys::set_rel_pathlist_hook_type> =
    OnceLock::new();
static PREV_CREATE_UPPER_PATHS: OnceLock<pg_sys::create_upper_paths_hook_type> =
    OnceLock::new();

pub(crate) fn init() {
    // SAFETY: `lagodb-base` installs each process-global slot once during
    // single-threaded shared-preload initialization, before provider loading.
    unsafe {
        PREV_PLANNER.get_or_init(|| {
            let previous = pg_sys::planner_hook;
            pg_sys::planner_hook = Some(planner);
            previous
        });
        PREV_SET_REL_PATHLIST.get_or_init(|| {
            let previous = pg_sys::set_rel_pathlist_hook;
            pg_sys::set_rel_pathlist_hook = Some(set_rel_pathlist);
            previous
        });
        PREV_CREATE_UPPER_PATHS.get_or_init(|| {
            let previous = pg_sys::create_upper_paths_hook;
            pg_sys::create_upper_paths_hook = Some(create_upper_paths);
            previous
        });
    }
}

fn callback_result(
    status: u32,
    error: &PlanErrorRecord,
    callback: &'static str,
) -> Result<(), PgReportError> {
    match status {
        PLANNING_CALLBACK_OK => Ok(()),
        PLANNING_CALLBACK_FAILED => {
            // SAFETY: registered exact-build callbacks must return text owned
            // by the active PostgreSQL context for this synchronous call.
            Err(unsafe { error.to_error(callback) })
        }
        status => Err(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            format!("{callback} returned unknown planning status {status}"),
        )),
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn set_rel_pathlist(
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
            let mut error = PlanErrorRecord::default();
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
unsafe extern "C-unwind" fn planner(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> *mut pg_sys::PlannedStmt {
    // SAFETY: PostgreSQL supplies all planner-hook arguments and keeps them
    // live until this guarded callback returns.
    unsafe { route_planner(parse, query_string, cursor_options, bound_params) }
        .report_unwrap()
}

unsafe fn route_planner(
    parse: *mut pg_sys::Query,
    query_string: *const c_char,
    cursor_options: i32,
    bound_params: pg_sys::ParamListInfo,
) -> Result<*mut pg_sys::PlannedStmt, PgReportError> {
    let snapshot = directory::modify_snapshot();
    snapshot.try_for_each(|descriptor| {
        let mut error = PlanErrorRecord::default();
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
    if planned.is_null() {
        return Ok(planned);
    }

    snapshot.try_for_each(|descriptor| {
        let mut error = PlanErrorRecord::default();
        // SAFETY: registration validated this exact-build callback and the
        // returned `PlannedStmt` remains live in its planner context.
        let status = unsafe {
            (descriptor.planner_post)(descriptor.context, planned, &mut error)
        };
        callback_result(status, &error, "modify planner post callback")
    })?;
    Ok(planned)
}

#[pg_guard]
unsafe extern "C-unwind" fn create_upper_paths(
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
    directory::modify_snapshot()
        .try_for_each(|descriptor| {
            let mut error = PlanErrorRecord::default();
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
        })
        .report_unwrap();
}
