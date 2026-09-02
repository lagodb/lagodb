//! Sole owner and router of PostgreSQL planning hook pointers.

mod directory;
mod pathlist;
mod planner;

use std::sync::OnceLock;

use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{
    FFI_OPERATION_FAILED, FFI_OPERATION_OK, FfiErrorRecord,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

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
            pg_sys::planner_hook = Some(planner::planner);
            previous
        });
        PREV_SET_REL_PATHLIST.get_or_init(|| {
            let previous = pg_sys::set_rel_pathlist_hook;
            pg_sys::set_rel_pathlist_hook = Some(pathlist::set_rel_pathlist);
            previous
        });
        PREV_CREATE_UPPER_PATHS.get_or_init(|| {
            let previous = pg_sys::create_upper_paths_hook;
            pg_sys::create_upper_paths_hook = Some(pathlist::create_upper_paths);
            previous
        });
    }
}

fn callback_result(
    status: u32,
    error: &FfiErrorRecord,
    callback: &'static str,
) -> Result<(), PgReportError> {
    match status {
        FFI_OPERATION_OK => Ok(()),
        FFI_OPERATION_FAILED => {
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
