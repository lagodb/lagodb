//! One maintenance command scope for provider execution and parent delegation.

use lagodb_core::diag::ReportableError;
use lagodb_core::hooks::{HookError, UtilityHookPhase};
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{pg_guard, pg_sys};

use super::{
    PREV_PROCESS_UTILITY, ProcessUtilityArgs, analyze_router, vacuum_router,
};

type MaintenanceRouteCallback =
    unsafe extern "C-unwind" fn(*const ProcessUtilityArgs) -> bool;

unsafe extern "C-unwind" {
    fn lagodb_check_maintenance_recursion(stmt: *mut pg_sys::VacuumStmt);
    fn lagodb_execute_maintenance_command(
        args: *const ProcessUtilityArgs,
        previous: pg_sys::ProcessUtility_hook_type,
        route: MaintenanceRouteCallback,
    ) -> bool;
}

pub(super) struct MaintenanceCommandScope;

impl MaintenanceCommandScope {
    /// # Safety
    /// The statement must be live on the PostgreSQL backend thread.
    pub(super) unsafe fn check_recursion(stmt: *mut pg_sys::VacuumStmt) {
        // SAFETY: the C check borrows the live statement and can raise ERROR.
        unsafe {
            pg_guard_ffi_boundary(|| lagodb_check_maintenance_recursion(stmt));
        }
    }

    /// Execute a maintenance statement, including parent fallback if unclaimed.
    /// Returns true only when LagoDB consumed the statement.
    ///
    /// # Safety
    /// Arguments must describe a live VacuumStmt invocation on the backend
    /// thread. Input nodes must survive the command's transaction boundaries.
    pub(super) unsafe fn execute(args: ProcessUtilityArgs) -> bool {
        let previous = PREV_PROCESS_UTILITY.get().copied().flatten();
        // SAFETY: repr(C) args match LagodbMaintenanceUtilityArgs and stay on
        // this stack across commits. The closure only calls C; routing uses a
        // pg_guard callback, and C owns direct parent calls and ERROR cleanup.
        unsafe {
            pg_guard_ffi_boundary(|| {
                lagodb_execute_maintenance_command(&args, previous, route_maintenance)
            })
        }
    }

    unsafe fn try_route(args: ProcessUtilityArgs) -> Result<bool, HookError> {
        // SAFETY: the C command scope calls this only for a live VacuumStmt.
        unsafe {
            let stmt = args.target_node().cast::<pg_sys::VacuumStmt>();
            let is_top_level = args.context
                == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL;
            if (*stmt).is_vacuumcmd {
                vacuum_router::try_route_vacuum(stmt, args, is_top_level)
            } else {
                analyze_router::try_route_analyze(stmt, args, is_top_level)
            }
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn route_maintenance(
    args: *const ProcessUtilityArgs,
) -> bool {
    // SAFETY: the bridge borrows the caller's stack arguments synchronously.
    unsafe { MaintenanceCommandScope::try_route(*args) }
        .map_err(|error| {
            error.with_utility_context(
                "TableMaintenanceRouter",
                UtilityHookPhase::Pre,
                pg_sys::NodeTag::T_VacuumStmt,
            )
        })
        .report_unwrap()
}
