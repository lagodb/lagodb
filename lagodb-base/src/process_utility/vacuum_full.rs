//! Provider `VACUUM FULL` routing and its provider-neutral execution bridge.
//!
//! VACUUM FULL is the one maintenance command PostgreSQL cannot run on a
//! provider relation: its rewrite targets relfiles that a provider does not
//! have. Routine VACUUM reaches providers through the table AM, and ANALYZE
//! runs natively. Neither is parsed or routed here.

use std::ffi::c_void;

use lagodb_core::diag::ReportableError;
use lagodb_core::handles::{RelationHandle, VacuumParamsHandle};
use lagodb_core::hooks::{HookError, UtilityHookPhase};
use lagodb_core::table_maintenance::postgres::{
    MaintenanceExecutor, MaintenancePlan,
};
use lagodb_core::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceRequest, TableMaintenanceRouter,
};
use pgrx::{pg_guard, pg_sys};

use crate::table_provider_registry::TableProviderRegistry;

use super::ProcessUtilityArgs;

/// Return true only when this router consumed the VACUUM statement.
///
/// # Safety
/// `args` must describe a live VacuumStmt invocation on the backend thread.
pub(super) unsafe fn try_route(args: ProcessUtilityArgs) -> Result<bool, HookError> {
    // SAFETY: the caller supplies the live VacuumStmt invocation. A consumed
    // command is top-level, because preparation rejects it everywhere else.
    unsafe {
        let stmt = args.target_node().cast::<pg_sys::VacuumStmt>();
        if !TableProviderRegistry::has_available_provider()
            || !MaintenanceExecutor::requests_full(stmt)
        {
            return Ok(false);
        }
        let command = MaintenanceExecutor::prepare_command(
            stmt,
            args.query_string,
            args.query_env,
            args.context == pg_sys::ProcessUtilityContext::PROCESS_UTILITY_TOPLEVEL,
        );
        if command.options() & pg_sys::VACOPT_PROCESS_MAIN == 0
            || !MaintenanceExecutor::probe_vacuum(stmt, is_provider_am)
        {
            return Ok(false);
        }

        MaintenanceExecutor::vacuum_full(
            MaintenancePlan::expand(
                stmt,
                command,
                TableProviderRegistry::is_registered_am,
            ),
            execute_provider,
            || -> Result<ProviderContext, HookError> {
                Ok(ProviderContext {
                    command_time: TableMaintenanceCommandTime::now()?,
                    budget: TableMaintenanceBudget::configured(),
                })
            },
            |utility| args.with_utility(utility).call_parent(),
        )
        .map_err(with_maintenance_context)?;
        Ok(true)
    }
}

/// Both error paths carry this context: a `vacuum_full` result error returns
/// to `try_route`, while a provider error raised inside `execute_provider`
/// reports from the C callback and never returns through that `Result`.
fn with_maintenance_context(error: HookError) -> HookError {
    error.with_utility_context(
        "TableMaintenanceRouter",
        UtilityHookPhase::Pre,
        pg_sys::NodeTag::T_VacuumStmt,
    )
}

/// Classify a catalog access method for the C VACUUM FULL probe.
#[pg_guard]
unsafe extern "C-unwind" fn is_provider_am(access_method: pg_sys::Oid) -> bool {
    TableProviderRegistry::is_registered_am(access_method)
}

#[derive(Clone, Copy)]
struct ProviderContext {
    command_time: TableMaintenanceCommandTime,
    budget: TableMaintenanceBudget,
}

#[pg_guard]
unsafe extern "C-unwind" fn execute_provider(
    relation: pg_sys::Relation,
    params: *mut pg_sys::VacuumParams,
    context: *mut c_void,
) {
    unsafe {
        let relation = RelationHandle::from_raw(relation);
        let context = &*context.cast::<ProviderContext>();
        let params = VacuumParamsHandle::from_raw(params);
        let options = TableMaintenanceOptions::from_vacuum_params(&params);
        let mode = TableMaintenanceMode::Full;
        TableMaintenanceRouter::execute(TableMaintenanceRequest {
            relation: &relation,
            mode,
            options,
            budget: context.budget.without_soft_limit(mode),
            command_time: context.command_time,
        })
        .map_err(|error| {
            with_maintenance_context(HookError::from(error.into_report_error()))
        })
        .report_unwrap();
    }
}
