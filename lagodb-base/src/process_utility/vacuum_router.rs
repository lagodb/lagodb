//! Provider `VACUUM FULL` routing.
//!
//! VACUUM FULL is the one maintenance command PostgreSQL cannot run on a
//! provider relation: its rewrite targets relfiles that a provider does not
//! have. Routine VACUUM reaches providers through the table AM, and ANALYZE
//! runs natively. Neither is parsed or routed here.

use lagodb_core::hooks::HookError;
use lagodb_core::table_maintenance::postgres::{
    MaintenanceExecutor, MaintenancePlan,
};
use lagodb_core::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime,
};
use pgrx::pg_sys;

use crate::table_provider_registry::TableProviderRegistry;

use super::ProcessUtilityArgs;
use super::provider_maintenance::{
    ProviderContext, execute_provider, is_provider_am,
};

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
        )?;
        Ok(true)
    }
}
