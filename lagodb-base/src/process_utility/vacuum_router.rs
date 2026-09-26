//! Ordered PostgreSQL `VACUUM` routing.

use lagodb_core::hooks::HookError;
use lagodb_core::table_maintenance::postgres::{
    MaintenanceExecutor, MaintenancePlan, VacuumCallbacks,
};
use lagodb_core::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceRouter,
};
use pgrx::pg_sys;

use super::ProcessUtilityArgs;
use super::provider_maintenance::{
    ProviderContext, VacuumProbe, execute_provider, routes_analyze_to_provider,
    routes_vacuum_to_provider,
};

/// Return true only when this router consumed the VACUUM statement.
pub(crate) unsafe fn try_route_vacuum(
    stmt: *mut pg_sys::VacuumStmt,
    args: ProcessUtilityArgs,
    is_top_level: bool,
) -> Result<bool, HookError> {
    unsafe {
        if !TableMaintenanceRouter::has_providers() {
            return Ok(false);
        }
        let command = MaintenanceExecutor::prepare_command(
            stmt,
            args.query_string,
            args.query_env,
            is_top_level,
        );
        if command.options() & pg_sys::VACOPT_ONLY_DATABASE_STATS != 0 {
            return Ok(false);
        }
        if !VacuumProbe::matches(stmt, command.options()) {
            return Ok(false);
        }

        let mode = if command.options() & pg_sys::VACOPT_FULL != 0 {
            TableMaintenanceMode::Full
        } else {
            TableMaintenanceMode::Routine
        };
        let plan = MaintenancePlan::expand(stmt, command);
        MaintenanceExecutor::vacuum(
            plan,
            VacuumCallbacks {
                route_vacuum: routes_vacuum_to_provider,
                route_analyze: routes_analyze_to_provider,
                execute_provider,
            },
            || -> Result<ProviderContext, HookError> {
                Ok(ProviderContext {
                    command_time: TableMaintenanceCommandTime::now()?,
                    budget: TableMaintenanceBudget::configured(),
                    mode,
                })
            },
        )?;
        Ok(true)
    }
}
