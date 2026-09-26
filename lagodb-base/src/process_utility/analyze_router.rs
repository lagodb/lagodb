//! PostgreSQL ANALYZE routing for provider-owned partitioned tables.

use lagodb_core::hooks::HookError;
use lagodb_core::table_maintenance::TableMaintenanceRouter;
use lagodb_core::table_maintenance::postgres::{
    MaintenanceExecutor, MaintenancePlan,
};
use pgrx::pg_sys;

use super::ProcessUtilityArgs;
use super::provider_maintenance::{VacuumProbe, routes_analyze_to_provider};

/// Return true only when this router consumed the ANALYZE statement.
pub(crate) unsafe fn try_route_analyze(
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
        if !VacuumProbe::matches(stmt, command.options()) {
            return Ok(false);
        }

        let plan = MaintenancePlan::expand(stmt, command);
        MaintenanceExecutor::analyze(plan, is_top_level, routes_analyze_to_provider);
        Ok(true)
    }
}
