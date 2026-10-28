//! Provider-neutral maintenance execution bridge.

use std::ffi::c_void;

use lagodb_core::diag::ReportableError;
use lagodb_core::handles::{RelationHandle, VacuumParamsHandle};
use lagodb_core::hooks::{HookError, UtilityHookPhase};
use lagodb_core::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceRequest, TableMaintenanceRouter,
};
use pgrx::{pg_guard, pg_sys};

use crate::table_provider_registry::TableProviderRegistry;

fn with_maintenance_context(error: HookError) -> HookError {
    error.with_utility_context(
        "TableMaintenanceRouter",
        UtilityHookPhase::Pre,
        pg_sys::NodeTag::T_VacuumStmt,
    )
}

/// Classify a catalog access method for the C VACUUM FULL probe.
#[pg_guard]
pub(super) unsafe extern "C-unwind" fn is_provider_am(
    access_method: pg_sys::Oid,
) -> bool {
    TableProviderRegistry::is_registered_am(access_method)
}

#[derive(Clone, Copy)]
pub(super) struct ProviderContext {
    pub(super) command_time: TableMaintenanceCommandTime,
    pub(super) budget: TableMaintenanceBudget,
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn execute_provider(
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
