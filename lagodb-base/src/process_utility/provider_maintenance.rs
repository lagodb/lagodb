//! Provider-neutral maintenance execution bridge.
//!
//! PostgreSQL exposes these relations as `RELKIND_PARTITIONED_TABLE`, while a
//! capable provider owns one physical table instead of PG child relations. Execution
//! classification happens from the locked `Relation` immediately before the
//! PostgreSQL or provider action.

use std::ffi::{c_char, c_void};

use lagodb_core::diag::ReportableError;
use lagodb_core::handles::{RelationHandle, VacuumParamsHandle};
use lagodb_core::hooks::{HookError, UtilityHookPhase};
use lagodb_core::table_maintenance::postgres::MaintenanceExecutor;
use lagodb_core::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceRequest, TableMaintenanceRouter,
};
use pgrx::{PgSqlErrorCode, pg_guard, pg_sys};

use crate::maintenance::MaintenanceRoutePolicy;
use crate::table_provider_registry;

#[pg_guard]
unsafe extern "C-unwind" fn matches_provider(
    access_method: pg_sys::Oid,
    relkind: c_char,
    options: pg_sys::bits32,
) -> bool {
    let Some(owns_partitioned_table) =
        table_provider_registry::provider_owns_partitioned_table(access_method)
    else {
        return false;
    };
    let partitioned_table =
        relkind as u8 == pg_sys::RELKIND_PARTITIONED_TABLE && owns_partitioned_table;
    let policy = MaintenanceRoutePolicy::new(options);

    policy.routes_vacuum_to_provider(partitioned_table)
        || policy.routes_analyze_to_provider(partitioned_table)
}

/// Probe VACUUM/ANALYZE targets for provider routing before execution locks.
/// The result is a routing hint; locked relations determine the execution path.
pub(super) struct VacuumProbe;

impl VacuumProbe {
    /// # Safety
    /// The statement must be live and options must come from command preparation.
    pub(super) unsafe fn matches(
        stmt: *mut pg_sys::VacuumStmt,
        options: pg_sys::bits32,
    ) -> bool {
        // SAFETY: the prepared options and live statement belong to this
        // invocation; the callback only classifies registered provider metadata.
        unsafe { MaintenanceExecutor::probe_vacuum(stmt, options, matches_provider) }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ProviderContext {
    pub(super) command_time: TableMaintenanceCommandTime,
    pub(super) budget: TableMaintenanceBudget,
    pub(super) mode: TableMaintenanceMode,
}

struct LockedMaintenanceRelation {
    partitioned_table: bool,
}

impl LockedMaintenanceRelation {
    fn resolve(relation: &RelationHandle<'_>) -> Option<Self> {
        let owns_partitioned_table =
            table_provider_registry::provider_owns_partitioned_table(
                relation.access_method_oid(),
            )?;
        Some(Self {
            partitioned_table: relation.relkind() as u8
                == pg_sys::RELKIND_PARTITIONED_TABLE
                && owns_partitioned_table,
        })
    }

    fn routes_vacuum(&self, policy: MaintenanceRoutePolicy) -> bool {
        policy.routes_vacuum_to_provider(self.partitioned_table)
    }

    fn routes_analyze(
        &self,
        policy: MaintenanceRoutePolicy,
        relation: &RelationHandle<'_>,
    ) -> Result<bool, HookError> {
        if !policy.routes_analyze_to_provider(self.partitioned_table) {
            return Ok(false);
        }
        if !TableMaintenanceRouter::supports_analyze(relation.access_method_oid())? {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "ANALYZE is not supported by this table maintenance provider",
            ));
        }
        Ok(true)
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn routes_vacuum_to_provider(
    relation: pg_sys::Relation,
    params: *mut pg_sys::VacuumParams,
) -> bool {
    unsafe {
        let policy = MaintenanceRoutePolicy::new((*params).options);
        let relation = RelationHandle::from_raw(relation);
        match LockedMaintenanceRelation::resolve(&relation) {
            Some(relation) => relation.routes_vacuum(policy),
            None => false,
        }
    }
}

#[pg_guard]
pub(super) unsafe extern "C-unwind" fn routes_analyze_to_provider(
    relation: pg_sys::Relation,
    params: *mut pg_sys::VacuumParams,
) -> bool {
    unsafe {
        let policy = MaintenanceRoutePolicy::new((*params).options);
        let relation = RelationHandle::from_raw(relation);
        let Some(route) = LockedMaintenanceRelation::resolve(&relation) else {
            return false;
        };

        route
            .routes_analyze(policy, &relation)
            .map_err(|error| {
                error.with_utility_context(
                    "TableMaintenanceRouter",
                    UtilityHookPhase::Pre,
                    pg_sys::NodeTag::T_VacuumStmt,
                )
            })
            .report_unwrap()
    }
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
        TableMaintenanceRouter::execute(TableMaintenanceRequest {
            relation: &relation,
            mode: context.mode,
            options,
            budget: context.budget.without_soft_limit(context.mode),
            command_time: context.command_time,
        })
        .map_err(HookError::from)
        .map_err(|error| {
            error.with_utility_context(
                "TableMaintenanceRouter",
                UtilityHookPhase::Pre,
                pg_sys::NodeTag::T_VacuumStmt,
            )
        })
        .report_unwrap();
    }
}
