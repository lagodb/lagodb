//! PostgreSQL command execution; the runtime supplies provider routing and policy.

use std::ffi::{c_char, c_void};
use std::ptr;

use pgrx::pg_sys;

use super::bridge::MaintenanceBridge;
use super::{
    MaintenanceCommand, MaintenancePlan, MaintenanceProbeCallback,
    MaintenanceProviderCallback, MaintenanceRouteCallback,
};

/// Runtime callbacks used only with execution-locked relations.
#[derive(Clone, Copy)]
pub struct VacuumCallbacks {
    /// Select provider storage maintenance after the VACUUM execution lock.
    pub route_vacuum: MaintenanceRouteCallback,
    /// Select partitioned table sampling after the ANALYZE execution lock.
    pub route_analyze: MaintenanceRouteCallback,
    /// Borrow the prepared provider context during the storage action.
    pub execute_provider: MaintenanceProviderCallback,
}

/// Owns PostgreSQL command preparation and maintenance execution sequencing.
pub struct MaintenanceExecutor;

impl MaintenanceExecutor {
    /// Parse options and enforce PostgreSQL command-state and transaction rules.
    ///
    /// # Safety
    /// Inputs are from a live VacuumStmt utility invocation on the backend
    /// thread; `is_top_level` describes its ProcessUtility context.
    pub unsafe fn prepare_command(
        stmt: *mut pg_sys::VacuumStmt,
        query_string: *const c_char,
        query_env: *mut pg_sys::QueryEnvironment,
        is_top_level: bool,
    ) -> MaintenanceCommand {
        // SAFETY: the caller supplies the live PostgreSQL invocation.
        unsafe {
            MaintenanceBridge::prepare_command(
                stmt,
                query_string,
                query_env,
                is_top_level,
            )
        }
    }

    /// Inspect targets for admission without retaining target locks.
    ///
    /// # Safety
    /// Options come from preparation, the statement stays live, and the
    /// callback must protect any Rust panic / PostgreSQL error it raises.
    pub unsafe fn probe_vacuum(
        stmt: *mut pg_sys::VacuumStmt,
        options: pg_sys::bits32,
        callback: MaintenanceProbeCallback,
    ) -> bool {
        // SAFETY: the caller retains the prepared statement and callback.
        unsafe { MaintenanceBridge::probe_vacuum(stmt, options, callback) }
    }

    /// Execute ordered VACUUM and optional ANALYZE with PostgreSQL transactions.
    ///
    /// Provider policy is prepared after expansion and buffer strategy setup,
    /// before the initial commit. Empty plans do not prepare provider policy.
    ///
    /// # Safety
    /// The runtime maintenance scope is active. Callbacks have pg_guard and
    /// borrow only relations held open by the synchronous C executor. The
    /// provider callback must interpret its context pointer as `Context`.
    pub unsafe fn vacuum<Context, Error>(
        mut plan: MaintenancePlan,
        callbacks: VacuumCallbacks,
        prepare_provider: impl FnOnce() -> Result<Context, Error>,
    ) -> Result<(), Error> {
        // SAFETY: the runtime scope and plan own the command across commits.
        unsafe {
            if plan.is_empty() {
                if pg_sys::ActiveSnapshotSet() {
                    pg_sys::PopActiveSnapshot();
                }
                pg_sys::CommitTransactionCommand();
                pg_sys::StartTransactionCommand();
                if plan.command.params.options & pg_sys::VACOPT_SKIP_DATABASE_STATS
                    == 0
                {
                    pg_sys::vac_update_datfrozenxid();
                }
                return Ok(());
            }
            let bstrategy = if plan.command.params.options & pg_sys::VACOPT_FULL == 0
                || plan.command.params.options & pg_sys::VACOPT_ANALYZE != 0
            {
                plan.buffer_strategy(plan.command.ring_size_kb)
            } else {
                ptr::null_mut()
            };
            let mut provider = prepare_provider()?;

            if pg_sys::ActiveSnapshotSet() {
                pg_sys::PopActiveSnapshot();
            }
            pg_sys::CommitTransactionCommand();
            MaintenanceBridge::initialize_costs();
            for relation in plan.relations.iter().copied() {
                let analyze = MaintenanceBridge::vacuum_relation(
                    relation,
                    &mut plan.command.params,
                    bstrategy,
                    callbacks.route_vacuum,
                    callbacks.execute_provider,
                    ptr::from_mut(&mut provider).cast::<c_void>(),
                );
                if analyze
                    && plan.command.params.options & pg_sys::VACOPT_ANALYZE != 0
                {
                    pg_sys::StartTransactionCommand();
                    pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
                    MaintenanceBridge::analyze_relation(
                        relation,
                        &mut plan.command.params,
                        false,
                        bstrategy,
                        callbacks.route_analyze,
                    );
                    pg_sys::PopActiveSnapshot();
                    pg_sys::CommandCounterIncrement();
                    pg_sys::CommitTransactionCommand();
                }
            }
            MaintenanceBridge::finish_costs();
            pg_sys::StartTransactionCommand();
            if plan.command.params.options & pg_sys::VACOPT_SKIP_DATABASE_STATS == 0 {
                pg_sys::vac_update_datfrozenxid();
            }
            Ok(())
        }
    }

    /// Execute ANALYZE with PostgreSQL's single / per-relation transaction policy.
    ///
    /// # Safety
    /// The runtime maintenance scope is active; `is_top_level` describes the
    /// utility invocation and `route` has pg_guard. The plan's borrowed input
    /// names and column lists survive all command transactions.
    pub unsafe fn analyze(
        mut plan: MaintenancePlan,
        is_top_level: bool,
        route: MaintenanceRouteCallback,
    ) {
        // SAFETY: the caller holds the scope and this plan owns PG allocations.
        unsafe {
            if plan.is_empty() {
                return;
            }
            let bstrategy = plan.buffer_strategy(plan.command.ring_size_kb);
            let in_outer_xact = pg_sys::IsInTransactionBlock(is_top_level);
            let use_own_xacts = !in_outer_xact && plan.relations.len() > 1;
            if use_own_xacts {
                if pg_sys::ActiveSnapshotSet() {
                    pg_sys::PopActiveSnapshot();
                }
                pg_sys::CommitTransactionCommand();
            }
            MaintenanceBridge::initialize_costs();
            for relation in plan.relations.iter().copied() {
                if use_own_xacts {
                    pg_sys::StartTransactionCommand();
                    pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
                }
                MaintenanceBridge::analyze_relation(
                    relation,
                    &mut plan.command.params,
                    in_outer_xact,
                    bstrategy,
                    route,
                );
                pg_sys::CommandCounterIncrement();
                if use_own_xacts {
                    pg_sys::PopActiveSnapshot();
                    pg_sys::CommitTransactionCommand();
                }
            }
            MaintenanceBridge::finish_costs();
            if use_own_xacts {
                pg_sys::StartTransactionCommand();
            }
        }
    }

    /// Execute one atomic PostgreSQL TRUNCATE statement with provider root dispatch.
    ///
    /// # Safety
    /// The statement belongs to the live backend utility invocation. Both
    /// callbacks have pg_guard and borrow only execution-locked relations.
    pub unsafe fn truncate(
        stmt: *mut pg_sys::TruncateStmt,
        owns_partitioned_table: unsafe extern "C-unwind" fn(pg_sys::Relation) -> bool,
        truncate_partitioned_table: unsafe extern "C-unwind" fn(pg_sys::Relation),
    ) {
        // SAFETY: the caller supplies a live command and guarded callbacks.
        unsafe {
            MaintenanceBridge::truncate(
                stmt,
                owns_partitioned_table,
                truncate_partitioned_table,
            )
        }
    }
}
