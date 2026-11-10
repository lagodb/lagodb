//! PostgreSQL command execution; the runtime supplies provider policy.

use std::ffi::{c_char, c_void};
use std::ptr;

use pgrx::pg_sys;

use super::bridge::MaintenanceBridge;
use super::{
    MaintenanceCommand, MaintenancePlan, MaintenanceProbeCallback,
    MaintenanceProviderCallback,
};

/// Owns PostgreSQL command preparation and maintenance execution sequencing.
pub struct MaintenanceExecutor;

impl MaintenanceExecutor {
    /// Whether this is a VACUUM command with the FULL option enabled.
    ///
    /// # Safety
    /// The statement must belong to a live VacuumStmt utility invocation.
    pub unsafe fn requests_full(stmt: *mut pg_sys::VacuumStmt) -> bool {
        // SAFETY: the caller supplies the live statement.
        unsafe { MaintenanceBridge::requests_full(stmt) }
    }

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
    /// The command is prepared, the statement stays live, and the callback
    /// must protect any Rust panic / PostgreSQL error it raises.
    pub unsafe fn probe_vacuum(
        stmt: *mut pg_sys::VacuumStmt,
        callback: MaintenanceProbeCallback,
    ) -> bool {
        // SAFETY: the caller retains the prepared statement and callback.
        unsafe { MaintenanceBridge::probe_vacuum(stmt, callback) }
    }

    /// Execute VACUUM FULL across PostgreSQL and provider relations.
    ///
    /// `run_parent` hands a statement to the parent utility chain. Native
    /// relations run there with the original options. Provider relations are
    /// rewritten here, then their ANALYZE phase runs there as a native ANALYZE:
    /// `analyze_rel()` relies on `vacuum()`'s recursion guard, so calling it
    /// directly would let an extended-statistics expression nest ANALYZE.
    /// A database-wide command therefore no longer follows catalog order.
    ///
    /// Provider policy is prepared before any relation is touched, and only
    /// when a provider relation survived expansion.
    ///
    /// # Safety
    /// `plan` describes the live top-level command. Callbacks have pg_guard and
    /// borrow only relations held open by the synchronous C executor. The
    /// provider callback must interpret its context pointer as `Context`.
    pub unsafe fn vacuum_full<Context, Error>(
        mut plan: MaintenancePlan,
        execute_provider: MaintenanceProviderCallback,
        prepare_provider: impl FnOnce() -> Result<Context, Error>,
        run_parent: impl Fn(*mut pg_sys::Node),
    ) -> Result<(), Error> {
        // SAFETY: the plan owns the command across transaction commits.
        unsafe {
            let mut provider = if plan.provider.is_empty() {
                None
            } else {
                Some(prepare_provider()?)
            };

            if !plan.native.is_empty() {
                run_parent(plan.native_stmt().cast());
                if provider.is_none() {
                    return Ok(());
                }
            }

            // PostgreSQL's vacuum() has already released the Portal snapshot
            // when it ran first.
            if pg_sys::ActiveSnapshotSet() {
                pg_sys::PopActiveSnapshot();
            }
            pg_sys::CommitTransactionCommand();
            let mut rewritten = Vec::with_capacity(plan.provider.len());
            if let Some(provider) = provider.as_mut() {
                for relation in plan.provider.iter().copied() {
                    if MaintenanceBridge::vacuum_full_relation(
                        relation,
                        &mut plan.command.params,
                        execute_provider,
                        ptr::from_mut(provider).cast::<c_void>(),
                    ) {
                        rewritten.push(relation);
                    }
                }
            }
            pg_sys::StartTransactionCommand();
            if plan.command.updates_database_stats() {
                pg_sys::vac_update_datfrozenxid();
            }

            if plan.command.analyzes() && !rewritten.is_empty() {
                // vacuum() runs a single-relation ANALYZE in this transaction
                // under the caller's snapshot, and pops it for several. The
                // Portal expects the stack it left: empty after VACUUM.
                pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
                run_parent(plan.analyze_stmt(&rewritten).cast());
                if pg_sys::ActiveSnapshotSet() {
                    pg_sys::PopActiveSnapshot();
                }
            }
            Ok(())
        }
    }
}
