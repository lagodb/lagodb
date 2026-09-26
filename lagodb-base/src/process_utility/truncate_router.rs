//! Statement admission for provider-owned partitioned table TRUNCATE.
//!
//! Provider registration only enables the probe. A statement is consumed only
//! when an explicit target matches a partitioned table owner. The probe takes no
//! target relation locks; the C executor resolves and classifies targets again
//! under PostgreSQL's execution locks. A missing target or catalog row ends
//! the probe immediately and delegates the complete statement to parent.
//!
//! This admission boundary is intentional. Consuming every TRUNCATE merely
//! because a partitioned table provider is registered would also consume native-only
//! and ordinary managed-table commands, even without any provider-owned partitioned table in the
//! database. The runtime's consumed branch skips the captured ProcessUtility
//! parent. If pg_stat_statements installed its hook before LagoDB, its utility
//! execution statistics (including timing, buffer and WAL accounting) would
//! then be omitted for these commands. The C executor's native storage branches
//! and PostgreSQL-equivalent target locking do not restore that skipped hook.
//! See PostgreSQL's utility.c:ProcessUtility/standard_ProcessUtility and
//! contrib/pg_stat_statements/pg_stat_statements.c:pgss_ProcessUtility.
//!
//! Each statement already has one executor: a negative probe delegates the
//! complete statement to parent; a positive probe selects the C executor for
//! all targets, including mixed native/provider commands. Calling parent first
//! and then the C executor is not an observation-only workaround: PostgreSQL's
//! standard_ProcessUtility calls ExecuteTruncate, so that would execute twice.
//!
//! The C executor acquires AccessExclusiveLock through RangeVarGetRelidExtended
//! before relation_open(..., NoLock). That NoLock reuses an existing execution
//! lock; this probe's NoLock acquires no target lock. Moving execution locks
//! ahead of parent fallback changes hook/lock ordering and excludes that lock
//! wait from a captured pg_stat_statements hook's timing. The probe therefore
//! must not be removed or made locking solely to eliminate its known limits;
//! either change requires revisiting the parent-hook integration contract.
//!
//! A negative probe has an accepted DDL window: a native target can be replaced
//! by a same-named provider-owned partitioned table before the parent hook resolves it. PostgreSQL
//! then skips that table's storage action. Locked reclassification protects only
//! the consumed path, not parent fallback. Name resolution also runs schema
//! permission checks and namespace hooks before the parent hook. See
//! `lagodb-core/csrc/truncate/README.md` for the routing and topology contract.

use crate::table_provider_registry;
use lagodb_core::catalog::{range_var_get_relid, search_syscache1};
use lagodb_core::hooks::{HookError, UtilityHookPhase};
use lagodb_core::table_maintenance::postgres::MaintenanceExecutor;
use pgrx::{PgList, pg_sys};

struct TruncateProbe;

impl TruncateProbe {
    /// # Safety
    /// `stmt` must belong to the live backend utility invocation.
    unsafe fn matches(stmt: *mut pg_sys::TruncateStmt) -> Result<bool, HookError> {
        // SAFETY: the statement owns this list and each RangeVar throughout
        // this synchronous probe. PgList borrows the PostgreSQL-owned list.
        let relations =
            unsafe { PgList::<pg_sys::RangeVar>::from_pg((*stmt).relations) };
        for relation in relations.iter_ptr() {
            // SAFETY: the RangeVar is borrowed from the live statement. NoLock
            // deliberately leaves object stability to the actual executor.
            let relid =
                unsafe { range_var_get_relid(relation, pg_sys::NoLock as _, true) }?;
            if relid == pg_sys::InvalidOid {
                // Let the executor report this target before name resolution
                // of a later target can report a different error.
                return Ok(false);
            }
            let Some(tuple) = search_syscache1(
                pg_sys::SysCacheIdentifier::RELOID as _,
                relid.into(),
            ) else {
                // No target lock protects the catalog row from concurrent DDL.
                // Leave the complete statement to parent for resolution.
                return Ok(false);
            };
            // SAFETY: RELOID pins a pg_class tuple until `tuple` is dropped.
            let (relkind, access_method) = unsafe {
                let class =
                    &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_class);
                (class.relkind, class.relam)
            };
            drop(tuple);
            if relkind as u8 == pg_sys::RELKIND_PARTITIONED_TABLE
                && table_provider_registry::provider_owns_partitioned_table(
                    access_method,
                ) == Some(true)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Consume the complete statement only when a target needs partitioned table
/// execution; otherwise the caller delegates the original statement to parent.
///
/// # Safety
///
/// `stmt` must point to the live `TruncateStmt` owned by the current
/// `ProcessUtility` invocation.
pub(super) unsafe fn try_route(
    stmt: *mut pg_sys::TruncateStmt,
) -> Result<bool, HookError> {
    if !table_provider_registry::has_partitioned_table_provider() {
        return Ok(false);
    }
    // SAFETY: the caller supplies the live TRUNCATE statement.
    if !unsafe { TruncateProbe::matches(stmt) }.map_err(|error| {
        error.with_utility_context(
            "TruncateRouter",
            UtilityHookPhase::Pre,
            pg_sys::NodeTag::T_TruncateStmt,
        )
    })? {
        return Ok(false);
    }

    // SAFETY: the caller supplies the live `TruncateStmt`; both callbacks have
    // backend lifetime and accept only relations held open by the synchronous
    // C execution call.
    unsafe {
        MaintenanceExecutor::truncate(
            stmt,
            table_provider_registry::owns_partitioned_table,
            table_provider_registry::truncate_partitioned_table,
        );
    }
    Ok(true)
}
