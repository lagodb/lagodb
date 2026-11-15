//! Transaction-local state and pre-commit publication for managed Iceberg
//! tables; the managed counterpart of `foreign_table::transaction`.
//!
//! [`ManagedTransaction`] owns the bookkeeping for every managed table modified
//! inside a single PostgreSQL top-level transaction:
//!
//! - the transaction-local schema/data action log accumulated across statements,
//! - a per-savepoint history stack used to roll back on
//!   `ROLLBACK TO SAVEPOINT`,
//! - the exclusive VACUUM action and its commit attempt,
//! - one final metadata materialization during top-level pre-commit.
//!
//! It implements [`TransactionResource`] so the lagodb-core transaction
//! framework drives it through pre-commit / commit / abort / sub-abort
//! callbacks.
//!
//! The single thread-local `Rc<ManagedTransaction>` is reached through
//! [`ManagedTransaction::current`]; callers (mutation, scan, DDL) never touch
//! the TLS directly.

mod commit;
mod loaded_metadata;
mod read;
mod state;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use iceberg_lite::io::FileIO;
use iceberg_lite::transaction::PreparedSchemaUpdate;
use lagodb_core::diag::PgReportError;
use lagodb_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::IcebergError;
use crate::managed_table::catalog::metadata_table::IcebergMetadata;
use crate::managed_table::maintenance::{
    AutomaticMaintenanceNotifier, PreparedVacuum,
};
use crate::managed_table::{ManagedResult, ManagedTableError};
use crate::write::{
    ExclusiveTransactionAction, PreparedTablePropertyUpdate, RelationRowRegistry,
    TableTransactionState, TxTableActionLog as SharedActionLog, TxWriteBatch,
};

pub use self::loaded_metadata::LoadedTableMetadata;
pub(crate) use self::loaded_metadata::LoadedTableRead;

type ManagedTableActionLog = SharedActionLog<PreparedVacuum, String>;
type ManagedTableTransaction = TableTransactionState<PreparedVacuum, String>;

use state::ManagedTableState;

/// AM-owned commit input detached before local metadata I/O begins.
///
/// The shared action snapshot contains no local catalog state. The AM adapter
/// adds `FileIO` here before resolving and publishing a metadata location.
pub(super) struct TableCommitInput {
    pub(super) actions: Rc<ManagedTableActionLog>,
    pub(super) file_io: FileIO,
}

/// Mutable state hidden inside [`ManagedTransaction`]'s `RefCell`.
#[derive(Debug, Default)]
struct ManagedTransactionInner {
    tables: HashMap<pg_sys::Oid, ManagedTableState>,
}

impl ManagedTransactionInner {
    /// Return this relation's state, creating it the first time the
    /// transaction touches the relation. Entries are never removed before the
    /// transaction ends; savepoint rollback keeps an empty entry.
    fn table_mut(&mut self, relid: pg_sys::Oid) -> &mut ManagedTableState {
        self.tables
            .entry(relid)
            .or_insert_with(ManagedTableState::new)
    }
}

/// Per-transaction state and the transaction resource that publishes it.
///
/// Always reached through [`ManagedTransaction::current`]. The thread-local
/// handle is reset on commit / top-level abort, so a fresh
/// `ManagedTransaction` is created for the next transaction.
#[derive(Debug)]
pub struct ManagedTransaction {
    inner: RefCell<ManagedTransactionInner>,
}

thread_local! {
    /// Per-transaction `ManagedTransaction`, created lazily by
    /// [`ManagedTransaction::current`] and torn down by `on_commit` / `on_abort`.
    static CURRENT: RefCell<Option<Rc<ManagedTransaction>>> = const { RefCell::new(None) };
}

impl ManagedTransaction {
    /// Whether this backend has transaction-local actions whose relation view
    /// cannot be reconstructed independently by a PostgreSQL parallel worker.
    pub(crate) fn has_local_actions(relid: pg_sys::Oid) -> bool {
        CURRENT.with(|slot| {
            let slot = slot.borrow();
            slot.as_ref().is_some_and(|tracker| {
                tracker
                    .inner
                    .borrow()
                    .tables
                    .get(&relid)
                    .is_some_and(ManagedTableState::has_changes)
            })
        })
    }

    /// Get (or lazily install) the `ManagedTransaction` for the current transaction.
    ///
    /// The first call inside a transaction also registers the instance with
    /// lagodb-core's transaction framework, pinned at top-level
    /// (`nest_level = 1`) so it survives any savepoint abort and can run
    /// `commit_all` in `on_pre_commit`.
    pub fn current() -> Rc<ManagedTransaction> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(existing) = slot.as_ref() {
                return Rc::clone(existing);
            }
            let me = Rc::new(ManagedTransaction {
                inner: RefCell::new(ManagedTransactionInner::default()),
            });
            transaction::register_resource(
                Rc::clone(&me) as Rc<dyn TransactionResource>
            );
            *slot = Some(Rc::clone(&me));
            me
        })
    }

    /// Register a table the first time this transaction touches it. Idempotent.
    ///
    /// Module-private because external callers must go through
    /// [`ManagedTransaction::begin_table_modify`] to acquire write-side metadata; that
    /// method bundles registration with the metadata read so callers cannot
    /// accidentally bypass tracking by reading without registering first.
    fn register_table(&self, relid: pg_sys::Oid) {
        self.inner.borrow_mut().table_mut(relid);
    }

    /// Publish one fully constructed relation-local write batch atomically.
    pub(crate) fn stage_batch(
        &self,
        relid: pg_sys::Oid,
        batch: TxWriteBatch,
        file_io: &FileIO,
    ) -> ManagedResult<()> {
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_batch(nest_level, batch)
        })
    }

    // Intentionally no equality-delete staging API. With the single-snapshot
    // materialization used by this tracker, equality deletes cannot delete data
    // files appended in the same PostgreSQL transaction because both files
    // inherit the same Iceberg snapshot sequence number, while equality deletes
    // require delete_seq > data_seq. A hidden multi-snapshot materialization
    // could model that ordering, but it would write multiple manifest lists,
    // complicate v3 row-lineage accounting, and expose transaction-internal
    // intermediate snapshots through Iceberg snapshot history/time travel.
    // SQL DELETE/UPDATE should stage position deletes instead.

    /// Statement write/DDL path: record a prepared Iceberg schema update
    /// without generating metadata files immediately.
    pub fn stage_schema_update(
        &self,
        relid: pg_sys::Oid,
        update: PreparedSchemaUpdate,
        file_io: &FileIO,
    ) -> ManagedResult<()> {
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_schema_update(nest_level, update)
        })
    }

    /// DDL path: stage a fully resolved Iceberg table-property replacement.
    pub(crate) fn stage_table_property_update(
        &self,
        relid: pg_sys::Oid,
        update: PreparedTablePropertyUpdate,
        file_io: &FileIO,
    ) -> ManagedResult<()> {
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_table_property_update(nest_level, update)
        })
    }

    /// Stage one aggregate VACUUM action. It is intentionally exclusive with
    /// Data writes, schema evolution, TRUNCATE, and DROP for this relation.
    pub(crate) fn stage_vacuum(
        &self,
        relid: pg_sys::Oid,
        vacuum: PreparedVacuum,
        file_io: &FileIO,
    ) -> ManagedResult<()> {
        // The borrow ends with this statement, before the staging borrow.
        let rebuilt = self.inner.borrow_mut().table_mut(relid).was_rebuilt();
        if rebuilt {
            return Err(vacuum.conflict_error());
        }
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_exclusive_action(nest_level, vacuum)
        })
    }

    /// Check that no exclusive action conflicts with a local rebuild.
    pub(crate) fn prepare_local_rebuild(
        &self,
        relid: pg_sys::Oid,
    ) -> ManagedResult<()> {
        let mut inner = self.inner.borrow_mut();
        let state = inner.table_mut(relid);
        if let Some(error) = state.transaction.actions.exclusive_conflict_error() {
            return Err(error);
        }
        Ok(())
    }

    /// A rebuilt table is already published in the transactional PG catalog.
    /// Discard the retired generation's actions but keep a savepoint restore
    /// point and a lifecycle marker even when the new action log is empty.
    pub(crate) fn record_local_rebuild(&self, relid: pg_sys::Oid, file_io: &FileIO) {
        let nest_level = current_nest_level();
        self.inner
            .borrow_mut()
            .table_mut(relid)
            .record_rebuild(nest_level, file_io);
    }

    /// Stage a full-table truncate against the metadata location visible when
    /// PostgreSQL invokes the table AM truncate callback.
    pub fn stage_truncate(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> ManagedResult<()> {
        let expected_metadata_location = IcebergMetadata::get(relid)?
            .metadata_location
            .ok_or(ManagedTableError::MetadataLocationNull)?;
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_truncate(nest_level, expected_metadata_location)
        })
    }

    /// Mark an Iceberg relation as dropped. DROP always creates tracker state
    /// so PREPARE can reject the non-serializable lifecycle action and
    /// savepoint rollback can restore any earlier actions.
    pub fn stage_drop(relid: pg_sys::Oid) -> ManagedResult<()> {
        let tracker = Self::current();
        let nest_level = current_nest_level();
        let mut inner = tracker.inner.borrow_mut();
        inner.table_mut(relid).transaction.record_drop(nest_level)
    }

    fn stage_table_mutation<F>(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
        mutation: F,
    ) -> ManagedResult<()>
    where
        F: FnOnce(&mut ManagedTableTransaction, i32) -> ManagedResult<()>,
    {
        let nest_level = current_nest_level();
        let mut inner = self.inner.borrow_mut();
        let state = inner.table_mut(relid);
        if state.file_io.is_none() {
            state.file_io = Some(file_io.clone());
        }
        mutation(&mut state.transaction, nest_level)
    }

    /// Return the transaction-scoped physical-row registry for one relation.
    ///
    /// The clone is a single-backend `Rc` handle. Callers retain it across row
    /// callbacks so the hot path does not re-enter the relation HashMap.
    pub(crate) fn row_registry(
        &self,
        relid: pg_sys::Oid,
    ) -> ManagedResult<RelationRowRegistry> {
        let mut inner = self.inner.try_borrow_mut().map_err(|_| {
            IcebergError::InvariantViolated("managed transaction is already borrowed")
        })?;
        Ok(inner.table_mut(relid).row_registry.clone())
    }

    /// Detach one table's immutable commit input before any metadata I/O.
    fn commit_input(
        &self,
        relid: pg_sys::Oid,
    ) -> ManagedResult<Option<TableCommitInput>> {
        let inner = self.inner.borrow();
        let Some(state) = inner.tables.get(&relid) else {
            return Ok(None);
        };
        if state.transaction.actions.is_empty() {
            return Ok(None);
        }
        if state.transaction.actions.is_dropped() {
            return Ok(None);
        }
        let file_io = state.file_io.clone().ok_or_else(|| {
            ManagedTableError::ManagedTransaction(format!(
                "table {} has staged Iceberg metadata changes without FileIO",
                relid
            ))
        })?;
        Ok(Some(TableCommitInput {
            actions: Rc::clone(&state.transaction.actions),
            file_io,
        }))
    }

    // -------------------------------------------------------------------------
    // Sub-abort
    // -------------------------------------------------------------------------

    /// Roll back every tracked table to `target_level`.
    ///
    /// Empty table entries are intentionally retained until transaction end:
    /// their file registry is the authority for transaction-stable synthetic
    /// ctid IDs, and IDs allocated inside an aborted savepoint must not be
    /// reused. Entries with no delta, validation, or schema update are ignored
    /// by `commit_input`.
    fn rollback_to_level(&self, target_level: i32) {
        let mut inner = self.inner.borrow_mut();
        for state in inner.tables.values_mut() {
            state.rollback_to_level(target_level);
        }
    }

    /// Promote every table's internal nest levels down to `from_level - 1`.
    ///
    /// Called from `on_commit_sub` to mirror PostgreSQL's `RELEASE SAVEPOINT`
    /// reparenting: state owned by the released subtransaction now belongs
    /// to its parent, so its recorded nest level must drop accordingly.
    /// Without this, a sibling savepoint opened later at the same nest
    /// level would alias the released one. A `ROLLBACK TO` of the sibling
    /// would then incorrectly discard already-promoted writes.
    fn promote_to_level(&self, from_level: i32) {
        let mut inner = self.inner.borrow_mut();
        for state in inner.tables.values_mut() {
            state.promote_to_level(from_level);
        }
    }
}

// =============================================================================
// TransactionResource integration
// =============================================================================

impl TransactionResource for ManagedTransaction {
    /// Pinned at top-level on purpose: `commit_all` must run for the entire
    /// transaction's accumulated changes regardless of any savepoint
    /// promotions, so this resource never participates in nest-level
    /// promotion (see `set_nest_level`).
    fn nest_level(&self) -> i32 {
        1
    }

    /// No-op: the framework's `set_nest_level` is only relevant to resources
    /// whose lifetime is tied to a specific savepoint. `ManagedTransaction` is
    /// transaction-scoped and pinned at level 1; promotion would not be
    /// meaningful here.
    fn set_nest_level(&self, _level: i32) {}

    fn on_pre_commit(&self) -> TransactionResult<()> {
        if self.commit_all()? {
            AutomaticMaintenanceNotifier::stage_wakeup()?;
        }
        Ok(())
    }

    fn on_pre_prepare(&self) -> TransactionResult<()> {
        let has_staged_actions = self
            .inner
            .borrow()
            .tables
            .values()
            .any(ManagedTableState::has_changes);
        if has_staged_actions {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot PREPARE a transaction with staged Iceberg metadata changes",
            ));
        }
        Ok(())
    }

    fn on_commit(&self) {
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_prepare(&self) {
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_abort(&self) {
        // The PG transaction is rolling back; no compensating writes needed.
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        // RELEASE SAVEPOINT promotes the released subtransaction's state to
        // its parent. The framework only updates `nest_level()` on the
        // resource itself, but per-frame and mutation-owner `nest_level`
        // values are owned here and must be promoted too.
        // See `ManagedTransaction::promote_to_level` for why this is required.
        self.promote_to_level(current_nest_level);
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        // `current_nest_level` is the level being aborted, matching lagodb-core's
        // resource removal rule (resources with `nest_level >= current_nest_level`
        // are removed from the resource list).
        self.rollback_to_level(current_nest_level);
    }
}

// =============================================================================
// Helpers
// =============================================================================

fn current_nest_level() -> i32 {
    unsafe { pg_sys::GetCurrentTransactionNestLevel() }
}
