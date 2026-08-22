//! Per-transaction Iceberg metadata tracking.
//!
//! [`TxMetadata`] owns the bookkeeping for every Iceberg table modified inside
//! a single PostgreSQL top-level transaction:
//!
//! - the transaction-local schema/data action log accumulated across statements,
//! - a per-savepoint history stack used to roll back on
//!   `ROLLBACK TO SAVEPOINT`,
//! - one final metadata materialization during top-level pre-commit.
//!
//! It also implements [`TransactionResource`] so the pg-lakebase-core
//! transaction framework can drive it through pre-commit / commit / abort /
//! sub-abort callbacks.
//!
//! The single thread-local `Rc<TxMetadata>` is reached through
//! [`TxMetadata::current`]; callers (mutation, scan) never touch the TLS directly.

mod commit;
mod loaded_metadata;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use iceberg_lite::io::FileIO;
use iceberg_lite::overlay::DeleteFileIdentity;
use iceberg_lite::spec::{DataFile, TableMetadata};
use iceberg_lite::transaction::{PreparedSchemaUpdate, RowDeltaValidation};
use pg_lakebase_core::diag::PgReportError;
use pg_lakebase_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::engine::write::{
    PreparedTablePropertyUpdate, RelationRowRegistry, TableTransactionState,
    TxTableActionLog as SharedActionLog,
};
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::catalog::metadata_table::IcebergMetadata;
use crate::managed_table::maintenance::{
    AutomaticMaintenanceNotifier, PreparedVacuum,
};

pub use self::loaded_metadata::LoadedTableMetadata;

type ManagedTableActionLog = SharedActionLog<PreparedVacuum, String>;
type ManagedTableTransaction = TableTransactionState<PreparedVacuum, String>;

#[derive(Debug)]
struct ManagedTableState {
    transaction: ManagedTableTransaction,
    file_io: Option<FileIO>,
}

impl ManagedTableState {
    fn new() -> Self {
        Self {
            transaction: ManagedTableTransaction::new(),
            file_io: None,
        }
    }
}

/// AM-owned commit input detached before local metadata I/O begins.
///
/// The shared action snapshot contains no local catalog state. The AM adapter
/// adds `FileIO` here before resolving and publishing a metadata location.
pub(super) struct TableCommitInput {
    pub(super) actions: Rc<ManagedTableActionLog>,
    pub(super) file_io: FileIO,
}

/// Mutable state hidden inside [`TxMetadata`]'s `RefCell`.
#[derive(Debug, Default)]
struct TxMetadataInner {
    tables: HashMap<pg_sys::Oid, ManagedTableState>,
}

/// Single per-transaction tracking + transaction-resource object.
///
/// Combines what used to be `MetadataLocationTracker` (state) and
/// `MetadataLocationResource` (transaction-callback shim) into one type so
/// that ownership and lifecycle live in one place.
///
/// Always reached through [`TxMetadata::current`]. The thread-local handle is
/// reset on commit / top-level abort, so a fresh `TxMetadata` is created for
/// the next transaction.
#[derive(Debug)]
pub struct TxMetadata {
    inner: RefCell<TxMetadataInner>,
}

thread_local! {
    /// Per-transaction `TxMetadata`, created lazily by [`TxMetadata::current`]
    /// and torn down by `on_commit` / `on_abort`.
    static CURRENT: RefCell<Option<Rc<TxMetadata>>> = const { RefCell::new(None) };
}

impl TxMetadata {
    /// Get (or lazily install) the `TxMetadata` for the current transaction.
    ///
    /// The first call inside a transaction also registers the instance with
    /// pg-lakebase-core's transaction framework, pinned at top-level
    /// (`nest_level = 1`) so it survives any savepoint abort and can run
    /// `commit_all` in `on_pre_commit`.
    pub fn current() -> Rc<TxMetadata> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(existing) = slot.as_ref() {
                return Rc::clone(existing);
            }
            let me = Rc::new(TxMetadata {
                inner: RefCell::new(TxMetadataInner::default()),
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
    /// [`TxMetadata::begin_table_modify`] to acquire write-side metadata; that
    /// method bundles registration with the metadata read so callers cannot
    /// accidentally bypass tracking by reading without registering first.
    fn register_table(&self, relid: pg_sys::Oid) {
        let mut inner = self.inner.borrow_mut();
        if inner.tables.contains_key(&relid) {
            return;
        }
        let state = ManagedTableState::new();
        inner.tables.insert(relid, state);
    }

    /// Statement write path: record new data files in the transaction-local
    /// delta without generating Iceberg metadata files.
    pub fn stage_data_files(
        &self,
        relid: pg_sys::Oid,
        new_data_files: Vec<DataFile>,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_data_files(nest_level, new_data_files)
        })
    }

    /// Statement write path: record a position delete file in the
    /// transaction-local delta without generating Iceberg metadata files.
    pub fn stage_position_delete_file<I, S>(
        &self,
        relid: pg_sys::Oid,
        delete_file: DataFile,
        referenced_data_files: I,
        file_io: &FileIO,
    ) -> IcebergResult<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let referenced_data_files: Vec<String> =
            referenced_data_files.into_iter().map(Into::into).collect();
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_position_delete_file(
                nest_level,
                delete_file,
                referenced_data_files,
            )
        })
    }

    /// Statement write path: remove a live delete manifest entry by identity
    /// in the transaction-local delta.
    pub fn stage_remove_delete_file(
        &self,
        relid: pg_sys::Oid,
        identity: DeleteFileIdentity,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_remove_delete_file(nest_level, identity)
        })
    }

    /// Statement write path: record Iceberg RowDelta conflict validation to be
    /// evaluated at the transaction's final metadata commit.
    pub fn stage_row_delta_validation(
        &self,
        relid: pg_sys::Oid,
        validation: RowDeltaValidation,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_validation(nest_level, validation)
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
    ) -> IcebergResult<()> {
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
    ) -> IcebergResult<()> {
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
    ) -> IcebergResult<()> {
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_exclusive_action(nest_level, vacuum)
        })
    }

    /// Stage a full-table truncate against the metadata location visible when
    /// PostgreSQL invokes the table AM truncate callback.
    pub fn stage_truncate(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        let expected_metadata_location = IcebergMetadata::get(relid)?
            .metadata_location
            .ok_or(IcebergError::MetadataLocationNull)?;
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_truncate(nest_level, expected_metadata_location)
        })
    }

    /// Mark an Iceberg relation as dropped. DROP always creates tracker state
    /// so PREPARE can reject the non-serializable lifecycle action and
    /// savepoint rollback can restore any earlier actions.
    pub fn stage_drop(relid: pg_sys::Oid) -> IcebergResult<()> {
        let tracker = Self::current();
        tracker.register_table(relid);
        let nest_level = current_nest_level();
        let mut inner = tracker.inner.borrow_mut();
        let state = inner.tables.get_mut(&relid).ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "Table {} not registered before staging DROP",
                relid
            ))
        })?;
        state.transaction.record_drop(nest_level)?;
        Ok(())
    }

    fn stage_table_mutation<F>(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
        mutation: F,
    ) -> IcebergResult<()>
    where
        F: FnOnce(&mut ManagedTableTransaction, i32) -> IcebergResult<()>,
    {
        self.register_table(relid);
        let nest_level = current_nest_level();
        let mut inner = self.inner.borrow_mut();
        let state = inner.tables.get_mut(&relid).ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "Table {} not registered in metadata tracker",
                relid
            ))
        })?;
        if state.file_io.is_none() {
            state.file_io = Some(file_io.clone());
        }
        mutation(&mut state.transaction, nest_level)
    }

    /// Read-side entry point for scans and planner statistics.
    ///
    /// Reads the latest committed metadata location every time, then attaches
    /// any transaction-local schema update and file delta for this relation.
    /// That gives Read Committed behavior without writing statement-time
    /// metadata files.
    pub fn current_table_metadata(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<LoadedTableMetadata> {
        let actions = {
            let mut inner = self.inner.borrow_mut();
            match inner.tables.get_mut(&relid) {
                Some(state) => {
                    if state.file_io.is_none() {
                        state.file_io = Some(file_io.clone());
                    }
                    Some(Rc::clone(&state.transaction.actions))
                }
                None => None,
            }
        };

        let catalog_metadata = IcebergMetadata::get(relid)?;
        let location = catalog_metadata
            .metadata_location
            .ok_or(IcebergError::MetadataLocationNull)?;
        let mut metadata = TableMetadata::read_from(file_io, &location)?;
        let delta = if let Some(actions) = actions {
            metadata = actions.overlay_metadata(metadata)?;
            actions.combined_delta()?
        } else {
            None
        };
        Ok(LoadedTableMetadata {
            location,
            maintenance_due_at: catalog_metadata.maintenance_due_at,
            metadata,
            delta,
        })
    }

    /// Write-side entry point for mutation.
    ///
    /// Registers the relation with this transaction's tracker (idempotent),
    /// then returns the latest committed metadata plus any prior
    /// transaction-local schema update and file delta for statement-local
    /// reads.
    ///
    /// This is the single supported way for a writer to obtain its base
    /// snapshot: it bundles `register_table` with the metadata read so a
    /// caller cannot accidentally observe metadata without enrolling the
    /// table in the tracker.
    pub fn begin_table_modify(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<LoadedTableMetadata> {
        self.register_table(relid);
        self.current_table_metadata(relid, file_io)
    }

    /// Return the transaction-scoped physical-row registry for one relation.
    ///
    /// The clone is a single-backend `Rc` handle. Callers retain it across row
    /// callbacks so the hot path does not re-enter the relation HashMap.
    pub(crate) fn row_registry(
        &self,
        relid: pg_sys::Oid,
    ) -> IcebergResult<RelationRowRegistry> {
        self.register_table(relid);
        let inner = self.inner.try_borrow().map_err(|_| {
            IcebergError::InvariantViolated(
                "transaction metadata tracker is already mutably borrowed",
            )
        })?;
        inner
            .tables
            .get(&relid)
            .map(|state| state.transaction.row_registry.clone())
            .ok_or_else(|| {
                IcebergError::MetadataTracker(format!(
                    "table {relid} has no row registry"
                ))
            })
    }

    /// Detach one table's immutable commit input before any metadata I/O.
    fn commit_input(
        &self,
        relid: pg_sys::Oid,
    ) -> IcebergResult<Option<TableCommitInput>> {
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
            IcebergError::MetadataTracker(format!(
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
            state.transaction.rollback_to_level(target_level);
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
            state.transaction.promote_to_level(from_level);
        }
    }
}

// =============================================================================
// TransactionResource integration
// =============================================================================

impl TransactionResource for TxMetadata {
    /// Pinned at top-level on purpose: `commit_all` must run for the entire
    /// transaction's accumulated changes regardless of any savepoint
    /// promotions, so this resource never participates in nest-level
    /// promotion (see `set_nest_level`).
    fn nest_level(&self) -> i32 {
        1
    }

    /// No-op: the framework's `set_nest_level` is only relevant to resources
    /// whose lifetime is tied to a specific savepoint. `TxMetadata` is
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
            .any(|state| !state.transaction.actions.is_empty());
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

    fn on_abort(&self) {
        // The PG transaction is rolling back; no compensating writes needed.
        CURRENT.with(|slot| *slot.borrow_mut() = None);
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        // RELEASE SAVEPOINT promotes the released subtransaction's state to
        // its parent. The framework only updates `nest_level()` on the
        // resource itself, but per-frame and mutation-owner `nest_level`
        // values are owned here and must be promoted too.
        // See `TxMetadata::promote_to_level` for why this is required.
        self.promote_to_level(current_nest_level);
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        // `current_nest_level` is the level being aborted, matching pg-lakebase-core's
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
