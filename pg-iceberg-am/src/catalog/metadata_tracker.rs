//! Per-transaction Iceberg metadata tracking.
//!
//! [`TxMetadata`] owns the bookkeeping for every Iceberg table modified inside
//! a single PostgreSQL top-level transaction:
//!
//! - the transaction-local Iceberg file delta accumulated across statements,
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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::overlay::{SnapshotDelta, SnapshotDeltaMarker};
use iceberg_lite::spec::{
    DataContentType, DataFile, ManifestContentType, TableMetadata,
};
use iceberg_lite::table::Table;
use iceberg_lite::transaction::{
    ApplyTransactionAction, RowDeltaValidation, Transaction,
};
use pg_lakebase_core::diag;
use pg_lakebase_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;

use crate::catalog::bridge::{IcebergTableId, StagedCatalog};
use crate::catalog::metadata_table::{CasUpdate, IcebergMetadata};
use crate::catalog::row_mutations::RelationRowRegistry;
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;
use crate::storage::transactional_artifacts::MetadataAttempt;

const TOTAL_RECORDS: &str = "total-records";
const TOTAL_FILES_SIZE: &str = "total-files-size";

// =============================================================================
// Per-table state
// =============================================================================

/// One savepoint frame on a table's history stack.
///
/// Captures the state BEFORE a statement that wrote to this table at
/// `nest_level`, so a later `ROLLBACK TO SAVEPOINT` can restore it.
#[derive(Debug, Clone)]
struct HistoryFrame {
    nest_level: i32,
    marker: SnapshotDeltaMarker,
    validation_len: usize,
}

/// Metadata location bookkeeping for a single Iceberg table inside one
/// top-level transaction.
#[derive(Debug)]
struct TableState {
    /// Transaction-local file operations layered over the latest committed
    /// Iceberg metadata for statement-local reads.
    ///
    /// Stored behind `Arc` so scan specs can hold a stable statement view.
    /// Later mutation calls mutate through `Arc::make_mut`, preserving any older
    /// scan's snapshot of the delta.
    delta: Arc<SnapshotDelta>,

    /// FileIO captured from the first statement to drive the final commit.
    file_io: Option<FileIO>,

    /// Savepoint history stack. Each frame is the state BEFORE a write at
    /// `nest_level`, so sub-abort can restore it by popping frames whose
    /// `nest_level >= aborted_level`.
    ///
    /// Top-level writes do not need frames: top-level abort drops the whole
    /// tracker, and top-level commit never rolls back through this stack.
    level_history: Vec<HistoryFrame>,

    /// Row-level conflict validations that must be checked at Iceberg commit
    /// time before materializing this transaction's delta.
    validations: Vec<RowDeltaValidation>,

    /// Physical-row claims used to reproduce PostgreSQL `TM_SelfModified`
    /// semantics across sibling ModifyTable nodes and nested SPI executions.
    row_registry: RelationRowRegistry,
}

/// Owned per-table state detached from the tracker before commit I/O begins.
///
/// Keeping this snapshot owned releases the [`RefCell`] borrow on
/// [`TxMetadataInner`] before metadata reads, transaction materialization, and
/// catalog CAS retries.
struct TableCommitInput {
    delta: Arc<SnapshotDelta>,
    validations: Vec<RowDeltaValidation>,
    file_io: FileIO,
}

impl TableState {
    fn new() -> Self {
        Self {
            delta: Arc::new(SnapshotDelta::new()),
            file_io: None,
            level_history: Vec::new(),
            validations: Vec::new(),
            row_registry: RelationRowRegistry::default(),
        }
    }

    fn record_history(&mut self, nest_level: i32) -> (SnapshotDeltaMarker, bool) {
        let marker = self.delta.mark();
        let should_record_history = nest_level > 1;
        if should_record_history {
            self.level_history.push(HistoryFrame {
                nest_level,
                marker,
                validation_len: self.validations.len(),
            });
        }
        (marker, should_record_history)
    }

    fn record_delta_mutation<F>(
        &mut self,
        nest_level: i32,
        mutation: F,
    ) -> IcebergResult<()>
    where
        F: FnOnce(&mut SnapshotDelta) -> iceberg_lite::Result<()>,
    {
        let (marker, should_record_history) = self.record_history(nest_level);
        let delta = Arc::make_mut(&mut self.delta);
        if let Err(err) = mutation(delta) {
            delta.truncate(marker);
            if should_record_history {
                self.level_history.pop();
            }
            return Err(err.into());
        }

        Ok(())
    }

    fn record_validation(&mut self, nest_level: i32, validation: RowDeltaValidation) {
        self.record_history(nest_level);
        self.validations.push(validation);
    }

    fn record_data_files(
        &mut self,
        nest_level: i32,
        new_data_files: Vec<DataFile>,
    ) -> IcebergResult<()> {
        if new_data_files.is_empty() {
            return Ok(());
        }

        self.record_delta_mutation(nest_level, |delta| {
            for data_file in new_data_files {
                delta.add_data_file(data_file)?;
            }
            Ok(())
        })
    }

    fn record_position_delete_file(
        &mut self,
        nest_level: i32,
        delete_file: DataFile,
        referenced_data_files: Vec<String>,
    ) -> IcebergResult<()> {
        self.record_delta_mutation(nest_level, |delta| {
            delta.add_position_delete_file(delete_file, referenced_data_files)?;
            Ok(())
        })
    }

    fn record_remove_data_file(
        &mut self,
        nest_level: i32,
        file_path: String,
    ) -> IcebergResult<()> {
        self.record_delta_mutation(nest_level, |delta| {
            delta.remove_data_file(file_path)?;
            Ok(())
        })
    }

    /// Roll back every history frame whose `nest_level >= target_level`.
    ///
    /// Matches PostgreSQL's `ROLLBACK TO SAVEPOINT` semantics: the target
    /// savepoint's own subtransaction is aborted (and recreated under the
    /// same name), so writes performed *at* `target_level` are discarded
    /// along with anything deeper.
    ///
    /// PostgreSQL implements `ROLLBACK TO SAVEPOINT` by aborting every
    /// nested subtransaction up to and including the named savepoint, then
    /// recreating the named savepoint fresh. Each abort fires its own
    /// `SUBXACT_EVENT_ABORT_SUB`, so this method is called once per level
    /// from innermost outwards:
    ///
    /// ```text
    /// Level 1: BEGIN
    /// Level 2: SAVEPOINT sp1   (writes A, frame.nest_level = 2)
    /// Level 3: SAVEPOINT sp2   (writes B, frame.nest_level = 3)
    /// ROLLBACK TO sp1
    ///   on_abort_sub(3) -> rollback_to_level(3)  drops B
    ///   on_abort_sub(2) -> rollback_to_level(2)  drops A
    ///   PostgreSQL recreates sp1 at level 2, empty.
    /// ```
    fn rollback_to_level(&mut self, target_level: i32) {
        while let Some(frame) = self.level_history.last() {
            if frame.nest_level < target_level {
                break;
            }
            let frame = self.level_history.pop().unwrap();
            Arc::make_mut(&mut self.delta).truncate(frame.marker);
            self.validations.truncate(frame.validation_len);
        }
        self.row_registry.rollback_to_level(target_level);
    }

    /// Promote every nest level `>= from_level` down to `from_level - 1`.
    ///
    /// Mirrors `transactional_artifacts::handle_commit_sub`: when a
    /// `RELEASE SAVEPOINT` fires for `from_level`, every state owned by
    /// that subtransaction (and any lingering deeper frames) belongs to
    /// the parent now, so its recorded nest level must drop accordingly.
    ///
    /// Without this, a sibling `SAVEPOINT` opened at `from_level` after the
    /// release would alias the released savepoint's level, and a
    /// `ROLLBACK TO` of the sibling would incorrectly throw away changes
    /// that were already promoted to the parent.
    fn promote_to_level(&mut self, from_level: i32) {
        for frame in &mut self.level_history {
            if frame.nest_level >= from_level {
                frame.nest_level = from_level - 1;
            }
        }
        // Frames promoted to top-level are no longer useful: top-level abort
        // drops the whole tracker, and sibling savepoints must not roll them
        // back.
        self.level_history.retain(|frame| frame.nest_level > 1);
        self.row_registry.promote_to_level(from_level);
    }
}

// =============================================================================
// Transaction-level metadata context
// =============================================================================

/// Outcome of a metadata read for one table inside the current transaction.
///
/// Pairs the resolved metadata location with the parsed `TableMetadata` so
/// callers do not have to perform the same `read_from` against `FileIO`
/// twice. Returned by [`TxMetadata::current_table_metadata`] and
/// [`TxMetadata::begin_table_modify`].
#[derive(Debug)]
pub struct LoadedTableMetadata {
    pub location: String,
    pub metadata: TableMetadata,
    pub delta: Option<Arc<SnapshotDelta>>,
}

impl LoadedTableMetadata {
    /// Return planner-facing relation statistics for the committed metadata
    /// plus this transaction's staged delta.
    ///
    /// Mirrors Iceberg snapshot-summary totals closely enough for PostgreSQL
    /// planner sizing: data-file appends add rows and bytes, delete-file
    /// appends add only bytes, and data-file removes subtract the committed
    /// file's rows and bytes.
    pub(crate) fn relation_stats(
        &self,
        file_io: &FileIO,
    ) -> IcebergResult<(u64, u64)> {
        let mut rows = Self::summary_u64(&self.metadata, TOTAL_RECORDS).unwrap_or(0);
        let mut bytes =
            Self::summary_u64(&self.metadata, TOTAL_FILES_SIZE).unwrap_or(0);

        let Some(delta) = self.delta.as_ref() else {
            return Ok((rows, bytes));
        };

        let delta_stats = delta.stats();
        rows = rows.saturating_add(delta_stats.added_data_records);
        bytes = bytes
            .saturating_add(delta_stats.added_data_file_bytes)
            .saturating_add(delta_stats.added_delete_file_bytes);

        self.subtract_removed_data_file_stats(
            file_io,
            &delta_stats.removed_data_paths,
            &mut rows,
            &mut bytes,
        )?;

        Ok((rows, bytes))
    }

    fn has_live_data_file_path(
        &self,
        file_io: &FileIO,
        file_path: &str,
    ) -> IcebergResult<bool> {
        if file_path.is_empty() {
            return Ok(false);
        }

        if let Some(delta) = self.delta.as_ref() {
            if delta.has_live_added_data_file_path(file_path) {
                return Ok(true);
            }
            if delta.has_removed_data_path(file_path) {
                return Ok(false);
            }
        }

        let Some(snapshot) = self.metadata.current_snapshot() else {
            return Ok(false);
        };
        let manifest_list = snapshot.load_manifest_list(file_io, &self.metadata)?;
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != iceberg_lite::spec::ManifestContentType::Data
            {
                continue;
            }
            let manifest = manifest_file.load_manifest(file_io)?;
            if manifest.entries().iter().any(|entry| {
                entry.is_alive()
                    && entry.content_type() == DataContentType::Data
                    && entry.file_path() == file_path
            }) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn summary_u64(metadata: &TableMetadata, key: &str) -> Option<u64> {
        metadata
            .current_snapshot()
            .and_then(|snapshot| snapshot.summary().additional_properties.get(key))
            .and_then(|value| value.parse::<u64>().ok())
    }

    fn subtract_removed_data_file_stats(
        &self,
        file_io: &FileIO,
        removed_paths: &[String],
        rows: &mut u64,
        bytes: &mut u64,
    ) -> IcebergResult<()> {
        if removed_paths.is_empty() {
            return Ok(());
        }

        let Some(snapshot) = self.metadata.current_snapshot() else {
            return Ok(());
        };

        let mut remaining: HashSet<&str> =
            removed_paths.iter().map(String::as_str).collect();
        let manifest_list = snapshot.load_manifest_list(file_io, &self.metadata)?;
        for manifest_file in manifest_list.entries() {
            if remaining.is_empty() {
                break;
            }
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }

            let manifest = manifest_file.load_manifest(file_io)?;
            for entry in manifest.entries() {
                if entry.is_alive()
                    && entry.content_type() == DataContentType::Data
                    && remaining.remove(entry.file_path())
                {
                    *rows = rows.saturating_sub(entry.record_count());
                    *bytes = bytes.saturating_sub(entry.file_size_in_bytes());
                }
            }
        }

        Ok(())
    }
}

/// Mutable state hidden inside [`TxMetadata`]'s `RefCell`.
#[derive(Debug, Default)]
struct TxMetadataInner {
    tables: HashMap<pg_sys::Oid, TableState>,
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
        let state = TableState::new();
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
        self.stage_delta_mutation(relid, file_io, |state, nest_level| {
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
        self.stage_delta_mutation(relid, file_io, |state, nest_level| {
            state.record_position_delete_file(
                nest_level,
                delete_file,
                referenced_data_files,
            )
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
        self.stage_delta_mutation(relid, file_io, |state, nest_level| {
            state.record_validation(nest_level, validation);
            Ok(())
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

    /// Statement write path: remove a live data file by path in the
    /// transaction-local delta without generating Iceberg metadata files.
    ///
    /// This is intentionally a single-file staging API. Future SQL
    /// DELETE/UPDATE plumbing that removes many files should add a batch API
    /// that collects the statement's live data-file paths once; looping over
    /// this method would repeatedly walk the manifest entries, even when the
    /// ObjectCache amortizes manifest IO.
    pub fn stage_remove_data_file(
        &self,
        relid: pg_sys::Oid,
        file_path: impl Into<String>,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        let file_path = file_path.into();
        let loaded = self.begin_table_modify(relid, file_io)?;
        if !loaded.has_live_data_file_path(file_io, &file_path)? {
            return Err(IcebergError::MetadataTracker(format!(
                "Cannot remove non-live Iceberg data file from transaction view: {}",
                file_path
            )));
        }
        self.stage_delta_mutation(relid, file_io, |state, nest_level| {
            state.record_remove_data_file(nest_level, file_path)
        })
    }

    fn stage_delta_mutation<F>(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
        mutation: F,
    ) -> IcebergResult<()>
    where
        F: FnOnce(&mut TableState, i32) -> IcebergResult<()>,
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
        mutation(state, nest_level)
    }

    /// Read-side entry point for scans and planner statistics.
    ///
    /// Reads the latest committed metadata location every time, then attaches
    /// any transaction-local delta for this relation. That gives Read
    /// Committed behavior without writing statement-time metadata files.
    pub fn current_table_metadata(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<LoadedTableMetadata> {
        let delta = {
            let mut inner = self.inner.borrow_mut();
            inner.tables.get_mut(&relid).and_then(|state| {
                if state.file_io.is_none() {
                    state.file_io = Some(file_io.clone());
                }
                (!state.delta.is_empty()).then(|| Arc::clone(&state.delta))
            })
        };

        let location = IcebergMetadata::get(relid)?
            .metadata_location
            .ok_or(IcebergError::MetadataLocationNull)?;
        let metadata = TableMetadata::read_from(file_io, &location)?;
        Ok(LoadedTableMetadata {
            location,
            metadata,
            delta,
        })
    }

    /// Write-side entry point for mutation.
    ///
    /// Registers the relation with this transaction's tracker (idempotent),
    /// then returns the latest committed metadata plus any prior
    /// transaction-local delta for statement-local reads.
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
            .map(|state| state.row_registry.clone())
            .ok_or_else(|| {
                IcebergError::MetadataTracker(format!(
                    "table {relid} has no row registry"
                ))
            })
    }

    // -------------------------------------------------------------------------
    // Top-level commit
    // -------------------------------------------------------------------------

    /// Commit every tracked table to the catalog with optimistic concurrency
    /// control.
    ///
    /// Per table: materialize the transaction-local delta on top of the latest
    /// global metadata, then attempt a CAS update from that base to the new
    /// metadata location. On `MetadataCatalogConflict`, rebase and retry up
    /// to `gucs::max_commit_retries()`.
    fn commit_all(&self) -> IcebergResult<()> {
        let table_oids: Vec<pg_sys::Oid> =
            self.inner.borrow().tables.keys().copied().collect();

        for relid in table_oids {
            let Some(TableCommitInput {
                delta,
                validations,
                file_io,
            }) = self.commit_input(relid)?
            else {
                continue;
            };

            let mut retries = 0;
            let max_retries = gucs::max_commit_retries();

            loop {
                if retries > max_retries {
                    return Err(IcebergError::MetadataCommitConflict {
                        relid,
                        max_retries,
                    });
                }
                retries += 1;

                let latest_global_location = IcebergMetadata::get(relid)?
                    .metadata_location
                    .ok_or(IcebergError::MetadataLocationNull)?;
                let metadata =
                    TableMetadata::read_from(&file_io, &latest_global_location)?;
                let base_table = Table::builder()
                    .metadata_location(latest_global_location.clone())
                    .metadata(metadata)
                    .identifier(
                        IcebergTableId::for_relation(relid).into_table_ident(),
                    )
                    .file_io(file_io.clone())
                    .build()?;

                let catalog = StagedCatalog::new(&base_table);
                let tx = Transaction::new(&base_table);
                // The tracker commits every staged overlay through
                // SnapshotDeltaAction/RowDeltaAction. AddData is an overlay
                // operation, so append-only and mixed append/delete/remove
                // transactions must share the same read and materialization
                // semantics. RowDelta adds Iceberg row-delta conflict validation
                // when DELETE/UPDATE/MERGE produced a statement delta.
                let tx = if validations.is_empty() {
                    tx.snapshot_delta(Arc::clone(&delta)).apply(tx)?
                } else {
                    tx.row_delta(Arc::clone(&delta))
                        .add_validations(validations.clone())
                        .apply(tx)?
                };
                // FileIO registrations during materialization belong to this
                // attempt until the catalog CAS decides whether they survive.
                let metadata_attempt = MetadataAttempt::begin()?;
                let updated_table = tx.commit(&catalog)?;
                let new_metadata_location = updated_table
                    .metadata_location()
                    .ok_or(IcebergError::MetadataLocationNull)?;
                if new_metadata_location == latest_global_location {
                    metadata_attempt.discard()?;
                    break;
                }

                match IcebergMetadata::cas_update(
                    relid,
                    Some(&latest_global_location),
                    CasUpdate {
                        metadata_location: Some(new_metadata_location),
                        previous_metadata_location: Some(&latest_global_location),
                    },
                ) {
                    Ok(()) => {
                        metadata_attempt.promote()?;
                        break;
                    }
                    Err(IcebergError::MetadataCatalogConflict) => {
                        metadata_attempt.discard()?;
                        diag::report_notice(
                            "Concurrent Iceberg update detected, rebasing...",
                        );
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }

    fn commit_input(
        &self,
        relid: pg_sys::Oid,
    ) -> IcebergResult<Option<TableCommitInput>> {
        let inner = self.inner.borrow();
        let Some(state) = inner.tables.get(&relid) else {
            return Ok(None);
        };
        if state.delta.is_empty() && state.validations.is_empty() {
            return Ok(None);
        }
        let file_io = state.file_io.clone().ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "table {} has staged Iceberg delta without FileIO",
                relid
            ))
        })?;
        Ok(Some(TableCommitInput {
            delta: Arc::clone(&state.delta),
            validations: state.validations.clone(),
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
    /// reused. Empty deltas are ignored by `commit_input`.
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
        self.commit_all()?;
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
