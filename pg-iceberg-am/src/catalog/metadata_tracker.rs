//! Per-transaction Iceberg metadata tracking.
//!
//! [`TxMetadata`] owns the bookkeeping for every Iceberg table modified inside
//! a single PostgreSQL top-level transaction:
//!
//! - the metadata location each statement produced,
//! - the global metadata location each rebase was based on,
//! - the data files accumulated across statements,
//! - a per-savepoint history stack used to roll back on
//!   `ROLLBACK TO SAVEPOINT`.
//!
//! It also implements [`TransactionResource`] so the pg-lakebase-core
//! transaction framework can drive it through pre-commit / commit / abort /
//! sub-abort callbacks.
//!
//! The single thread-local `Rc<TxMetadata>` is reached through
//! [`TxMetadata::current`]; callers (DML, scan) never touch the TLS directly.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{DataFile, TableMetadata};
use iceberg_lite::table::Table;
use iceberg_lite::transaction::Transaction;
use pg_lakebase_core::diag;
use pg_lakebase_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;

use crate::catalog::bridge::{IcebergTableId, StagedCatalog};
use crate::catalog::metadata_table::{CasUpdate, IcebergMetadata};
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;

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
    prev_metadata_location: Option<String>,
    prev_files_count: usize,
    prev_last_base: Option<String>,
}

/// Metadata location bookkeeping for a single Iceberg table inside one
/// top-level transaction.
#[derive(Debug)]
struct TableState {
    /// PostgreSQL OID - the authoritative identity for the relation.
    relid: pg_sys::Oid,

    /// Metadata location written by this transaction's most recent statement.
    current_metadata_location: Option<String>,

    /// The global metadata location our most recent rebase sat on top of.
    /// Used both as the CAS expected value at commit time and as a
    /// fast-path key: if the global location hasn't moved, the current
    /// location is still valid and we can skip the rebase.
    last_base_metadata_location: Option<String>,

    /// Files accumulated across every statement in this transaction:
    /// `[stmt_1_files .. stmt_2_files .. ...]`.
    ///
    /// Stored as `Arc<DataFile>` so the tracker's own bookkeeping (history
    /// frames, savepoint truncation, multiple rebases inside one transaction)
    /// only bumps reference counts. Note that handing the files off to
    /// iceberg-lite's `add_data_files` still performs a deep clone per call
    /// because that API is owned-only. See the TODO in `rebase_inner` for
    /// the upstream change required to remove that last copy.
    accumulated_data_files: Vec<Arc<DataFile>>,

    /// FileIO captured from the first statement to drive the final commit.
    file_io: Option<FileIO>,

    /// Transaction nest level at which this table was first registered.
    /// Used by sub-abort to drop tables that only existed inside the
    /// rolled-back savepoint.
    first_modified_at_level: i32,

    /// Savepoint history stack. Each frame is the state BEFORE a write at
    /// `nest_level`, so sub-abort can restore it by popping frames whose
    /// `nest_level >= aborted_level`.
    level_history: Vec<HistoryFrame>,
}

impl TableState {
    fn new(relid: pg_sys::Oid, nest_level: i32, base: Option<String>) -> Self {
        Self {
            relid,
            current_metadata_location: base.clone(),
            last_base_metadata_location: base,
            accumulated_data_files: Vec::new(),
            file_io: None,
            first_modified_at_level: nest_level,
            level_history: Vec::new(),
        }
    }

    /// Push a history frame and apply the new state.
    ///
    /// `new_data_files` are already `Arc`-wrapped by the caller so each file
    /// is heap-allocated exactly once for the life of the transaction.
    fn record_change(
        &mut self,
        nest_level: i32,
        new_metadata_location: String,
        new_data_files: Vec<Arc<DataFile>>,
    ) {
        self.level_history.push(HistoryFrame {
            nest_level,
            prev_metadata_location: self.current_metadata_location.clone(),
            prev_files_count: self.accumulated_data_files.len(),
            prev_last_base: self.last_base_metadata_location.clone(),
        });
        self.current_metadata_location = Some(new_metadata_location);
        self.accumulated_data_files.extend(new_data_files);
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
            self.current_metadata_location = frame.prev_metadata_location;
            self.accumulated_data_files
                .truncate(frame.prev_files_count);
            self.last_base_metadata_location = frame.prev_last_base;
        }
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
        if self.first_modified_at_level >= from_level {
            self.first_modified_at_level = from_level - 1;
        }
        for frame in &mut self.level_history {
            if frame.nest_level >= from_level {
                frame.nest_level = from_level - 1;
            }
        }
    }
}

// =============================================================================
// Transaction-level metadata context
// =============================================================================

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
            transaction::register_resource(Rc::clone(&me) as Rc<dyn TransactionResource>);
            *slot = Some(Rc::clone(&me));
            me
        })
    }

    /// Register a table the first time this transaction touches it. Idempotent.
    pub fn register_table(&self, relid: pg_sys::Oid) -> IcebergResult<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.tables.contains_key(&relid) {
            return Ok(());
        }
        let nest_level = current_nest_level();
        let iceberg_meta = IcebergMetadata::get(relid)?;
        let state = TableState::new(relid, nest_level, iceberg_meta.metadata_location);
        inner.tables.insert(relid, state);
        Ok(())
    }

    /// Statement path: rebase pending changes on top of the latest global
    /// metadata, append `new_data_files` (may be empty for a read-side rebase),
    /// generate a new intermediate metadata file, and push a history frame so
    /// a later `ROLLBACK TO SAVEPOINT` can roll this statement back.
    ///
    /// `new_data_files` enters the tracker as `Arc<DataFile>` exactly once,
    /// so the tracker's own bookkeeping (history frames, savepoint
    /// truncation, multiple in-transaction rebases) only bumps reference
    /// counts. The handoff to iceberg-lite's owned `add_data_files` API
    /// inside `rebase_inner` still performs a deep clone per call. See the
    /// TODO there.
    ///
    /// Under Read Committed isolation we continuously rebase on the latest
    /// global state to absorb concurrent commits.
    pub fn rebase_for_statement(
        &self,
        relid: pg_sys::Oid,
        new_data_files: Vec<DataFile>,
        file_io: &FileIO,
    ) -> IcebergResult<String> {
        // Wrap in Arc once. Tracker-internal copies (history frames,
        // accumulated list, retry replay reads) are reference-count bumps
        // only; the deep clone happens just before iceberg-lite consumes
        // owned DataFile values in rebase_inner.
        let new_data_files: Vec<Arc<DataFile>> =
            new_data_files.into_iter().map(Arc::new).collect();
        let nest_level = current_nest_level();
        let mut inner = self.inner.borrow_mut();
        match Self::rebase_inner(&mut inner, relid, &new_data_files, file_io)? {
            None => current_or_err(&inner, relid),
            Some((latest_global, new_meta)) => {
                let state = inner.tables.get_mut(&relid).expect("registered");
                state.last_base_metadata_location = Some(latest_global);
                state.record_change(nest_level, new_meta.clone(), new_data_files);
                Ok(new_meta)
            }
        }
    }

    /// Commit path: rebase pending changes onto the latest global metadata so
    /// the subsequent CAS update can succeed. Does NOT push a history frame:
    /// the tracker is cleared in `on_commit` immediately after `commit_all`,
    /// so any frame would be dead state.
    fn rebase_for_commit(
        inner: &mut TxMetadataInner,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<String> {
        match Self::rebase_inner(inner, relid, &[], file_io)? {
            None => current_or_err(inner, relid),
            Some((latest_global, new_meta)) => {
                let state = inner.tables.get_mut(&relid).expect("registered");
                state.last_base_metadata_location = Some(latest_global);
                state.current_metadata_location = Some(new_meta.clone());
                Ok(new_meta)
            }
        }
    }

    /// Read the current metadata location for `relid`.
    ///
    /// - If the table has been written in this transaction, rebase first so
    ///   the caller observes any concurrent commits since our last
    ///   statement, then return the intermediate metadata location.
    /// - Otherwise, return the catalog's latest metadata location directly.
    ///
    /// This is the single read entry point for both DML startup and scans.
    pub fn current_metadata_location(
        &self,
        relid: pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<String> {
        let already_tracked = self.inner.borrow().tables.contains_key(&relid);
        if already_tracked {
            return self.rebase_for_statement(relid, Vec::new(), file_io);
        }
        IcebergMetadata::get(relid)?
            .metadata_location
            .ok_or(IcebergError::MetadataLocationNull)
    }

    // -------------------------------------------------------------------------
    // Private rebase core
    // -------------------------------------------------------------------------

    /// Loads the latest global metadata, takes the fast path if possible,
    /// otherwise replays accumulated files + (optional) `new_data_files` on
    /// top of the latest global base and writes a new intermediate metadata
    /// file.
    ///
    /// Returns `Ok(None)` for the fast path, `Ok(Some((latest_global, new)))`
    /// when a new file was produced. Bookkeeping (`current`, `last_base`,
    /// history) is intentionally left to the wrappers since their semantics
    /// differ.
    fn rebase_inner(
        inner: &mut TxMetadataInner,
        relid: pg_sys::Oid,
        new_data_files: &[Arc<DataFile>],
        file_io: &FileIO,
    ) -> IcebergResult<Option<(String, String)>> {
        let state = inner.tables.get_mut(&relid).ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "Table {} not registered in metadata tracker",
                relid
            ))
        })?;

        // Cache FileIO for final commit
        if state.file_io.is_none() {
            state.file_io = Some(file_io.clone());
        }

        // 1. Get latest global metadata
        let current_global = IcebergMetadata::get(relid)?;
        let latest_global_location =
            current_global.metadata_location.ok_or_else(|| {
                IcebergError::MetadataTracker(format!(
                    "Metadata location is null for table {}",
                    relid
                ))
            })?;

        // Fast path: no new files AND last rebase already sat on the latest
        // global metadata. Current location is still valid.
        if new_data_files.is_empty()
            && state.last_base_metadata_location.as_ref()
                == Some(&latest_global_location)
        {
            return Ok(None);
        }

        // 2. Load base table.
        // The Iceberg `TableIdent` is synthesized from the PG OID through
        // `IcebergTableId` so the only place that maps `Oid -> TableIdent`
        // is `bridge`.
        let metadata = TableMetadata::read_from(file_io, &latest_global_location)?;
        let base_table = Table::builder()
            .metadata_location(latest_global_location.clone())
            .metadata(metadata)
            .identifier(IcebergTableId::for_relation(state.relid).into_table_ident())
            .file_io(file_io.clone())
            .build()?;

        // 3. Storage-only catalog wrapper. The catalog derives its FileIO
        //    from `base_table` itself, so manifest reads and the new
        //    metadata file write share a single IO context.
        let catalog = StagedCatalog::new(&base_table);

        // 4. Apply pending changes through a fast-append.
        //
        // TODO(metadata-rebase): Avoid deep-cloning accumulated `DataFile`
        // values when handing them to iceberg-lite. The accumulated state
        // is already shared as `Arc<DataFile>` so multiple rebases inside
        // the same transaction share a single allocation, but the
        // `add_data_files(impl IntoIterator<Item = DataFile>)` API in
        // iceberg-lite still demands owned values, forcing a deep clone
        // here on every rebase / CAS retry.
        //
        // Long-term fix lives in iceberg-lite (do NOT modify it from this
        // crate; we sync it from upstream iceberg-rust): introduce
        // `add_data_file_refs(&[Arc<DataFile>])` (or borrowed equivalent)
        // through `FastAppendAction` -> `SnapshotProducer` so retries
        // re-apply the same logical append without reallocating
        // `DataFile` internals.
        let tx = Transaction::new(&base_table);
        let mut append_action = tx.fast_append();
        if !state.accumulated_data_files.is_empty() {
            append_action = append_action.add_data_files(
                state
                    .accumulated_data_files
                    .iter()
                    .map(|f| (**f).clone()),
            );
        }
        if !new_data_files.is_empty() {
            append_action = append_action
                .add_data_files(new_data_files.iter().map(|f| (**f).clone()));
        }

        // 5. Commit the transaction to materialise a new metadata file.
        use iceberg_lite::transaction::ApplyTransactionAction;
        let tx = append_action.apply(tx)?;
        let updated_table = tx.commit(&catalog)?;

        let new_metadata_location = updated_table
            .metadata_location()
            .ok_or(IcebergError::MetadataLocationNull)?
            .to_string();

        Ok(Some((latest_global_location, new_metadata_location)))
    }

    // -------------------------------------------------------------------------
    // Top-level commit
    // -------------------------------------------------------------------------

    /// Commit every tracked table to the catalog with optimistic concurrency
    /// control.
    ///
    /// Per table: rebase pending changes onto the latest global metadata,
    /// then attempt a CAS update from `last_base` to the new intermediate
    /// metadata location. On `MetadataCatalogConflict`, rebase and retry up
    /// to `gucs::max_commit_retries()`.
    ///
    /// TODO(metadata-rebase): Each retry currently re-runs the full rebase:
    /// re-read catalog, re-read global metadata, rebuild Table, re-`fast_append`
    /// every accumulated DataFile, and write a fresh metadata file. Cost is
    /// `O(retries * accumulated_files * manifest IO)`. Acceptable short-term
    /// because `max_commit_retries` bounds the loop, but the long-term fix is
    /// an append-log replay model in iceberg-lite (see TODO in
    /// `rebase_inner`) that lets us re-apply the same logical append without
    /// reallocating the physical metadata each time.
    fn commit_all(&self) -> IcebergResult<()> {
        let mut inner = self.inner.borrow_mut();
        let table_oids: Vec<pg_sys::Oid> = inner.tables.keys().copied().collect();

        for relid in table_oids {
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

                let file_io = inner
                    .tables
                    .get(&relid)
                    .and_then(|s| s.file_io.clone())
                    .unwrap_or_else(FileIO::memory);

                // 1. Rebase pending changes onto the latest global metadata.
                //    Does not push history (tracker is about to be cleared).
                let new_metadata_location =
                    Self::rebase_for_commit(&mut inner, relid, &file_io)?;

                // 2. The CAS expected value is the global metadata we just
                //    rebased on (= state.last_base after rebase).
                let last_base = inner
                    .tables
                    .get(&relid)
                    .expect("registered")
                    .last_base_metadata_location
                    .clone();

                // 3. CAS update against the catalog.
                //
                // Standard Iceberg architecture alignment:
                // - `IcebergMetadata::cas_update` plays the role of the
                //   "catalog" (Hive-style), enforcing strict CAS.
                // - This loop plays the role of "snapshot producer client",
                //   handling retry/rebase. Under Read Committed we MUST
                //   rebase appends to avoid aborting the transaction.
                match IcebergMetadata::cas_update(
                    relid,
                    last_base.as_deref(),
                    CasUpdate {
                        metadata_location: Some(&new_metadata_location),
                        previous_metadata_location: last_base.as_deref(),
                    },
                ) {
                    Ok(()) => break,
                    Err(IcebergError::MetadataCatalogConflict) => {
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

    // -------------------------------------------------------------------------
    // Sub-abort
    // -------------------------------------------------------------------------

    /// Roll back every tracked table to `target_level`.
    ///
    /// Two cleanups:
    /// 1. Drop tables that were first registered at or above `target_level`
    ///    (they only existed inside the rolled-back savepoint).
    /// 2. Pop history frames at or above `target_level` from surviving tables.
    fn rollback_to_level(&self, target_level: i32) {
        let mut inner = self.inner.borrow_mut();
        inner.tables.retain(|_, state| {
            if state.first_modified_at_level >= target_level {
                false
            } else {
                state.rollback_to_level(target_level);
                true
            }
        });
    }

    /// Promote every table's internal nest levels down to `from_level - 1`.
    ///
    /// Called from `on_commit_sub` to mirror PostgreSQL's `RELEASE SAVEPOINT`
    /// reparenting: state owned by the released subtransaction now belongs
    /// to its parent, so its recorded nest level must drop accordingly.
    /// Without this, a sibling savepoint opened later at the same nest
    /// level would alias the released one. A `ROLLBACK TO` of the sibling
    /// would then incorrectly discard already-promoted writes (or worse,
    /// drop the entire `TableState` if the table was first registered
    /// inside the released savepoint).
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
        // resource itself, but our per-table `first_modified_at_level` and
        // per-frame `nest_level` are owned here and must be promoted too.
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

fn current_or_err(
    inner: &TxMetadataInner,
    relid: pg_sys::Oid,
) -> IcebergResult<String> {
    inner
        .tables
        .get(&relid)
        .expect("registered")
        .current_metadata_location
        .clone()
        .ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "Current metadata location is null for table {}",
                relid
            ))
        })
}
