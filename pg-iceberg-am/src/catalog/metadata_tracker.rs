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

use std::cell::{OnceCell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::overlay::{DeleteFileIdentity, SnapshotDelta, SnapshotDeltaMarker};
use iceberg_lite::spec::{
    DataContentType, DataFile, ManifestContentType, TableMetadata,
};
use iceberg_lite::table::Table;
use iceberg_lite::transaction::{
    ApplyTransactionAction, PreparedSchemaUpdate, RowDeltaValidation, Transaction,
};
use pg_lakebase_core::diag::{self, PgReportError};
use pg_lakebase_core::transaction::{self, TransactionResource, TransactionResult};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

use crate::catalog::bridge::{IcebergTableId, StagedCatalog};
use crate::catalog::metadata_table::{
    CasUpdate, IcebergMetadata, MaintenanceScheduleUpdate,
};
use crate::catalog::row_mutations::RelationRowRegistry;
use crate::catalog::table_properties::PreparedTablePropertyUpdate;
use crate::error::{IcebergError, IcebergResult};
use crate::gucs;
use crate::maintenance::PreparedVacuum;
use crate::storage::transactional_artifacts::MetadataAttempt;

const TOTAL_RECORDS: &str = "total-records";
const TOTAL_FILES_SIZE: &str = "total-files-size";
const TOTAL_DELETE_FILES: &str = "total-delete-files";

// =============================================================================
// Per-table state
// =============================================================================

/// One savepoint frame on a table's history stack.
///
/// Captures the state BEFORE a statement that wrote to this table at
/// `nest_level`, so a later `ROLLBACK TO SAVEPOINT` can restore it.
#[derive(Debug, Clone, Copy)]
struct HistoryFrame {
    nest_level: i32,
    marker: TxTableActionLogMarker,
}

/// Metadata location bookkeeping for a single Iceberg table inside one
/// top-level transaction.
#[derive(Debug)]
struct TableState {
    /// FileIO captured from the first statement to drive the final commit.
    file_io: Option<FileIO>,

    /// Ordered transaction-local schema/data actions.
    actions: Rc<TxTableActionLog>,

    /// Savepoint history stack. Each frame is the state BEFORE a write at
    /// `nest_level`, so sub-abort can restore it by popping frames whose
    /// `nest_level >= aborted_level`.
    ///
    /// Top-level writes do not need frames: top-level abort drops the whole
    /// tracker, and top-level commit never rolls back through this stack.
    level_history: Vec<HistoryFrame>,

    /// Physical-row claims used to reproduce PostgreSQL `TM_SelfModified`
    /// semantics across sibling ModifyTable nodes and nested SPI executions.
    row_registry: RelationRowRegistry,
}

/// Owned per-table state detached from the tracker before commit I/O begins.
///
/// The action log is an immutable shared snapshot. Keeping it owned releases
/// the [`RefCell`] borrow on
/// [`TxMetadataInner`] before metadata reads, transaction materialization, and
/// catalog CAS retries.
struct TableCommitInput {
    actions: Rc<TxTableActionLog>,
    file_io: FileIO,
}

#[derive(Debug, Clone, Default)]
struct TxTableActionLog {
    actions: Vec<TxTableAction>,
    /// Cached transaction-local file overlay. It depends only on Data,
    /// Truncate, and Drop actions; schema actions are replayed separately onto
    /// the latest committed metadata and do not invalidate this value.
    combined_delta_cache: OnceCell<Option<Arc<SnapshotDelta>>>,
}

#[derive(Debug, Clone, Copy)]
struct TxTableActionLogMarker {
    action_len: usize,
    current_data_epoch: Option<TxDataEpochMarker>,
}

#[derive(Debug, Clone)]
enum TxTableAction {
    Schema(PreparedSchemaUpdate),
    Properties(PreparedTablePropertyUpdate),
    Data(TxDataEpoch),
    Truncate(TxTruncateAction),
    Vacuum(Box<PreparedVacuum>),
    Drop,
}

#[derive(Debug, Clone)]
struct TxTruncateAction {
    expected_metadata_location: String,
}

#[derive(Debug, Clone)]
struct TxDataEpoch {
    delta: Arc<SnapshotDelta>,
    validations: Vec<RowDeltaValidation>,
}

struct TxTableCommitPlan<'a> {
    actions: Vec<EffectiveCommitAction<'a>>,
    vacuum: Option<&'a PreparedVacuum>,
    expected_metadata_location: Option<&'a str>,
    canceled_created_paths: Vec<String>,
}

enum EffectiveCommitAction<'a> {
    Schema(&'a PreparedSchemaUpdate),
    Properties(&'a PreparedTablePropertyUpdate),
    Data {
        epoch: &'a TxDataEpoch,
        truncate_base: bool,
    },
    TruncateOnly,
}

#[derive(Debug, Clone, Copy)]
struct TxDataEpochMarker {
    delta: SnapshotDeltaMarker,
    validation_len: usize,
}

impl Default for TxDataEpoch {
    fn default() -> Self {
        Self {
            delta: Arc::new(SnapshotDelta::new()),
            validations: Vec::new(),
        }
    }
}

impl TxDataEpoch {
    fn is_empty(&self) -> bool {
        self.delta.is_empty() && self.validations.is_empty()
    }
}

impl TxTableActionLog {
    fn has_vacuum(&self) -> bool {
        self.actions
            .iter()
            .any(|action| matches!(action, TxTableAction::Vacuum(_)))
    }

    fn invalidate_combined_delta(&mut self) {
        self.combined_delta_cache.take();
    }

    fn last_truncate(&self) -> Option<(usize, &TxTruncateAction)> {
        self.actions
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, action)| match action {
                TxTableAction::Truncate(truncate) => Some((index, truncate)),
                TxTableAction::Schema(_)
                | TxTableAction::Properties(_)
                | TxTableAction::Data(_)
                | TxTableAction::Vacuum(_)
                | TxTableAction::Drop => None,
            })
    }

    fn commit_plan(&self) -> IcebergResult<TxTableCommitPlan<'_>> {
        if let Some(TxTableAction::Vacuum(vacuum)) = self.actions.first() {
            if self.actions.len() != 1 {
                return Err(IcebergError::InvariantViolated(
                    "VACUUM action is not exclusive in metadata tracker",
                ));
            }
            return Ok(TxTableCommitPlan {
                actions: Vec::new(),
                vacuum: Some(vacuum.as_ref()),
                expected_metadata_location: None,
                canceled_created_paths: Vec::new(),
            });
        }
        let last_truncate = self.last_truncate();
        let last_truncate_index = last_truncate.map(|(index, _)| index);
        let expected_metadata_location = last_truncate
            .map(|(_, truncate)| truncate.expected_metadata_location.as_str());
        let mut effective_actions = Vec::with_capacity(self.actions.len());
        let mut paths = HashSet::new();
        let mut pending_truncate = last_truncate.is_some();

        for (index, action) in self.actions.iter().enumerate() {
            match action {
                TxTableAction::Schema(update) => {
                    effective_actions.push(EffectiveCommitAction::Schema(update));
                }
                TxTableAction::Properties(update) => {
                    effective_actions.push(EffectiveCommitAction::Properties(update));
                }
                TxTableAction::Data(epoch) => {
                    if let Some(truncate_index) = last_truncate_index {
                        let canceled = if index < truncate_index {
                            epoch.delta.created_file_paths()
                        } else {
                            epoch.delta.canceled_created_file_paths()
                        };
                        paths.extend(canceled);

                        if index < truncate_index {
                            continue;
                        }
                    }
                    if epoch.is_empty() {
                        continue;
                    }
                    effective_actions.push(EffectiveCommitAction::Data {
                        epoch,
                        truncate_base: pending_truncate,
                    });
                    pending_truncate = false;
                }
                TxTableAction::Truncate(_) => {}
                TxTableAction::Vacuum(_) => {
                    return Err(IcebergError::InvariantViolated(
                        "non-exclusive VACUUM reached commit planning",
                    ));
                }
                TxTableAction::Drop => {
                    return Err(IcebergError::InvariantViolated(
                        "dropped table reached Iceberg commit planning",
                    ));
                }
            }
        }
        if pending_truncate {
            effective_actions.push(EffectiveCommitAction::TruncateOnly);
        }

        let mut canceled_created_paths: Vec<String> = paths.into_iter().collect();
        canceled_created_paths.sort_unstable();
        Ok(TxTableCommitPlan {
            actions: effective_actions,
            vacuum: None,
            expected_metadata_location,
            canceled_created_paths,
        })
    }

    fn mark(&self) -> TxTableActionLogMarker {
        let current_data_epoch = match self.actions.last() {
            Some(TxTableAction::Data(epoch)) => Some(TxDataEpochMarker {
                delta: epoch.delta.mark(),
                validation_len: epoch.validations.len(),
            }),
            _ => None,
        };

        TxTableActionLogMarker {
            action_len: self.actions.len(),
            current_data_epoch,
        }
    }

    fn truncate(&mut self, marker: TxTableActionLogMarker) {
        self.invalidate_combined_delta();
        self.actions.truncate(marker.action_len);
        if let Some(epoch_marker) = marker.current_data_epoch
            && let Some(TxTableAction::Data(epoch)) = self.actions.last_mut()
        {
            Arc::make_mut(&mut epoch.delta).truncate(epoch_marker.delta);
            epoch.validations.truncate(epoch_marker.validation_len);
        }
    }

    fn is_empty(&self) -> bool {
        self.actions.iter().all(|action| match action {
            TxTableAction::Schema(update) => update.is_empty(),
            TxTableAction::Properties(_) => false,
            TxTableAction::Data(epoch) => epoch.is_empty(),
            TxTableAction::Truncate(_) => false,
            TxTableAction::Vacuum(_) => false,
            TxTableAction::Drop => false,
        })
    }

    fn stage_schema(&mut self, update: PreparedSchemaUpdate) {
        if !update.is_empty() {
            self.actions.push(TxTableAction::Schema(update));
        }
    }

    fn stage_properties(&mut self, update: PreparedTablePropertyUpdate) {
        self.actions.push(TxTableAction::Properties(update));
    }

    fn stage_vacuum(&mut self, vacuum: PreparedVacuum) -> IcebergResult<()> {
        if !self.actions.is_empty() {
            return Err(IcebergError::Vacuum {
                source: crate::error::IcebergVacuumError::ActionConflict,
            });
        }
        self.invalidate_combined_delta();
        self.actions.push(TxTableAction::Vacuum(Box::new(vacuum)));
        Ok(())
    }

    fn current_data_epoch_mut(&mut self) -> &mut TxDataEpoch {
        self.invalidate_combined_delta();
        let needs_new_epoch =
            !matches!(self.actions.last(), Some(TxTableAction::Data(_)));
        if needs_new_epoch {
            self.actions
                .push(TxTableAction::Data(TxDataEpoch::default()));
        }
        match self.actions.last_mut() {
            Some(TxTableAction::Data(epoch)) => epoch,
            _ => unreachable!("last action must be a data epoch"),
        }
    }

    fn stage_truncate(&mut self, expected_metadata_location: String) {
        self.invalidate_combined_delta();
        self.actions.push(TxTableAction::Truncate(TxTruncateAction {
            expected_metadata_location,
        }));
    }

    fn stage_drop(&mut self) {
        self.invalidate_combined_delta();
        self.actions.push(TxTableAction::Drop);
    }

    fn is_dropped(&self) -> bool {
        matches!(self.actions.last(), Some(TxTableAction::Drop))
    }

    fn record_data_files(
        &mut self,
        new_data_files: Vec<DataFile>,
    ) -> IcebergResult<()> {
        if new_data_files.is_empty() {
            return Ok(());
        }
        let epoch = self.current_data_epoch_mut();
        let delta = Arc::make_mut(&mut epoch.delta);
        for data_file in new_data_files {
            delta.add_data_file(data_file)?;
        }
        Ok(())
    }

    fn record_position_delete_file(
        &mut self,
        delete_file: DataFile,
        referenced_data_files: Vec<String>,
    ) -> IcebergResult<()> {
        let epoch = self.current_data_epoch_mut();
        Arc::make_mut(&mut epoch.delta)
            .add_position_delete_file(delete_file, referenced_data_files)?;
        Ok(())
    }

    fn record_remove_delete_file(
        &mut self,
        identity: DeleteFileIdentity,
    ) -> IcebergResult<()> {
        let epoch = self.current_data_epoch_mut();
        Arc::make_mut(&mut epoch.delta).remove_delete_file(identity)?;
        Ok(())
    }

    fn record_remove_data_file(&mut self, file_path: String) -> IcebergResult<()> {
        let epoch = self.current_data_epoch_mut();
        Arc::make_mut(&mut epoch.delta).remove_data_file(file_path)?;
        Ok(())
    }

    fn record_validation(&mut self, validation: RowDeltaValidation) {
        self.current_data_epoch_mut().validations.push(validation);
    }

    fn overlay_metadata(
        &self,
        mut metadata: TableMetadata,
    ) -> IcebergResult<TableMetadata> {
        for action in &self.actions {
            match action {
                TxTableAction::Schema(update) => {
                    update
                        .validate_base_metadata(&metadata)
                        .map_err(IcebergError::schema_evolution_conflict)?;
                    metadata = update
                        .apply_to_metadata(&metadata)
                        .map_err(IcebergError::from)?;
                }
                TxTableAction::Properties(update) => {
                    metadata = update.apply_to_metadata(&metadata)?;
                }
                TxTableAction::Data(_)
                | TxTableAction::Truncate(_)
                | TxTableAction::Vacuum(_)
                | TxTableAction::Drop => {}
            }
        }
        Ok(metadata)
    }

    fn combined_delta(&self) -> IcebergResult<Option<Arc<SnapshotDelta>>> {
        if let Some(delta) = self.combined_delta_cache.get() {
            return Ok(delta.clone());
        }

        let mut combined = SnapshotDelta::new();
        let mut has_delta = false;
        for action in &self.actions {
            match action {
                TxTableAction::Data(epoch) if !epoch.delta.is_empty() => {
                    combined.append_delta(&epoch.delta)?;
                    has_delta = true;
                }
                TxTableAction::Truncate(_) => {
                    combined.truncate_table()?;
                    has_delta = true;
                }
                TxTableAction::Drop => return Ok(None),
                TxTableAction::Vacuum(_) => return Ok(None),
                TxTableAction::Schema(_)
                | TxTableAction::Properties(_)
                | TxTableAction::Data(_) => {}
            }
        }
        let delta = has_delta.then(|| Arc::new(combined));
        self.combined_delta_cache
            .set(delta.clone())
            .map_err(|_| {
                IcebergError::InvariantViolated(
                    "combined delta cache was initialized more than once without invalidation",
                )
            })?;
        Ok(delta)
    }
}

impl TableState {
    fn new() -> Self {
        Self {
            file_io: None,
            actions: Rc::new(TxTableActionLog::default()),
            level_history: Vec::new(),
            row_registry: RelationRowRegistry::default(),
        }
    }

    fn record_history(&mut self, nest_level: i32) -> (TxTableActionLogMarker, bool) {
        let marker = self.actions.mark();
        let should_record_history = nest_level > 1;
        if should_record_history {
            self.level_history.push(HistoryFrame { nest_level, marker });
        }
        (marker, should_record_history)
    }

    fn record_action_mutation<F>(
        &mut self,
        nest_level: i32,
        mutation: F,
    ) -> IcebergResult<()>
    where
        F: FnOnce(&mut TxTableActionLog) -> IcebergResult<()>,
    {
        if self.actions.has_vacuum() {
            return Err(IcebergError::Vacuum {
                source: crate::error::IcebergVacuumError::ActionConflict,
            });
        }
        let (marker, should_record_history) = self.record_history(nest_level);
        if let Err(err) = mutation(Rc::make_mut(&mut self.actions)) {
            Rc::make_mut(&mut self.actions).truncate(marker);
            if should_record_history {
                self.level_history.pop();
            }
            return Err(err);
        }

        Ok(())
    }

    fn record_validation(
        &mut self,
        nest_level: i32,
        validation: RowDeltaValidation,
    ) -> IcebergResult<()> {
        if self.actions.has_vacuum() {
            return Err(IcebergError::Vacuum {
                source: crate::error::IcebergVacuumError::ActionConflict,
            });
        }
        self.record_history(nest_level);
        Rc::make_mut(&mut self.actions).record_validation(validation);
        Ok(())
    }

    fn record_schema_update(
        &mut self,
        nest_level: i32,
        update: PreparedSchemaUpdate,
    ) -> IcebergResult<()> {
        if update.is_empty() {
            return Ok(());
        }

        if self.actions.has_vacuum() {
            return Err(IcebergError::Vacuum {
                source: crate::error::IcebergVacuumError::ActionConflict,
            });
        }

        self.record_history(nest_level);
        Rc::make_mut(&mut self.actions).stage_schema(update);
        Ok(())
    }

    fn record_table_property_update(
        &mut self,
        nest_level: i32,
        update: PreparedTablePropertyUpdate,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.stage_properties(update);
            Ok(())
        })
    }

    fn record_data_files(
        &mut self,
        nest_level: i32,
        new_data_files: Vec<DataFile>,
    ) -> IcebergResult<()> {
        if new_data_files.is_empty() {
            return Ok(());
        }

        self.record_action_mutation(nest_level, |actions| {
            actions.record_data_files(new_data_files)
        })
    }

    fn record_truncate(
        &mut self,
        nest_level: i32,
        expected_metadata_location: String,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.stage_truncate(expected_metadata_location);
            Ok(())
        })
    }

    fn record_drop(&mut self, nest_level: i32) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.stage_drop();
            Ok(())
        })
    }

    fn record_position_delete_file(
        &mut self,
        nest_level: i32,
        delete_file: DataFile,
        referenced_data_files: Vec<String>,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.record_position_delete_file(delete_file, referenced_data_files)
        })
    }

    fn record_remove_delete_file(
        &mut self,
        nest_level: i32,
        identity: DeleteFileIdentity,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.record_remove_delete_file(identity)
        })
    }

    fn record_remove_data_file(
        &mut self,
        nest_level: i32,
        file_path: String,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.record_remove_data_file(file_path)
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
            Rc::make_mut(&mut self.actions).truncate(frame.marker);
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
    pub maintenance_due_at: Option<pg_sys::TimestampTz>,
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
        if delta_stats.truncates_base {
            rows = 0;
            bytes = 0;
        }
        rows = rows.saturating_add(delta_stats.added_data_records);
        bytes = bytes
            .saturating_add(delta_stats.added_data_file_bytes)
            .saturating_add(delta_stats.added_delete_file_bytes);

        if !delta_stats.truncates_base {
            self.subtract_removed_data_file_stats(
                file_io,
                &delta_stats.removed_data_paths,
                &mut rows,
                &mut bytes,
            )?;
        }

        Ok((rows, bytes))
    }

    /// Whether the captured snapshot may contain row-level delete files.
    ///
    /// A false result is exact and allows the manifest `total-records` value to
    /// serve as the live-row estimate. A true result is deliberately
    /// conservative when transaction-local removal of the last committed
    /// delete file cannot be proven from summary counters alone.
    pub(crate) fn may_have_row_deletes(&self) -> bool {
        let delta_stats = self.delta.as_ref().map(|delta| delta.stats());
        let base_was_replaced = delta_stats
            .as_ref()
            .is_some_and(|stats| stats.truncates_base);
        let base_may_have_deletes =
            if base_was_replaced || self.metadata.current_snapshot().is_none() {
                false
            } else {
                // Snapshot summary properties are extensible metadata. Treat a
                // missing or malformed delete count conservatively: returning
                // false here authorizes the planner to treat physical records as
                // an exact live-row count.
                Self::summary_u64(&self.metadata, TOTAL_DELETE_FILES)
                    .is_none_or(|count| count != 0)
            };
        base_may_have_deletes
            || delta_stats.is_some_and(|stats| stats.added_delete_file_bytes != 0)
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
            if delta.truncates_base() {
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
        self.stage_table_mutation(relid, file_io, |state, nest_level| {
            state.record_remove_data_file(nest_level, file_path)
        })
    }

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
            state.record_action_mutation(nest_level, |actions| {
                actions.stage_vacuum(vacuum)
            })
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
        state.record_drop(nest_level)?;
        Ok(())
    }

    fn stage_table_mutation<F>(
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
                    Some(Rc::clone(&state.actions))
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
            .map(|state| state.row_registry.clone())
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
        if state.actions.is_empty() {
            return Ok(None);
        }
        if state.actions.is_dropped() {
            return Ok(None);
        }
        let file_io = state.file_io.clone().ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "table {} has staged Iceberg metadata changes without FileIO",
                relid
            ))
        })?;
        Ok(Some(TableCommitInput {
            actions: Rc::clone(&state.actions),
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
            crate::maintenance::AutomaticMaintenanceNotifier::stage_wakeup()?;
        }
        Ok(())
    }

    fn on_pre_prepare(&self) -> TransactionResult<()> {
        let has_staged_actions = self
            .inner
            .borrow()
            .tables
            .values()
            .any(|state| !state.actions.is_empty());
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

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg_lite::spec::{DataFileBuilder, DataFileFormat, Struct};

    #[test]
    fn combined_delta_keeps_only_data_after_last_truncate() {
        let mut actions = TxTableActionLog::default();
        actions
            .record_data_files(vec![data_file("before.parquet")])
            .unwrap();
        actions.stage_truncate("metadata-v1.json".to_owned());
        actions
            .record_data_files(vec![data_file("after.parquet")])
            .unwrap();

        let delta = actions.combined_delta().unwrap().unwrap();
        assert!(delta.truncates_base());
        assert_eq!(
            delta
                .added_data_files()
                .iter()
                .map(DataFile::file_path)
                .collect::<Vec<_>>(),
            vec!["after.parquet"]
        );
        assert_eq!(
            actions.commit_plan().unwrap().canceled_created_paths,
            vec!["before.parquet".to_owned()]
        );
        let plan = actions.commit_plan().unwrap();
        assert!(matches!(
            plan.actions.as_slice(),
            [EffectiveCommitAction::Data {
                truncate_base: true,
                ..
            }]
        ));
    }

    #[test]
    fn commit_plan_preserves_data_only_epochs_without_truncate() {
        let mut actions = TxTableActionLog::default();
        actions
            .record_data_files(vec![data_file("data.parquet")])
            .unwrap();

        let plan = actions.commit_plan().unwrap();
        assert!(plan.expected_metadata_location.is_none());
        assert!(plan.canceled_created_paths.is_empty());
        assert!(matches!(
            plan.actions.as_slice(),
            [EffectiveCommitAction::Data {
                truncate_base: false,
                ..
            }]
        ));
    }

    #[test]
    fn commit_plan_replaces_pre_truncate_data_with_truncate_only() {
        let mut actions = TxTableActionLog::default();
        actions
            .record_data_files(vec![data_file("discarded.parquet")])
            .unwrap();
        actions.stage_truncate("metadata-v1.json".to_owned());

        let plan = actions.commit_plan().unwrap();
        assert_eq!(plan.expected_metadata_location, Some("metadata-v1.json"));
        assert_eq!(
            plan.canceled_created_paths,
            vec!["discarded.parquet".to_owned()]
        );
        assert!(matches!(
            plan.actions.as_slice(),
            [EffectiveCommitAction::TruncateOnly]
        ));
    }

    #[test]
    fn combined_delta_is_shared_until_the_action_log_changes() {
        let mut actions = TxTableActionLog::default();
        actions
            .record_data_files(vec![data_file("first.parquet")])
            .unwrap();

        let first = actions.combined_delta().unwrap().unwrap();
        let cached = actions.combined_delta().unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &cached));

        actions
            .record_data_files(vec![data_file("second.parquet")])
            .unwrap();
        let rebuilt = actions.combined_delta().unwrap().unwrap();
        assert!(!Arc::ptr_eq(&first, &rebuilt));
        assert!(rebuilt.has_live_added_data_file_path("second.parquet"));
    }

    #[test]
    fn populated_combined_delta_cache_is_invalidated_by_savepoint_rollback() {
        let mut actions = TxTableActionLog::default();
        actions
            .record_data_files(vec![data_file("before.parquet")])
            .unwrap();
        let marker = actions.mark();
        let before = actions.combined_delta().unwrap().unwrap();

        actions.stage_truncate("metadata-v1.json".to_owned());
        let truncated = actions.combined_delta().unwrap().unwrap();
        assert!(truncated.truncates_base());
        assert!(!Arc::ptr_eq(&before, &truncated));

        actions.truncate(marker);
        let restored = actions.combined_delta().unwrap().unwrap();
        assert!(!restored.truncates_base());
        assert!(restored.has_live_added_data_file_path("before.parquet"));
        assert!(!Arc::ptr_eq(&truncated, &restored));
    }

    #[test]
    fn shared_action_log_snapshot_is_copy_on_write() {
        let mut current = Rc::new(TxTableActionLog::default());
        Rc::make_mut(&mut current)
            .record_data_files(vec![data_file("before.parquet")])
            .unwrap();
        let commit_snapshot = Rc::clone(&current);

        Rc::make_mut(&mut current)
            .record_data_files(vec![data_file("after.parquet")])
            .unwrap();

        let snapshotted_delta = commit_snapshot.combined_delta().unwrap().unwrap();
        assert!(snapshotted_delta.has_live_added_data_file_path("before.parquet"));
        assert!(!snapshotted_delta.has_live_added_data_file_path("after.parquet"));
        let current_delta = current.combined_delta().unwrap().unwrap();
        assert!(current_delta.has_live_added_data_file_path("after.parquet"));
    }

    #[test]
    fn last_truncate_controls_baseline_and_canceled_regions() {
        let mut actions = TxTableActionLog::default();
        actions.stage_truncate("metadata-v1.json".to_owned());
        actions
            .record_data_files(vec![data_file("middle.parquet")])
            .unwrap();
        actions.stage_truncate("metadata-v2.json".to_owned());
        actions
            .record_data_files(vec![data_file("after.parquet")])
            .unwrap();

        let (index, truncate) = actions.last_truncate().unwrap();
        assert_eq!(index, 2);
        assert_eq!(truncate.expected_metadata_location, "metadata-v2.json");
        assert_eq!(
            actions.commit_plan().unwrap().canceled_created_paths,
            vec!["middle.parquet".to_owned()]
        );
    }

    #[test]
    fn action_log_marker_rolls_back_truncate_and_later_data() {
        let mut actions = TxTableActionLog::default();
        actions
            .record_data_files(vec![data_file("before.parquet")])
            .unwrap();
        let marker = actions.mark();
        actions.stage_truncate("metadata-v1.json".to_owned());
        actions
            .record_data_files(vec![data_file("after.parquet")])
            .unwrap();

        actions.truncate(marker);

        assert!(actions.last_truncate().is_none());
        let delta = actions.combined_delta().unwrap().unwrap();
        assert!(!delta.truncates_base());
        assert!(delta.has_live_added_data_file_path("before.parquet"));
        assert!(!delta.has_live_added_data_file_path("after.parquet"));
    }

    #[test]
    fn canceled_paths_include_local_removes_after_truncate() {
        let mut actions = TxTableActionLog::default();
        actions.stage_truncate("metadata-v1.json".to_owned());
        actions
            .record_data_files(vec![data_file("after.parquet")])
            .unwrap();
        actions
            .record_remove_data_file("after.parquet".to_owned())
            .unwrap();

        assert_eq!(
            actions.commit_plan().unwrap().canceled_created_paths,
            vec!["after.parquet".to_owned()]
        );
    }

    #[test]
    fn drop_suppresses_commit_but_marker_can_restore_actions() {
        let mut actions = TxTableActionLog::default();
        actions.stage_truncate("metadata-v1.json".to_owned());
        let marker = actions.mark();
        actions.stage_drop();

        assert!(actions.is_dropped());
        assert!(actions.combined_delta().unwrap().is_none());

        actions.truncate(marker);
        assert!(!actions.is_dropped());
        assert!(actions.combined_delta().unwrap().unwrap().truncates_base());
    }

    fn data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }
}
