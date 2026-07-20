//! Per-table transactional state and savepoint history.

use std::rc::Rc;

use iceberg_lite::io::FileIO;
use iceberg_lite::overlay::DeleteFileIdentity;
use iceberg_lite::spec::DataFile;
use iceberg_lite::transaction::{PreparedSchemaUpdate, RowDeltaValidation};

use crate::catalog::row_mutations::RelationRowRegistry;
use crate::catalog::table_properties::PreparedTablePropertyUpdate;
use crate::error::{IcebergError, IcebergResult};

use super::action_log::{TxTableActionLog, TxTableActionLogMarker};

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
pub(super) struct TableState {
    /// FileIO captured from the first statement to drive the final commit.
    pub(super) file_io: Option<FileIO>,

    /// Ordered transaction-local schema/data actions.
    pub(super) actions: Rc<TxTableActionLog>,

    /// Savepoint history stack. Each frame is the state BEFORE a write at
    /// `nest_level`, so sub-abort can restore it by popping frames whose
    /// `nest_level >= aborted_level`.
    ///
    /// Top-level writes do not need frames: top-level abort drops the whole
    /// tracker, and top-level commit never rolls back through this stack.
    level_history: Vec<HistoryFrame>,

    /// Physical-row claims used to reproduce PostgreSQL `TM_SelfModified`
    /// semantics across sibling ModifyTable nodes and nested SPI executions.
    pub(super) row_registry: RelationRowRegistry,
}

/// Owned per-table state detached from the tracker before commit I/O begins.
///
/// The action log is an immutable shared snapshot. Keeping it owned releases
/// the [`std::cell::RefCell`] borrow on [`super::TxMetadataInner`] before
/// metadata reads, transaction materialization, and catalog CAS retries.
pub(super) struct TableCommitInput {
    pub(super) actions: Rc<TxTableActionLog>,
    pub(super) file_io: FileIO,
}

impl TableState {
    pub(super) fn new() -> Self {
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

    pub(super) fn record_action_mutation<F>(
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

    pub(super) fn record_validation(
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

    pub(super) fn record_schema_update(
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

    pub(super) fn record_table_property_update(
        &mut self,
        nest_level: i32,
        update: PreparedTablePropertyUpdate,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.stage_properties(update);
            Ok(())
        })
    }

    pub(super) fn record_data_files(
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

    pub(super) fn record_truncate(
        &mut self,
        nest_level: i32,
        expected_metadata_location: String,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.stage_truncate(expected_metadata_location);
            Ok(())
        })
    }

    pub(super) fn record_drop(&mut self, nest_level: i32) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.stage_drop();
            Ok(())
        })
    }

    pub(super) fn record_position_delete_file(
        &mut self,
        nest_level: i32,
        delete_file: DataFile,
        referenced_data_files: Vec<String>,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.record_position_delete_file(delete_file, referenced_data_files)
        })
    }

    pub(super) fn record_remove_delete_file(
        &mut self,
        nest_level: i32,
        identity: DeleteFileIdentity,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.record_remove_delete_file(identity)
        })
    }

    pub(super) fn record_remove_data_file(
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
    pub(super) fn rollback_to_level(&mut self, target_level: i32) {
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
    pub(super) fn promote_to_level(&mut self, from_level: i32) {
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
