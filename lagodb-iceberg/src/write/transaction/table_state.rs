//! Per-table recoverable action state and savepoint history.

use std::rc::Rc;

use iceberg_lite::transaction::PreparedSchemaUpdate;
use pgrx::pg_sys;

use crate::error::IcebergResult;

use super::action_log::{
    ExclusiveTransactionAction, TxTableActionLog, TxTableActionLogMarker,
};
use super::{PreparedTablePropertyUpdate, TxWriteBatch};

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

/// Catalog-independent action state for one Iceberg table.
///
/// Catalog adapters retain this state when a storage generation must be
/// restored on rollback. Transaction-scoped row identities belong to the
/// enclosing table state and survive generation changes.
#[derive(Debug)]
pub(crate) struct TableTransactionState<E, G> {
    /// Ordered transaction-local schema/data actions.
    pub(crate) actions: Rc<TxTableActionLog<E, G>>,

    /// Savepoint history stack. Each frame is the state BEFORE a write at
    /// `nest_level`, so sub-abort can restore it by popping frames whose
    /// `nest_level >= aborted_level`.
    ///
    /// Top-level writes do not need frames: top-level abort drops the whole
    /// tracker, and top-level commit never rolls back through this stack.
    level_history: Vec<HistoryFrame>,
}

impl<E, G> TableTransactionState<E, G>
where
    E: ExclusiveTransactionAction,
    G: Clone + std::fmt::Debug,
{
    pub(crate) fn new() -> Self {
        Self {
            actions: Rc::new(TxTableActionLog::default()),
            level_history: Vec::new(),
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

    pub(crate) fn record_action_mutation<F>(
        &mut self,
        nest_level: i32,
        mutation: F,
    ) -> IcebergResult<()>
    where
        F: FnOnce(&mut TxTableActionLog<E, G>) -> IcebergResult<()>,
    {
        if let Some(error) = self.actions.exclusive_conflict_error() {
            return Err(error);
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

    pub(crate) fn record_batch(
        &mut self,
        nest_level: i32,
        batch: TxWriteBatch,
    ) -> IcebergResult<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.record_action_mutation(nest_level, |actions| {
            actions.record_batch(batch);
            Ok(())
        })
    }

    pub(crate) fn record_schema_update(
        &mut self,
        nest_level: i32,
        update: PreparedSchemaUpdate,
    ) -> IcebergResult<()> {
        if update.is_empty() {
            return Ok(());
        }

        if let Some(error) = self.actions.exclusive_conflict_error() {
            return Err(error);
        }

        self.record_history(nest_level);
        Rc::make_mut(&mut self.actions).stage_schema(update);
        Ok(())
    }

    pub(crate) fn record_table_property_update(
        &mut self,
        nest_level: i32,
        update: PreparedTablePropertyUpdate,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.stage_properties(update);
            Ok(())
        })
    }

    pub(crate) fn record_exclusive_action(
        &mut self,
        nest_level: i32,
        action: E,
    ) -> IcebergResult<()> {
        let (marker, should_record_history) = self.record_history(nest_level);
        if let Err(error) = Rc::make_mut(&mut self.actions).stage_exclusive(action) {
            Rc::make_mut(&mut self.actions).truncate(marker);
            if should_record_history {
                self.level_history.pop();
            }
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn record_truncate(
        &mut self,
        nest_level: i32,
        guard: G,
    ) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            // SAFETY: TRUNCATE is staged synchronously by its utility command.
            let command_id = unsafe { pg_sys::GetCurrentCommandId(true) };
            actions.stage_truncate(guard, command_id);
            Ok(())
        })
    }

    pub(crate) fn record_drop(&mut self, nest_level: i32) -> IcebergResult<()> {
        self.record_action_mutation(nest_level, |actions| {
            actions.stage_drop();
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
    pub(crate) fn rollback_to_level(&mut self, target_level: i32) {
        while let Some(frame) = self.level_history.last() {
            if frame.nest_level < target_level {
                break;
            }
            let frame = self.level_history.pop().unwrap();
            Rc::make_mut(&mut self.actions).truncate(frame.marker);
        }
    }

    /// Promote every nest level `>= from_level` down to `from_level - 1`.
    ///
    /// Mirrors storage transaction resource promotion: when
    /// `RELEASE SAVEPOINT` fires for `from_level`, every state owned by
    /// that subtransaction (and any lingering deeper frames) belongs to
    /// the parent now, so its recorded nest level must drop accordingly.
    ///
    /// Without this, a sibling `SAVEPOINT` opened at `from_level` after the
    /// release would alias the released savepoint's level, and a
    /// `ROLLBACK TO` of the sibling would incorrectly throw away changes
    /// that were already promoted to the parent.
    pub(crate) fn promote_to_level(&mut self, from_level: i32) {
        for frame in &mut self.level_history {
            if frame.nest_level >= from_level {
                frame.nest_level = from_level - 1;
            }
        }
        // Frames promoted to top-level are no longer useful: top-level abort
        // drops the whole tracker, and sibling savepoints must not roll them
        // back.
        self.level_history.retain(|frame| frame.nest_level > 1);
    }
}
