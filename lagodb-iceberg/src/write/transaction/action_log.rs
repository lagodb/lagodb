//! Ordered transaction-local Iceberg table actions and commit planning.

use std::cell::{OnceCell, RefCell};
use std::collections::HashSet;
use std::sync::Arc;

use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::spec::{SchemaRef, TableMetadata};
use iceberg_lite::transaction::{
    ApplyTransactionAction, PreparedSchemaUpdate, Transaction,
};

use crate::error::{IcebergError, IcebergResult};

use super::PreparedTablePropertyUpdate;
use super::data_epoch::{TxDataEpoch, TxWriteBatch};
use pgrx::pg_sys;

/// Provider-owned operation that must be exclusive with ordinary Iceberg
/// mutations in one PostgreSQL transaction.
///
/// The shared tracker controls ordering and savepoint lifetime, while the
/// provider retains the operation's policy and exact error contract.
pub(crate) trait ExclusiveTransactionAction: Clone + std::fmt::Debug {
    const NOT_EXCLUSIVE_INVARIANT: &'static str;
    const MISPLACED_INVARIANT: &'static str;

    fn conflict_error(&self) -> IcebergError;
}

#[derive(Debug, Clone)]
pub(crate) struct TxTableActionLog<E, G> {
    actions: Vec<TxTableAction<E, G>>,
    /// Cached transaction-local file overlay. It depends only on Data,
    /// Truncate, and Drop actions; schema actions are replayed separately onto
    /// the latest committed metadata and do not invalidate this value.
    combined_delta_cache: OnceCell<Option<Arc<SnapshotDelta>>>,
    visible_delta_cache: RefCell<Option<VisibleDelta>>,
    /// Maximum CID across Data batches and Truncate actions; None means no CIDs.
    max_write_command_id_cache: OnceCell<Option<pg_sys::CommandId>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TxTableActionLogMarker {
    action_len: usize,
    current_data_epoch: Option<usize>,
}

#[derive(Debug, Clone)]
enum TxTableAction<E, G> {
    Schema(PreparedSchemaUpdate),
    Properties(PreparedTablePropertyUpdate),
    Data(TxDataEpoch),
    Truncate(TxTruncateAction<G>),
    Exclusive(Box<E>),
    Drop,
}

#[derive(Debug, Clone)]
pub(crate) struct TxTruncateAction<G> {
    pub(crate) guard: G,
    command_id: pg_sys::CommandId,
}

#[derive(Debug, Clone)]
struct VisibleDelta {
    read_command_id: pg_sys::CommandId,
    delta: Option<Arc<SnapshotDelta>>,
}

pub(crate) struct TxTableCommitPlan<'a, E, G> {
    pub(crate) actions: Vec<EffectiveCommitAction<'a>>,
    pub(crate) exclusive_action: Option<&'a E>,
    pub(crate) truncate_guard: Option<&'a G>,
    pub(crate) canceled_created_paths: Vec<String>,
}

pub(crate) enum EffectiveCommitAction<'a> {
    Schema(&'a PreparedSchemaUpdate),
    Properties(&'a PreparedTablePropertyUpdate),
    Data {
        epoch: &'a TxDataEpoch,
        truncate_base: bool,
    },
    TruncateOnly,
}

impl<E, G> TxTableCommitPlan<'_, E, G> {
    pub(crate) fn has_data_change(&self) -> bool {
        self.actions.iter().any(|action| {
            matches!(
                action,
                EffectiveCommitAction::Data { .. }
                    | EffectiveCommitAction::TruncateOnly
            )
        })
    }

    /// Apply provider-independent Iceberg actions to a transaction rebased on
    /// `base_metadata`. Catalog publication is deliberately left to the AM or
    /// FDW adapter.
    pub(crate) fn apply_to_transaction(
        &self,
        mut transaction: Transaction,
        base_metadata: &TableMetadata,
    ) -> IcebergResult<Transaction> {
        let mut schema_metadata = base_metadata.clone();
        for action in &self.actions {
            match action {
                EffectiveCommitAction::Schema(schema_update) => {
                    schema_update
                        .validate_base_metadata(&schema_metadata)
                        .map_err(IcebergError::schema_evolution_conflict)?;
                    schema_metadata = schema_update
                        .apply_to_metadata(&schema_metadata)
                        .map_err(IcebergError::from)?;
                    transaction = (**schema_update).clone().apply(transaction)?;
                }
                EffectiveCommitAction::Properties(update) => {
                    update.validate_base_metadata(&schema_metadata)?;
                    schema_metadata = update.apply_to_metadata(&schema_metadata)?;
                    transaction = update.apply_to_transaction(transaction)?;
                }
                EffectiveCommitAction::Data {
                    epoch,
                    truncate_base,
                } => {
                    let delta = epoch.delta()?;
                    let validations = epoch.validations();
                    transaction = if validations.is_empty() {
                        let mut action =
                            transaction.snapshot_delta(Arc::clone(delta));
                        if *truncate_base {
                            action = action.truncate_base();
                        }
                        action.apply(transaction)?
                    } else {
                        let mut action = transaction
                            .row_delta(Arc::clone(delta))
                            .add_validations(validations);
                        if *truncate_base {
                            action = action.truncate_base();
                        }
                        action.apply(transaction)?
                    };
                }
                EffectiveCommitAction::TruncateOnly => {
                    transaction = transaction
                        .snapshot_delta(Arc::new(SnapshotDelta::new()))
                        .truncate_base()
                        .apply(transaction)?;
                }
            }
        }
        Ok(transaction)
    }
}

impl<E, G> Default for TxTableActionLog<E, G> {
    fn default() -> Self {
        Self {
            actions: Vec::new(),
            combined_delta_cache: OnceCell::new(),
            visible_delta_cache: RefCell::new(None),
            max_write_command_id_cache: OnceCell::new(),
        }
    }
}

impl<E, G> TxTableActionLog<E, G>
where
    E: ExclusiveTransactionAction,
    G: Clone + std::fmt::Debug,
{
    pub(crate) fn exclusive_conflict_error(&self) -> Option<IcebergError> {
        self.actions.iter().find_map(|action| match action {
            TxTableAction::Exclusive(exclusive) => Some(exclusive.conflict_error()),
            _ => None,
        })
    }

    fn invalidate_read_caches(&mut self) {
        self.combined_delta_cache.take();
        self.visible_delta_cache.get_mut().take();
        self.max_write_command_id_cache.take();
    }

    pub(crate) fn last_truncate(&self) -> Option<(usize, &TxTruncateAction<G>)> {
        self.actions
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, action)| match action {
                TxTableAction::Truncate(truncate) => Some((index, truncate)),
                TxTableAction::Schema(_)
                | TxTableAction::Properties(_)
                | TxTableAction::Data(_)
                | TxTableAction::Exclusive(_)
                | TxTableAction::Drop => None,
            })
    }

    pub(crate) fn commit_plan(&self) -> IcebergResult<TxTableCommitPlan<'_, E, G>> {
        if let Some(TxTableAction::Exclusive(exclusive)) = self.actions.first() {
            if self.actions.len() != 1 {
                return Err(IcebergError::InvariantViolated(
                    E::NOT_EXCLUSIVE_INVARIANT,
                ));
            }
            return Ok(TxTableCommitPlan {
                actions: Vec::new(),
                exclusive_action: Some(exclusive.as_ref()),
                truncate_guard: None,
                canceled_created_paths: Vec::new(),
            });
        }
        let last_truncate = self.last_truncate();
        let last_truncate_index = last_truncate.map(|(index, _)| index);
        let truncate_guard = last_truncate.map(|(_, truncate)| &truncate.guard);
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
                            epoch.delta()?.created_file_paths()
                        } else {
                            epoch.delta()?.canceled_created_file_paths()
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
                TxTableAction::Exclusive(_) => {
                    return Err(IcebergError::InvariantViolated(
                        E::MISPLACED_INVARIANT,
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
            exclusive_action: None,
            truncate_guard,
            canceled_created_paths,
        })
    }

    pub(crate) fn mark(&self) -> TxTableActionLogMarker {
        let current_data_epoch = match self.actions.last() {
            Some(TxTableAction::Data(epoch)) => Some(epoch.mark()),
            _ => None,
        };

        TxTableActionLogMarker {
            action_len: self.actions.len(),
            current_data_epoch,
        }
    }

    pub(crate) fn truncate(&mut self, marker: TxTableActionLogMarker) {
        self.invalidate_read_caches();
        self.actions.truncate(marker.action_len);
        if let Some(epoch_marker) = marker.current_data_epoch
            && let Some(TxTableAction::Data(epoch)) = self.actions.last_mut()
        {
            epoch.truncate(epoch_marker);
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.actions.iter().all(|action| match action {
            TxTableAction::Schema(update) => update.is_empty(),
            TxTableAction::Properties(_) => false,
            TxTableAction::Data(epoch) => epoch.is_empty(),
            TxTableAction::Truncate(_) => false,
            TxTableAction::Exclusive(_) => false,
            TxTableAction::Drop => false,
        })
    }

    pub(crate) fn stage_schema(&mut self, update: PreparedSchemaUpdate) {
        if !update.is_empty() {
            self.actions.push(TxTableAction::Schema(update));
        }
    }

    pub(crate) fn stage_properties(&mut self, update: PreparedTablePropertyUpdate) {
        self.actions.push(TxTableAction::Properties(update));
    }

    pub(crate) fn stage_exclusive(&mut self, action: E) -> IcebergResult<()> {
        if !self.actions.is_empty() {
            return Err(action.conflict_error());
        }
        self.invalidate_read_caches();
        self.actions
            .push(TxTableAction::Exclusive(Box::new(action)));
        Ok(())
    }

    fn current_data_epoch_mut(&mut self) -> &mut TxDataEpoch {
        self.invalidate_read_caches();
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

    pub(crate) fn stage_truncate(&mut self, guard: G, command_id: pg_sys::CommandId) {
        self.invalidate_read_caches();
        self.actions.push(TxTableAction::Truncate(TxTruncateAction {
            guard,
            command_id,
        }));
    }

    pub(crate) fn stage_drop(&mut self) {
        self.invalidate_read_caches();
        self.actions.push(TxTableAction::Drop);
    }

    pub(crate) fn is_dropped(&self) -> bool {
        matches!(self.actions.last(), Some(TxTableAction::Drop))
    }

    pub(crate) fn record_batch(&mut self, batch: TxWriteBatch) {
        if !batch.is_empty() {
            self.current_data_epoch_mut().push(batch);
        }
    }

    pub(crate) fn overlay_metadata(
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
                | TxTableAction::Exclusive(_)
                | TxTableAction::Drop => {}
            }
        }
        Ok(metadata)
    }

    /// Data-only writes need no metadata copy to obtain the current schema.
    /// Replay definition actions only when a local schema update exists.
    pub(crate) fn read_schema(
        &self,
        metadata: &TableMetadata,
    ) -> IcebergResult<SchemaRef> {
        if !self
            .actions
            .iter()
            .any(|action| matches!(action, TxTableAction::Schema(_)))
        {
            return Ok(Arc::clone(metadata.current_schema()));
        }
        let definition = self.overlay_metadata(metadata.clone())?;
        Ok(Arc::clone(definition.current_schema()))
    }

    pub(crate) fn combined_delta(&self) -> IcebergResult<Option<Arc<SnapshotDelta>>> {
        self.visible_delta(None)
    }

    /// Build only the file overlay, independently of current schema/property
    /// replay. The bounded cache is invalidated by every file-log mutation.
    pub(crate) fn visible_delta(
        &self,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> IcebergResult<Option<Arc<SnapshotDelta>>> {
        if let Some(cid) = read_command_id {
            // Later commands normally see all writes. Reuse the full overlay
            // across different reader CIDs until the log changes.
            // Completion order need not follow CID order. Cache the maximum
            // so repeated reads do not revisit every batch before a cache hit.
            let max_write_command_id =
                self.max_write_command_id_cache.get_or_init(|| {
                    self.actions
                        .iter()
                        .filter_map(|action| match action {
                            TxTableAction::Data(epoch) => {
                                epoch.max_write_command_id()
                            }
                            TxTableAction::Truncate(truncate) => {
                                Some(truncate.command_id)
                            }
                            TxTableAction::Schema(_)
                            | TxTableAction::Properties(_)
                            | TxTableAction::Exclusive(_)
                            | TxTableAction::Drop => None,
                        })
                        .max()
                });
            if max_write_command_id.is_none_or(|max| max < cid) {
                return self.combined_delta();
            }
            if let Some(cached) = self.visible_delta_cache.borrow().as_ref()
                && cached.read_command_id == cid
            {
                return Ok(cached.delta.clone());
            }
        } else if let Some(delta) = self.combined_delta_cache.get() {
            return Ok(delta.clone());
        }

        // Share the first complete epoch. Only a second file component or a
        // TRUNCATE needs its own aggregate; the epoch cache stays immutable for
        // commit planning and already-bound readers.
        let mut combined: Option<Arc<SnapshotDelta>> = None;
        for action in &self.actions {
            match action {
                TxTableAction::Data(epoch) => {
                    if read_command_id.is_none() {
                        let delta = epoch.delta()?;
                        if !delta.is_empty() {
                            match &mut combined {
                                Some(combined) => {
                                    Arc::make_mut(combined).append_delta(delta)?;
                                }
                                None => combined = Some(Arc::clone(delta)),
                            }
                        }
                    } else {
                        let combined = combined
                            .get_or_insert_with(|| Arc::new(SnapshotDelta::new()));
                        epoch.append_visible(
                            Arc::make_mut(combined),
                            read_command_id,
                        )?;
                    }
                }
                TxTableAction::Truncate(truncate) => {
                    if read_command_id.is_none_or(|cid| truncate.command_id < cid) {
                        let combined = combined
                            .get_or_insert_with(|| Arc::new(SnapshotDelta::new()));
                        Arc::make_mut(combined).truncate_table()?;
                    }
                }
                TxTableAction::Drop | TxTableAction::Exclusive(_) => return Ok(None),
                TxTableAction::Schema(_) | TxTableAction::Properties(_) => {}
            }
        }
        let delta = combined.filter(|delta| !delta.is_empty());
        if let Some(read_command_id) = read_command_id {
            *self.visible_delta_cache.borrow_mut() = Some(VisibleDelta {
                read_command_id,
                delta: delta.clone(),
            });
            Ok(delta)
        } else {
            Ok(self.combined_delta_cache.get_or_init(|| delta).clone())
        }
    }
}
