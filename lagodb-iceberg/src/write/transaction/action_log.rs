//! Ordered transaction-local Iceberg table actions and commit planning.

use std::cell::OnceCell;
use std::collections::HashSet;
use std::sync::Arc;

use iceberg_lite::overlay::{DeleteFileIdentity, SnapshotDelta, SnapshotDeltaMarker};
use iceberg_lite::spec::{DataFile, TableMetadata};
use iceberg_lite::transaction::{
    ApplyTransactionAction, PreparedSchemaUpdate, RowDeltaValidation, Transaction,
};

use crate::error::{IcebergError, IcebergResult};

use super::PreparedTablePropertyUpdate;

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
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TxTableActionLogMarker {
    action_len: usize,
    current_data_epoch: Option<TxDataEpochMarker>,
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
}

#[derive(Debug, Clone)]
pub(crate) struct TxDataEpoch {
    pub(crate) delta: Arc<SnapshotDelta>,
    pub(crate) validations: Vec<RowDeltaValidation>,
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
                    transaction = if epoch.validations.is_empty() {
                        let mut action =
                            transaction.snapshot_delta(Arc::clone(&epoch.delta));
                        if *truncate_base {
                            action = action.truncate_base();
                        }
                        action.apply(transaction)?
                    } else {
                        let mut action = transaction
                            .row_delta(Arc::clone(&epoch.delta))
                            .add_validations(epoch.validations.clone());
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

impl<E, G> Default for TxTableActionLog<E, G> {
    fn default() -> Self {
        Self {
            actions: Vec::new(),
            combined_delta_cache: OnceCell::new(),
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

    fn invalidate_combined_delta(&mut self) {
        self.combined_delta_cache.take();
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

    pub(crate) fn truncate(&mut self, marker: TxTableActionLogMarker) {
        self.invalidate_combined_delta();
        self.actions.truncate(marker.action_len);
        if let Some(epoch_marker) = marker.current_data_epoch
            && let Some(TxTableAction::Data(epoch)) = self.actions.last_mut()
        {
            Arc::make_mut(&mut epoch.delta).truncate(epoch_marker.delta);
            epoch.validations.truncate(epoch_marker.validation_len);
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
        self.invalidate_combined_delta();
        self.actions
            .push(TxTableAction::Exclusive(Box::new(action)));
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

    pub(crate) fn stage_truncate(&mut self, guard: G) {
        self.invalidate_combined_delta();
        self.actions
            .push(TxTableAction::Truncate(TxTruncateAction { guard }));
    }

    pub(crate) fn stage_drop(&mut self) {
        self.invalidate_combined_delta();
        self.actions.push(TxTableAction::Drop);
    }

    pub(crate) fn is_dropped(&self) -> bool {
        matches!(self.actions.last(), Some(TxTableAction::Drop))
    }

    pub(crate) fn record_data_files(
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

    pub(crate) fn record_position_delete_file(
        &mut self,
        delete_file: DataFile,
        referenced_data_files: Vec<String>,
    ) -> IcebergResult<()> {
        let epoch = self.current_data_epoch_mut();
        Arc::make_mut(&mut epoch.delta)
            .add_position_delete_file(delete_file, referenced_data_files)?;
        Ok(())
    }

    pub(crate) fn record_remove_delete_file(
        &mut self,
        identity: DeleteFileIdentity,
    ) -> IcebergResult<()> {
        let epoch = self.current_data_epoch_mut();
        Arc::make_mut(&mut epoch.delta).remove_delete_file(identity)?;
        Ok(())
    }

    pub(crate) fn record_validation(&mut self, validation: RowDeltaValidation) {
        self.current_data_epoch_mut().validations.push(validation);
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

    pub(crate) fn combined_delta(&self) -> IcebergResult<Option<Arc<SnapshotDelta>>> {
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
                TxTableAction::Exclusive(_) => return Ok(None),
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
