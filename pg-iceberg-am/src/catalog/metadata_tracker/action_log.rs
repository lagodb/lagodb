//! Ordered transaction-local table actions and commit planning.

use std::cell::OnceCell;
use std::collections::HashSet;
use std::sync::Arc;

use iceberg_lite::overlay::{DeleteFileIdentity, SnapshotDelta, SnapshotDeltaMarker};
use iceberg_lite::spec::{DataFile, TableMetadata};
use iceberg_lite::transaction::{PreparedSchemaUpdate, RowDeltaValidation};

use crate::catalog::table_properties::PreparedTablePropertyUpdate;
use crate::error::{IcebergError, IcebergResult};
use crate::maintenance::PreparedVacuum;

#[derive(Debug, Clone, Default)]
pub(super) struct TxTableActionLog {
    actions: Vec<TxTableAction>,
    /// Cached transaction-local file overlay. It depends only on Data,
    /// Truncate, and Drop actions; schema actions are replayed separately onto
    /// the latest committed metadata and do not invalidate this value.
    combined_delta_cache: OnceCell<Option<Arc<SnapshotDelta>>>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TxTableActionLogMarker {
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
pub(super) struct TxTruncateAction {
    pub(super) expected_metadata_location: String,
}

#[derive(Debug, Clone)]
pub(super) struct TxDataEpoch {
    pub(super) delta: Arc<SnapshotDelta>,
    pub(super) validations: Vec<RowDeltaValidation>,
}

pub(super) struct TxTableCommitPlan<'a> {
    pub(super) actions: Vec<EffectiveCommitAction<'a>>,
    pub(super) vacuum: Option<&'a PreparedVacuum>,
    pub(super) expected_metadata_location: Option<&'a str>,
    pub(super) canceled_created_paths: Vec<String>,
}

pub(super) enum EffectiveCommitAction<'a> {
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
    pub(super) fn has_vacuum(&self) -> bool {
        self.actions
            .iter()
            .any(|action| matches!(action, TxTableAction::Vacuum(_)))
    }

    fn invalidate_combined_delta(&mut self) {
        self.combined_delta_cache.take();
    }

    pub(super) fn last_truncate(&self) -> Option<(usize, &TxTruncateAction)> {
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

    pub(super) fn commit_plan(&self) -> IcebergResult<TxTableCommitPlan<'_>> {
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

    pub(super) fn mark(&self) -> TxTableActionLogMarker {
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

    pub(super) fn truncate(&mut self, marker: TxTableActionLogMarker) {
        self.invalidate_combined_delta();
        self.actions.truncate(marker.action_len);
        if let Some(epoch_marker) = marker.current_data_epoch
            && let Some(TxTableAction::Data(epoch)) = self.actions.last_mut()
        {
            Arc::make_mut(&mut epoch.delta).truncate(epoch_marker.delta);
            epoch.validations.truncate(epoch_marker.validation_len);
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.actions.iter().all(|action| match action {
            TxTableAction::Schema(update) => update.is_empty(),
            TxTableAction::Properties(_) => false,
            TxTableAction::Data(epoch) => epoch.is_empty(),
            TxTableAction::Truncate(_) => false,
            TxTableAction::Vacuum(_) => false,
            TxTableAction::Drop => false,
        })
    }

    pub(super) fn stage_schema(&mut self, update: PreparedSchemaUpdate) {
        if !update.is_empty() {
            self.actions.push(TxTableAction::Schema(update));
        }
    }

    pub(super) fn stage_properties(&mut self, update: PreparedTablePropertyUpdate) {
        self.actions.push(TxTableAction::Properties(update));
    }

    pub(super) fn stage_vacuum(
        &mut self,
        vacuum: PreparedVacuum,
    ) -> IcebergResult<()> {
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

    pub(super) fn stage_truncate(&mut self, expected_metadata_location: String) {
        self.invalidate_combined_delta();
        self.actions.push(TxTableAction::Truncate(TxTruncateAction {
            expected_metadata_location,
        }));
    }

    pub(super) fn stage_drop(&mut self) {
        self.invalidate_combined_delta();
        self.actions.push(TxTableAction::Drop);
    }

    pub(super) fn is_dropped(&self) -> bool {
        matches!(self.actions.last(), Some(TxTableAction::Drop))
    }

    pub(super) fn record_data_files(
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

    pub(super) fn record_position_delete_file(
        &mut self,
        delete_file: DataFile,
        referenced_data_files: Vec<String>,
    ) -> IcebergResult<()> {
        let epoch = self.current_data_epoch_mut();
        Arc::make_mut(&mut epoch.delta)
            .add_position_delete_file(delete_file, referenced_data_files)?;
        Ok(())
    }

    pub(super) fn record_remove_delete_file(
        &mut self,
        identity: DeleteFileIdentity,
    ) -> IcebergResult<()> {
        let epoch = self.current_data_epoch_mut();
        Arc::make_mut(&mut epoch.delta).remove_delete_file(identity)?;
        Ok(())
    }

    pub(super) fn record_remove_data_file(
        &mut self,
        file_path: String,
    ) -> IcebergResult<()> {
        let epoch = self.current_data_epoch_mut();
        Arc::make_mut(&mut epoch.delta).remove_data_file(file_path)?;
        Ok(())
    }

    pub(super) fn record_validation(&mut self, validation: RowDeltaValidation) {
        self.current_data_epoch_mut().validations.push(validation);
    }

    pub(super) fn overlay_metadata(
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

    pub(super) fn combined_delta(&self) -> IcebergResult<Option<Arc<SnapshotDelta>>> {
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
