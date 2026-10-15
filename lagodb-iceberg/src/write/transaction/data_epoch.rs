//! Immutable command-owned writes within one ordered data epoch.

use std::cell::OnceCell;
use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::spec::DataFile;
use iceberg_lite::transaction::RowDeltaValidation;
use pgrx::pg_sys;

use crate::error::IcebergResult;
use crate::write::RowDeleteOutput;

/// Complete output of a relation-local write session. The CID belongs to the
/// executor/COPY command, not to the later time at which files are published.
#[derive(Debug)]
pub(crate) struct TxWriteBatch {
    write_command_id: pg_sys::CommandId,
    delta: Arc<SnapshotDelta>,
    validation: Option<RowDeltaValidation>,
}

impl TxWriteBatch {
    pub(crate) fn new(
        write_command_id: pg_sys::CommandId,
        data_files: Vec<DataFile>,
        delete_files: Vec<RowDeleteOutput>,
        validation: Option<RowDeltaValidation>,
    ) -> IcebergResult<Self> {
        let mut delta = SnapshotDelta::new();
        for file in data_files {
            delta.add_data_file(file)?;
        }
        for output in delete_files {
            for identity in output.removed_delete_files {
                delta.remove_delete_file(identity)?;
            }
            delta.add_position_delete_file(
                output.delete_file,
                output.referenced_data_files,
            )?;
        }
        Ok(Self {
            write_command_id,
            delta: Arc::new(delta),
            validation,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.delta.is_empty() && self.validation.is_none()
    }
}

/// Completion order is significant, but CIDs need not be ordered: a nested
/// command can finish before its outer command. Savepoints mark batch counts;
/// readers select batches by CID without changing their completion order.
#[derive(Debug, Clone, Default)]
pub(crate) struct TxDataEpoch {
    batches: Vec<Rc<TxWriteBatch>>,
    combined: OnceCell<Arc<SnapshotDelta>>,
}

impl TxDataEpoch {
    pub(crate) fn push(&mut self, batch: TxWriteBatch) {
        self.combined.take();
        self.batches.push(Rc::new(batch));
    }

    pub(crate) fn mark(&self) -> usize {
        self.batches.len()
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        self.combined.take();
        self.batches.truncate(len);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub(crate) fn validations(&self) -> Vec<RowDeltaValidation> {
        self.batches
            .iter()
            .filter_map(|batch| batch.validation.clone())
            .collect()
    }

    pub(crate) fn max_write_command_id(&self) -> Option<pg_sys::CommandId> {
        self.batches
            .iter()
            .map(|batch| batch.write_command_id)
            .max()
    }

    pub(crate) fn delta(&self) -> IcebergResult<&Arc<SnapshotDelta>> {
        if let Some(delta) = self.combined.get() {
            return Ok(delta);
        }
        let delta = if let [batch] = self.batches.as_slice() {
            Arc::clone(&batch.delta)
        } else {
            let mut delta = SnapshotDelta::new();
            self.append_visible(&mut delta, None)?;
            Arc::new(delta)
        };
        Ok(self.combined.get_or_init(|| delta))
    }

    pub(crate) fn append_visible(
        &self,
        delta: &mut SnapshotDelta,
        read_command_id: Option<pg_sys::CommandId>,
    ) -> IcebergResult<()> {
        for batch in &self.batches {
            if read_command_id.is_none_or(|cid| batch.write_command_id < cid) {
                delta.append_delta(&batch.delta)?;
            }
        }
        Ok(())
    }
}
