//! Worker-local Iceberg reader over a leader-planned task inventory.

mod execution;
mod grouping;
mod payload;

use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use iceberg_lite::arrow::ArrowReaderBuilder;
use iceberg_lite::io::FileIO;
use iceberg_lite::scan::{ArrowRecordBatchIterator, FileScanTask};
use lagodb_arrow::scan::TableScanStream;
use lagodb_core::runtime_api::SourceWorkId;

use super::ScanError;
use crate::error::IcebergError;
pub(crate) use execution::PostgresParallelExecution;
pub(crate) use grouping::{TaskGrouping, TaskGroupingConfig};
use payload::WorkerSourcePayload;

pub(crate) struct WorkerSource {
    file_io: FileIO,
    schema: SchemaRef,
    inventory: Mutex<payload::DecodedTaskInventory>,
}

impl WorkerSource {
    pub(crate) fn encode(
        schema: &SchemaRef,
        tasks: &[FileScanTask],
        grouped: grouping::GroupedTaskRanges,
    ) -> Result<Box<[u8]>, ScanError> {
        Self::encode_prefixed(&[], schema, tasks, grouped)
    }

    pub(crate) fn encode_prefixed(
        prefix: &[u8],
        schema: &SchemaRef,
        tasks: &[FileScanTask],
        grouped: grouping::GroupedTaskRanges,
    ) -> Result<Box<[u8]>, ScanError> {
        WorkerSourcePayload::encode(prefix, schema, tasks, grouped)
    }

    /// # Safety
    ///
    /// `data` must remain mapped and immutable until this source and every
    /// stream opened from it have been released.
    pub(crate) unsafe fn decode_shared(
        data: &[u8],
        file_io: FileIO,
    ) -> Result<Self, ScanError> {
        let inventory = unsafe { WorkerSourcePayload::decode_shared(data) }?;
        Ok(Self::from_inventory(file_io, inventory))
    }

    fn from_inventory(
        file_io: FileIO,
        inventory: payload::DecodedTaskInventory,
    ) -> Self {
        Self {
            file_io,
            schema: Arc::clone(&inventory.schema),
            inventory: Mutex::new(inventory),
        }
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn open(
        &self,
        work_ids: &[SourceWorkId],
        batch_size: usize,
    ) -> Result<WorkerStream, ScanError> {
        let mut tasks = Vec::new();
        for work_id in work_ids {
            tasks.extend(self.take_tasks(*work_id)?);
        }
        let batches = ArrowReaderBuilder::new(self.file_io.clone())
            .with_batch_size(batch_size)
            .build()
            .read(tasks)
            .map_err(IcebergError::from)
            .map_err(ScanError::from)?;
        Ok(WorkerStream {
            schema: Arc::clone(&self.schema),
            batches,
        })
    }

    pub(crate) fn take_tasks(
        &self,
        work_id: SourceWorkId,
    ) -> Result<Vec<FileScanTask>, ScanError> {
        self.inventory
            .lock()
            .map_err(|_| {
                ScanError::WorkerPayload(
                    "parallel task inventory lock was poisoned".to_owned(),
                )
            })?
            .take_tasks(work_id)
    }
}

pub(crate) struct WorkerStream {
    schema: SchemaRef,
    batches: ArrowRecordBatchIterator,
}

impl TableScanStream for WorkerStream {
    type Error = ScanError;

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error> {
        // Match the parallel engine's cooperative batch boundary. Its stream
        // wrapper checks STOP/PG interrupts before this call and immediately
        // after synchronous provider I/O returns; no cancellation check belongs
        // on the per-row/per-datum path.
        self.batches
            .next()
            .transpose()
            .map_err(IcebergError::from)
            .map_err(ScanError::from)
    }
}
