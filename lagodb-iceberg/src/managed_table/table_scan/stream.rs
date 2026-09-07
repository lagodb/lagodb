//! Run-local lazy Arrow stream for a prepared Iceberg table scan.

use std::mem;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use iceberg_lite::scan::ArrowRecordBatchIterator;
use lagodb_arrow::query_source::TableScanStream;

use crate::error::IcebergError;

use super::{IcebergTableScanError, prepared::PreparedIcebergScan};

enum IcebergBatchCursor {
    Pending(Arc<PreparedIcebergScan>, usize),
    Open(ArrowRecordBatchIterator),
    Finished,
}

pub(super) struct IcebergArrowStream {
    schema: SchemaRef,
    cursor: IcebergBatchCursor,
}

impl IcebergArrowStream {
    pub(super) fn new(prepared: Arc<PreparedIcebergScan>, batch_size: usize) -> Self {
        Self {
            schema: Arc::clone(&prepared.arrow_schema),
            cursor: IcebergBatchCursor::Pending(prepared, batch_size),
        }
    }

    fn open_if_needed(&mut self) -> Result<(), IcebergTableScanError> {
        let IcebergBatchCursor::Pending(_, _) = &self.cursor else {
            return Ok(());
        };
        let IcebergBatchCursor::Pending(prepared, batch_size) =
            mem::replace(&mut self.cursor, IcebergBatchCursor::Finished)
        else {
            unreachable!("cursor state was matched immediately above")
        };
        let cursor = prepared
            .open_batches(batch_size)
            .map_err(IcebergError::from)
            .map_err(IcebergTableScanError::from)?;
        self.cursor = IcebergBatchCursor::Open(cursor);
        Ok(())
    }
}

impl TableScanStream for IcebergArrowStream {
    type Error = IcebergTableScanError;

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error> {
        self.open_if_needed()?;

        let next = match &mut self.cursor {
            IcebergBatchCursor::Open(cursor) => cursor.next(),
            IcebergBatchCursor::Finished => return Ok(None),
            IcebergBatchCursor::Pending(_, _) => {
                unreachable!("open_if_needed resolves the pending state")
            }
        };
        match next {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(error)) => {
                self.cursor = IcebergBatchCursor::Finished;
                Err(IcebergTableScanError::from(IcebergError::from(error)))
            }
            None => {
                self.cursor = IcebergBatchCursor::Finished;
                Ok(None)
            }
        }
    }
}
