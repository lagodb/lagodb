//! Arrow C Stream reader and its provider-error ownership.

use std::sync::Arc;

use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};

use super::handles::PlannedTableScanHandle;

/// Engine-side Arrow reader retaining the stream's fixed-layout error slot.
pub(in crate::datafusion) struct ProviderStreamReader {
    // Field order is intentional: Arrow release runs before the error slot is
    // freed, so the release callback can contain and record a Drop panic.
    reader: ArrowArrayStreamReader,
    planned: Arc<PlannedTableScanHandle>,
}

impl ProviderStreamReader {
    pub(super) fn new(
        reader: ArrowArrayStreamReader,
        planned: Arc<PlannedTableScanHandle>,
    ) -> Self {
        Self { reader, planned }
    }
}

impl Iterator for ProviderStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next() {
            Some(Err(arrow_error)) => match self
                .planned
                .stream_error
                .take_error("table scan stream batch")
            {
                Some(error) => {
                    Some(Err(ArrowError::from_external_error(Box::new(error))))
                }
                None => Some(Err(arrow_error)),
            },
            result => result,
        }
    }
}

impl RecordBatchReader for ProviderStreamReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}
