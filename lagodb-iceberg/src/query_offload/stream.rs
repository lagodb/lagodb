//! Provider error-boundary wrappers around the catalog-independent streams.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use lagodb_arrow::scan::TableScanStream;

use crate::scan::columnar::ArrowStream;
use crate::scan::parallel::WorkerStream as ScanWorkerStream;

use super::error::Error;

pub(super) struct Stream(ArrowStream);

impl Stream {
    pub(super) fn new(stream: ArrowStream) -> Self {
        Self(stream)
    }
}

impl TableScanStream for Stream {
    type Error = Error;

    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error> {
        Ok(self.0.next_batch()?)
    }
}

pub(super) struct WorkerStream(ScanWorkerStream);

impl WorkerStream {
    pub(super) fn new(stream: ScanWorkerStream) -> Self {
        Self(stream)
    }
}

impl TableScanStream for WorkerStream {
    type Error = Error;

    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, Self::Error> {
        Ok(self.0.next_batch()?)
    }
}
