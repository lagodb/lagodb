//! Arrow C Stream reader and its provider-error ownership.

use std::cell::UnsafeCell;
use std::sync::Arc;

use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::CallbackErrorReport;

use super::bound_scan::PlannedTableScanHandle;

pub(super) struct StreamErrorSlot(UnsafeCell<CallbackErrorReport>);

impl StreamErrorSlot {
    pub(super) fn new() -> Self {
        Self(UnsafeCell::new(CallbackErrorReport::default()))
    }

    pub(super) fn as_mut_ptr(&self) -> *mut CallbackErrorReport {
        self.0.get()
    }

    fn is_set(&self) -> bool {
        // SAFETY: Arrow callbacks and engine inspection are serialized on the
        // PostgreSQL backend main thread.
        unsafe { (*self.0.get()).is_set() }
    }

    pub(super) fn take_error(
        &self,
        operation: &'static str,
    ) -> Option<PgReportError> {
        if !self.is_set() {
            return None;
        }
        // SAFETY: the exporter wrote this record synchronously in the
        // still-live executor memory context, and callback/consumer access is
        // serialized on the backend main thread.
        let error = unsafe { (*self.0.get()).to_error(operation) };
        // SAFETY: the same serialized access permits clearing the consumed
        // record before the next callback.
        unsafe { *self.0.get() = CallbackErrorReport::default() };
        Some(error)
    }
}

// SAFETY: the slot exists only to satisfy DataFusion's `Send` stream contract.
// LagoDB invokes its exporter and reads the record on one backend main thread.
unsafe impl Send for StreamErrorSlot {}
// SAFETY: the current-thread serial runtime never performs concurrent
// callback/read access; stream ownership is synchronized by the parent mutex.
unsafe impl Sync for StreamErrorSlot {}

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
