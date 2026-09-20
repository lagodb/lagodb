//! Engine-side ownership of parallel-worker table-scan callbacks.

use std::ffi::c_void;
use std::fmt;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use lagodb_core::diag::PgReportError;
use lagodb_core::runtime_api::{
    CALLBACK_FAILED, CALLBACK_OK, CallbackErrorReport, DecodeTableScanWorkerSource,
    OpenTableScanWorkerStream, PrepareTableScanWorkerSource,
    PreparedWorkerSourceResult, ReleasePreparedTableScanWorkerSource,
    ReleaseTableScanWorkerSource, SourceWorkId, TableScanTaskMetrics,
    TableScanWorkerDescriptor, TableScanWorkerSourceRequest,
    TableScanWorkerStreamRequest, WORKER_SOURCE_FAILED, WORKER_SOURCE_READY,
    WORKER_SOURCE_UNSUPPORTED, WorkerTableScanSourceResult,
};
use pgrx::prelude::PgSqlErrorCode;

use super::bound_scan::{BoundTableScanHandle, PlannedTableScanHandle};
use super::stream_reader::StreamErrorSlot;

#[derive(Clone, Copy)]
pub struct WorkerTableScanCallbacks {
    context: *mut c_void,
    prepare_source: PrepareTableScanWorkerSource,
    release_prepared_source: ReleasePreparedTableScanWorkerSource,
    decode_source: DecodeTableScanWorkerSource,
    open_stream: OpenTableScanWorkerStream,
    release_source: ReleaseTableScanWorkerSource,
    backend_thread: PhantomData<Rc<()>>,
}

impl WorkerTableScanCallbacks {
    /// # Safety
    ///
    /// The descriptor was validated by the runtime registry and remains live
    /// for this backend process.
    pub unsafe fn from_validated_descriptor(
        descriptor: &TableScanWorkerDescriptor,
    ) -> Option<Self> {
        Some(Self {
            context: descriptor.context(),
            prepare_source: descriptor.prepare_source()?,
            release_prepared_source: descriptor.release_prepared_source()?,
            decode_source: descriptor.decode_source()?,
            open_stream: descriptor.open_stream()?,
            release_source: descriptor.release_source()?,
            backend_thread: PhantomData,
        })
    }

    fn operation_result(
        self,
        status: u32,
        error: &CallbackErrorReport,
        operation: &'static str,
    ) -> Result<(), PgReportError> {
        match status {
            CALLBACK_OK => Ok(()),
            CALLBACK_FAILED => {
                // SAFETY: the synchronous callback wrote a backend-context
                // diagnostic whose storage remains live for this inspection.
                Err(unsafe { error.to_error(operation) })
            }
            status => Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("{operation} returned unknown status {status}"),
            )),
        }
    }

    pub(in crate::datafusion) fn prepare(
        self,
        bound: &BoundTableScanHandle,
        planned: &PlannedTableScanHandle,
    ) -> Result<Option<PreparedWorkerSource>, PgReportError> {
        let mut output = PreparedWorkerSourceResult::default();
        let mut error = CallbackErrorReport::default();
        // SAFETY: registry validation pairs this context with its callbacks;
        // bound/planned belong to that provider and all stack outputs remain
        // writable until the synchronous backend-thread callback returns.
        let status = unsafe {
            (self.prepare_source)(
                self.context,
                bound.as_ptr(),
                planned.as_ptr(),
                &mut output,
                &mut error,
            )
        };
        match status {
            WORKER_SOURCE_READY => {}
            WORKER_SOURCE_UNSUPPORTED => return Ok(None),
            WORKER_SOURCE_FAILED => {
                // SAFETY: preparation wrote the live callback diagnostic.
                return Err(unsafe {
                    error.to_error("table scan worker source preparation")
                });
            }
            status => {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!(
                        "table scan worker source preparation returned unknown status {status}"
                    ),
                ));
            }
        }
        let payload = NonNull::new(output.payload).ok_or_else(|| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan worker source preparation returned a null payload",
            )
        })?;
        let result = if output.struct_size
            != size_of::<PreparedWorkerSourceResult>() as u32
            || output.data_len == 0
            || output.data.is_null()
            || output.work_count == 0
        {
            Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan worker source preparation returned an invalid result",
            ))
        } else {
            // SAFETY: READY lends this nonempty range from the still-owned
            // provider handle. The FFI boundary requires one copy before the
            // handle is released; statement ownership remains unique after it.
            let bytes =
                unsafe { core::slice::from_raw_parts(output.data, output.data_len) }
                    .to_vec()
                    .into_boxed_slice();
            Ok(PreparedWorkerSource {
                bytes,
                work_count: output.work_count,
                task_metrics: output.task_metrics,
            })
        };
        let cleanup = self.release_prepared(payload).err();
        match (result, cleanup) {
            (Err(primary), Some(cleanup)) => Err(primary.contextualize(
                "invalid worker source payload",
                Some(format!("payload release also failed: {cleanup}")),
            )),
            (Err(error), None) | (Ok(_), Some(error)) => Err(error),
            (Ok(prepared), None) => Ok(Some(prepared)),
        }
    }

    fn release_prepared(self, payload: NonNull<c_void>) -> Result<(), PgReportError> {
        let mut error = CallbackErrorReport::default();
        // SAFETY: preparation transferred this unique release handle; registry
        // validation guarantees the matching release callback and context.
        let status = unsafe {
            (self.release_prepared_source)(self.context, payload.as_ptr(), &mut error)
        };
        self.operation_result(status, &error, "table scan worker payload release")
    }

    pub(in crate::datafusion) fn decode(
        self,
        data: &[u8],
    ) -> Result<WorkerTableScanSource, PgReportError> {
        let request = TableScanWorkerSourceRequest::new(data);
        let mut output = WorkerTableScanSourceResult::default();
        let mut error = CallbackErrorReport::default();
        // SAFETY: request borrows immutable DSM bytes whose mapping outlives
        // the decoded source; the validated callback owns its backend-static
        // context and stack outputs.
        let status = unsafe {
            (self.decode_source)(self.context, &request, &mut output, &mut error)
        };
        self.operation_result(status, &error, "table scan worker source decode")?;
        let handle = NonNull::new(output.source).ok_or_else(|| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan worker source decode returned a null source",
            )
        })?;
        let mut source = WorkerTableScanSource {
            callbacks: self,
            handle: Some(handle),
        };
        if output.struct_size != size_of::<WorkerTableScanSourceResult>() as u32 {
            let primary = PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "table scan worker source decode returned an incompatible result",
            );
            return match source.release() {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(primary.contextualize(
                    "invalid decoded worker source",
                    Some(format!("source release also failed: {cleanup}")),
                )),
            };
        }
        Ok(source)
    }
}

pub(in crate::datafusion) struct PreparedWorkerSource {
    bytes: Box<[u8]>,
    work_count: u32,
    task_metrics: TableScanTaskMetrics,
}

impl PreparedWorkerSource {
    pub const fn work_count(&self) -> u32 {
        self.work_count
    }

    pub(in crate::datafusion) fn into_parts(
        self,
    ) -> (Box<[u8]>, u32, TableScanTaskMetrics) {
        (self.bytes, self.work_count, self.task_metrics)
    }
}

pub(in crate::datafusion) struct WorkerTableScanSource {
    callbacks: WorkerTableScanCallbacks,
    handle: Option<NonNull<c_void>>,
}

// SAFETY: the worker facet requires WorkerSource: Send + Sync + 'static.
// Registration proves the callback/handle pairing, and the worker owner
// releases all plan/stream shares on its backend's current-thread runtime
// before releasing this backend-local callback context.
unsafe impl Send for WorkerTableScanSource {}
// SAFETY: open_stream borrows the provider's immutable WorkerSource; the
// worker facet permits independent Send streams retaining that source. The
// backend-local callbacks themselves execute only on the worker owner thread.
unsafe impl Sync for WorkerTableScanSource {}

impl fmt::Debug for WorkerTableScanSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerTableScanSource")
            .field("is_open", &self.handle.is_some())
            .finish_non_exhaustive()
    }
}

impl WorkerTableScanSource {
    pub(in crate::datafusion) fn open_stream(
        self: &Arc<Self>,
        work_ids: &[SourceWorkId],
        maximum_batch_rows: u64,
    ) -> Result<WorkerProviderStreamReader, PgReportError> {
        let error_slot = Arc::new(StreamErrorSlot::new());
        let request = TableScanWorkerStreamRequest::new(
            work_ids,
            maximum_batch_rows,
            error_slot.as_mut_ptr(),
        );
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut error = CallbackErrorReport::default();
        // SAFETY: this source was decoded by the matching worker facet; the
        // borrowed IDs, writable Arrow stream and error records remain live
        // for the synchronous call. The stream's error slot is retained until
        // Arrow release, and its source is retained by the returned reader.
        let status = unsafe {
            (self.callbacks.open_stream)(
                self.callbacks.context,
                self.handle
                    .expect("worker table scan source is open")
                    .as_ptr(),
                &request,
                (&mut stream as *mut FFI_ArrowArrayStream).cast(),
                &mut error,
            )
        };
        self.callbacks.operation_result(
            status,
            &error,
            "table scan worker stream open",
        )?;
        let reader = ArrowArrayStreamReader::try_new(stream).map_err(|error| {
            error_slot.take_error("table scan worker stream schema").unwrap_or_else(|| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                    format!("table scan worker returned an invalid Arrow C Stream: {error}"),
                )
            })
        })?;
        Ok(WorkerProviderStreamReader {
            schema: reader.schema(),
            reader: Some(reader),
            _source: Arc::clone(self),
            error: error_slot,
        })
    }

    fn release(&mut self) -> Result<(), PgReportError> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        let mut error = CallbackErrorReport::default();
        // SAFETY: taking the handle transfers unique ownership to the matching
        // callback, before any error is propagated or Drop can run again.
        let status = unsafe {
            (self.callbacks.release_source)(
                self.callbacks.context,
                handle.as_ptr(),
                &mut error,
            )
        };
        self.callbacks.operation_result(
            status,
            &error,
            "table scan worker source release",
        )
    }

    pub(in crate::datafusion) fn close(mut self) -> Result<(), PgReportError> {
        self.release()
    }
}

impl Drop for WorkerTableScanSource {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

pub(in crate::datafusion) struct WorkerProviderStreamReader {
    // Arrow release runs while both the source and its error record are live.
    reader: Option<ArrowArrayStreamReader>,
    schema: SchemaRef,
    _source: Arc<WorkerTableScanSource>,
    error: Arc<StreamErrorSlot>,
}

impl WorkerProviderStreamReader {
    pub(in crate::datafusion) fn close(&mut self) -> Result<(), PgReportError> {
        drop(self.reader.take());
        match self.error.take_error("table scan worker stream release") {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Iterator for WorkerProviderStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.as_mut()?.next() {
            Some(Err(arrow_error)) => {
                match self.error.take_error("table scan worker stream batch") {
                    Some(error) => {
                        Some(Err(ArrowError::from_external_error(Box::new(error))))
                    }
                    None => Some(Err(arrow_error)),
                }
            }
            None => self
                .close()
                .err()
                .map(|error| Err(ArrowError::from_external_error(Box::new(error)))),
            result => result,
        }
    }
}

impl RecordBatchReader for WorkerProviderStreamReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
