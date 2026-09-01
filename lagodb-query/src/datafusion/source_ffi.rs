//! Engine-side ownership of provider prepared handles and Arrow C Streams.

use std::cell::UnsafeCell;
use std::ffi::c_void;
use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use lagodb_core::diag::PgReportError;
use lagodb_core::query_contract::SourceId;
use lagodb_core::runtime_api::{
    FFI_OPERATION_FAILED, FFI_OPERATION_OK, FfiErrorRecord, OpenQuerySourceStream,
    PrepareQuerySource, QuerySourcePrepareRequest, QuerySourceStreamRequest,
    ReleasePreparedQuerySource,
};
use pgrx::{pg_sys, prelude::PgSqlErrorCode};

/// Backend-thread-bound callbacks for one validated provider source.
///
/// This wrapper can cross the `lagodb-base`/`lagodb-query` crate boundary, but
/// cannot cross a thread boundary. It exposes no prepared handle or stream;
/// [`SerialCountExecution`](super::SerialCountExecution) consumes it while
/// constructing the sole PostgreSQL-owned execution lifecycle.
#[derive(Clone, Copy)]
pub struct SerialSourceCallbacks {
    context: *mut c_void,
    prepare_source: PrepareQuerySource,
    open_serial_stream: OpenQuerySourceStream,
    release_prepared: ReleasePreparedQuerySource,
    backend_thread: PhantomData<Rc<()>>,
}

impl SerialSourceCallbacks {
    /// Construct from a descriptor already validated by the runtime directory.
    ///
    /// # Safety
    ///
    /// The callback code and context must remain live for the backend lifetime.
    /// Callbacks must either originate from the typed provider adapter or from
    /// an unsafe raw registration that upholds the identical contract. The
    /// returned wrapper must be created, consumed, and dropped on the current
    /// PostgreSQL backend thread.
    pub unsafe fn from_validated_callbacks(
        context: *mut c_void,
        prepare_source: PrepareQuerySource,
        open_serial_stream: OpenQuerySourceStream,
        release_prepared: ReleasePreparedQuerySource,
    ) -> Self {
        Self {
            context,
            prepare_source,
            open_serial_stream,
            release_prepared,
            backend_thread: PhantomData,
        }
    }

    /// Prepare an immutable source handle on the PostgreSQL backend thread.
    ///
    /// # Safety
    ///
    /// `plan_data` must be a live provider plan frame in the active executor
    /// memory context.
    pub(super) unsafe fn prepare(
        self,
        source: SourceId,
        plan_data: *mut pg_sys::List,
    ) -> Result<PreparedSourceHandle, PgReportError> {
        let request = QuerySourcePrepareRequest::new(source, plan_data);
        let mut handle = std::ptr::null_mut();
        let mut error = FfiErrorRecord::default();
        // SAFETY: this method's contract supplies live plan data; registration
        // guarantees backend-live context/callback pointers and the stack
        // outputs remain writable for the synchronous call.
        let status = unsafe {
            (self.prepare_source)(self.context, &request, &mut handle, &mut error)
        };
        self.operation_result(status, &error, "query source prepare")?;
        let handle = NonNull::new(handle).ok_or_else(|| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "query source prepare returned a null handle",
            )
        })?;
        Ok(PreparedSourceHandle {
            source: self,
            handle: Some(handle),
            stream_error: Mutex::new(None),
        })
    }

    fn operation_result(
        self,
        status: u32,
        error: &FfiErrorRecord,
        operation: &'static str,
    ) -> Result<(), PgReportError> {
        match status {
            FFI_OPERATION_OK => Ok(()),
            FFI_OPERATION_FAILED => {
                // SAFETY: callbacks allocate error text in the active backend
                // context and this method consumes it synchronously.
                Err(unsafe { error.to_error(operation) })
            }
            status => Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("{operation} returned unknown status {status}"),
            )),
        }
    }
}

/// Engine-owned opaque prepared source.
pub(super) struct PreparedSourceHandle {
    source: SerialSourceCallbacks,
    handle: Option<NonNull<c_void>>,
    stream_error: Mutex<Option<Arc<StreamErrorSlot>>>,
}

// SAFETY: this private adapter is reachable only through the thread-bound
// `SerialCountExecution` owner. Its audited physical plan has one partition and
// only synchronous Aggregate/Projection/source operators, and its current-thread
// runtime polls and drops every run-local stream before releasing this handle on
// the owning PostgreSQL backend thread. The typed `QuerySourceAdapter` proves the
// opaque handle/callback type pairing; raw registration must uphold the same
// pairing and backend-lifetime contract.
unsafe impl Send for PreparedSourceHandle {}
// SAFETY: DataFusion shares the private handle only inside the same audited
// single-partition plan. The owner never exposes the plan or handle and cannot
// itself be sent or shared across threads.
unsafe impl Sync for PreparedSourceHandle {}

impl PreparedSourceHandle {
    /// Finish the previously dropped serial stream and surface any error its
    /// Arrow release callback recorded before another run is started.
    pub(super) fn finish_serial_stream(&self) -> Result<(), PgReportError> {
        match self.take_closed_stream_error()? {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) fn open_serial_stream(
        &self,
        maximum_batch_rows: u64,
    ) -> Result<ProviderStreamReader, PgReportError> {
        let mut current_error = self.stream_error.lock().map_err(|_| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "query source stream error state was poisoned",
            )
        })?;
        // Keep the installed slot intact until a replacement reader exists.
        if let Some(previous) = current_error.as_ref() {
            if Arc::strong_count(previous) != 1 {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "query source opened more than one serial stream",
                ));
            }
            if let Some(error) = previous.take_error("query source stream release") {
                return Err(error);
            }
        }
        let stream_error = Arc::new(StreamErrorSlot::new());
        let request = QuerySourceStreamRequest::new(
            maximum_batch_rows,
            stream_error.as_mut_ptr(),
        );
        let mut stream = FFI_ArrowArrayStream::empty();
        let mut error = FfiErrorRecord::default();
        // SAFETY: the prepared handle is open, the request/error slot outlive
        // the returned stream, and output storage is a live Arrow stream value.
        let status = unsafe {
            (self.source.open_serial_stream)(
                self.source.context,
                self.handle.expect("prepared source is open").as_ptr(),
                &request,
                (&mut stream as *mut FFI_ArrowArrayStream).cast(),
                &mut error,
            )
        };
        self.source
            .operation_result(status, &error, "query source stream open")?;
        match ArrowArrayStreamReader::try_new(stream) {
            Ok(reader) => {
                *current_error = Some(Arc::clone(&stream_error));
                Ok(ProviderStreamReader {
                    reader,
                    error: stream_error,
                })
            }
            Err(error) => match stream_error.take_error("query source stream schema")
            {
                Some(error) => Err(error),
                None => Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
                    format!(
                        "query source returned an invalid Arrow C Stream: {error}"
                    ),
                )),
            },
        }
    }

    pub(super) fn close(mut self) -> Result<(), PgReportError> {
        self.release()
    }

    fn release(&mut self) -> Result<(), PgReportError> {
        let Some(handle) = self.handle else {
            return Ok(());
        };
        let stream_error = self.take_closed_stream_error()?;
        self.handle = None;
        let mut error = FfiErrorRecord::default();
        // SAFETY: `handle` was produced by this registered descriptor and is
        // consumed exactly once; the stack error record is writable.
        let status = unsafe {
            (self.source.release_prepared)(
                self.source.context,
                handle.as_ptr(),
                &mut error,
            )
        };
        let prepared_result =
            self.source
                .operation_result(status, &error, "query source release");
        match (stream_error, prepared_result) {
            (Some(stream_error), Err(prepared_error)) => Err(stream_error
                .contextualize(
                    "query source stream release failed",
                    Some(format!(
                        "prepared source release also failed: {prepared_error}"
                    )),
                )),
            (Some(error), Ok(())) | (None, Err(error)) => Err(error),
            (None, Ok(())) => Ok(()),
        }
    }

    /// Consume an error recorded while closing the last stream.
    ///
    /// An error return means stream ownership cannot be proven closed, so the
    /// prepared handle must remain open. A provider callback error belongs to
    /// an already-closed stream and is returned inside `Ok` so prepared cleanup
    /// can still run and combine both cleanup failures.
    fn take_closed_stream_error(
        &self,
    ) -> Result<Option<PgReportError>, PgReportError> {
        let mut current_error = self.stream_error.lock().map_err(|_| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "query source stream error state was poisoned",
            )
        })?;
        let Some(stream_error) = current_error.as_ref() else {
            return Ok(None);
        };
        if Arc::strong_count(stream_error) != 1 {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                "query source stream remained live during prepared source release",
            ));
        }
        let stream_error = current_error
            .take()
            .expect("closed stream error slot remains installed");
        Ok(stream_error.take_error("query source stream release"))
    }
}

struct StreamErrorSlot(UnsafeCell<FfiErrorRecord>);

impl StreamErrorSlot {
    fn new() -> Self {
        Self(UnsafeCell::new(FfiErrorRecord::default()))
    }

    fn as_mut_ptr(&self) -> *mut FfiErrorRecord {
        self.0.get()
    }

    fn is_set(&self) -> bool {
        // SAFETY: Arrow callbacks and engine inspection are serialized on the
        // PostgreSQL backend main thread.
        unsafe { (*self.0.get()).is_set() }
    }

    fn take_error(&self, operation: &'static str) -> Option<PgReportError> {
        if !self.is_set() {
            return None;
        }
        // SAFETY: the exporter wrote this record synchronously in the
        // still-live executor memory context, and callback/consumer access is
        // serialized on the backend main thread.
        let error = unsafe { (*self.0.get()).to_error(operation) };
        // SAFETY: the same serialized access permits clearing the consumed
        // record before the next callback.
        unsafe { *self.0.get() = FfiErrorRecord::default() };
        Some(error)
    }
}

// SAFETY: the slot exists only to satisfy DataFusion's `Send` stream contract.
// LagoDB invokes its exporter and reads the record on one backend main thread.
unsafe impl Send for StreamErrorSlot {}
// SAFETY: the closed-world runtime never performs concurrent callback/read
// access; synchronization of stream ownership is handled by the parent mutex.
unsafe impl Sync for StreamErrorSlot {}

/// Engine-side Arrow reader retaining the stream's fixed-layout error slot.
pub(super) struct ProviderStreamReader {
    // Field order is intentional: Arrow release runs before the error slot is
    // freed, so the release callback can contain and record a Drop panic.
    reader: ArrowArrayStreamReader,
    error: Arc<StreamErrorSlot>,
}

impl Iterator for ProviderStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next() {
            Some(Err(arrow_error)) => {
                match self.error.take_error("query source stream batch") {
                    Some(error) => {
                        Some(Err(ArrowError::from_external_error(Box::new(error))))
                    }
                    None => Some(Err(arrow_error)),
                }
            }
            result => result,
        }
    }
}

impl RecordBatchReader for ProviderStreamReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

impl Drop for PreparedSourceHandle {
    fn drop(&mut self) {
        // Normal query lifecycle calls `close` at its PostgreSQL error boundary.
        // This fallback still guarantees exactly-once release during unwinding;
        // the typed contract requires non-panicking release and the FFI layer
        // contains a provider violation.
        let _ = self.release();
    }
}

impl fmt::Debug for PreparedSourceHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedSourceHandle")
            .field("is_open", &self.handle.is_some())
            .finish()
    }
}
