//! Typed adapter for the optional parallel-worker table-scan facet.

use std::ffi::c_void;
use std::marker::PhantomData;
use std::ptr;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use lagodb_core::diag::PgReportError;
use lagodb_core::hooks::register_table_scan_worker;
use lagodb_core::runtime_api::{
    CALLBACK_OK, CallbackErrorReport, PreparedWorkerSourceResult, SourceWorkId,
    TableScanTaskMetrics, TableScanWorkerDescriptor, TableScanWorkerSourceRequest,
    TableScanWorkerStreamRequest, WORKER_SOURCE_FAILED, WORKER_SOURCE_READY,
    WORKER_SOURCE_UNSUPPORTED, WorkerTableScanSourceResult,
};

use super::export;
use super::provider::{ScanSupport, TableScanProvider, TableScanStream};

/// Immutable provider payload copied into the parallel query protocol.
pub struct WorkerSourcePayload {
    bytes: Box<[u8]>,
    work_count: u32,
    task_metrics: TableScanTaskMetrics,
}

impl WorkerSourcePayload {
    /// Construct a payload whose work identifiers are exactly
    /// `0..work_count`. Empty inventories are not parallel-capable.
    pub fn new(
        bytes: Box<[u8]>,
        work_count: u32,
        task_metrics: TableScanTaskMetrics,
    ) -> Result<Self, &'static str> {
        if bytes.is_empty() {
            return Err("worker source payload is empty");
        }
        if work_count == 0 {
            return Err("worker source has no work units");
        }
        Ok(Self {
            bytes,
            work_count,
            task_metrics,
        })
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[inline]
    pub const fn work_count(&self) -> u32 {
        self.work_count
    }

    pub const fn task_metrics(&self) -> TableScanTaskMetrics {
        self.task_metrics
    }
}

/// Worker-local stream limits. Parallel execution intentionally has no
/// evolving provider predicate slot: worker payloads are immutable for one
/// isolated run.
#[derive(Clone, Copy)]
pub struct WorkerStreamOptions {
    maximum_batch_rows: u64,
}

impl WorkerStreamOptions {
    pub(super) const fn new(maximum_batch_rows: u64) -> Self {
        Self { maximum_batch_rows }
    }

    #[inline]
    pub const fn maximum_batch_rows(self) -> u64 {
        self.maximum_batch_rows
    }
}

/// Optional worker reconstruction capability for a [`TableScanProvider`].
///
/// The leader invokes `prepare_worker_source` only after normal provider task
/// planning. A worker decodes those exact tasks and opens assigned work IDs;
/// neither method may choose a snapshot or traverse manifests.
pub trait TableScanWorkerProvider: TableScanProvider {
    type WorkerSource: Send + Sync + 'static;
    type WorkerStream: TableScanStream<Error = Self::Error> + Send + 'static;

    fn prepare_worker_source(
        &self,
        bound: &Self::BoundScan,
        planned: &Self::PlannedTasks,
    ) -> Result<ScanSupport<WorkerSourcePayload>, Self::Error>;

    /// Reconstruct a worker source over an immutable shared payload.
    ///
    /// # Safety
    ///
    /// The payload may be retained by `WorkerSource`. The engine guarantees
    /// that its DSM mapping remains attached until the returned source and all
    /// streams borrowing it have been released.
    unsafe fn decode_worker_source(
        &self,
        payload: &[u8],
    ) -> Result<Self::WorkerSource, Self::Error>;

    fn open_worker_stream(
        &self,
        source: &Self::WorkerSource,
        work_ids: &[SourceWorkId],
        options: WorkerStreamOptions,
    ) -> Result<Self::WorkerStream, Self::Error>;
}

/// Complete C-compatible worker descriptor for one typed provider.
pub struct TableScanWorkerAdapter<P>(PhantomData<P>);

impl<P: TableScanWorkerProvider> TableScanWorkerAdapter<P> {
    fn descriptor(provider: &'static P) -> TableScanWorkerDescriptor {
        // SAFETY: every callback is monomorphized for `P`, and the provider is
        // backend-static.
        unsafe {
            TableScanWorkerDescriptor::new(
                P::ROUTES,
                ptr::from_ref(provider).cast_mut().cast(),
                Self::prepare_source,
                export::release_boxed::<WorkerSourcePayload>,
                Self::decode_source,
                Self::open_stream,
                export::release_boxed::<P::WorkerSource>,
            )
        }
    }

    pub fn register(provider: &'static P) {
        // SAFETY: the descriptor is generated solely from this typed adapter.
        unsafe { register_table_scan_worker(Self::descriptor(provider)) }
    }

    unsafe fn provider(context: *mut c_void) -> &'static P {
        unsafe { &*context.cast::<P>() }
    }

    unsafe extern "C-unwind" fn prepare_source(
        context: *mut c_void,
        bound: *mut c_void,
        planned: *mut c_void,
        output: *mut PreparedWorkerSourceResult,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let mut outcome = WORKER_SOURCE_UNSUPPORTED;
        let operation = || {
            let provider = unsafe { Self::provider(context) };
            let bound = unsafe { &*bound.cast::<P::BoundScan>() };
            let planned = unsafe { &*planned.cast::<P::PlannedTasks>() };
            let support = provider
                .prepare_worker_source(bound, planned)
                .map_err(PgReportError::from_domain_error)?;
            let ScanSupport::Planned(payload) = support else {
                return Ok(());
            };
            let data = payload.bytes().as_ptr();
            let data_len = payload.bytes().len();
            let work_count = payload.work_count();
            let task_metrics = payload.task_metrics();
            unsafe {
                *output = PreparedWorkerSourceResult {
                    payload: Box::into_raw(Box::new(payload)).cast(),
                    data,
                    data_len,
                    work_count,
                    task_metrics,
                };
            }
            outcome = WORKER_SOURCE_READY;
            Ok(())
        };
        if unsafe { (&mut *error).capture(operation) } == CALLBACK_OK {
            outcome
        } else {
            WORKER_SOURCE_FAILED
        }
    }

    unsafe extern "C-unwind" fn decode_source(
        context: *mut c_void,
        request: *const TableScanWorkerSourceRequest<'_>,
        output: *mut WorkerTableScanSourceResult,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            let request = unsafe { &*request };
            let bytes = request.data.as_slice();
            let provider = unsafe { Self::provider(context) };
            // SAFETY: the engine-side worker source owner keeps the immutable
            // DSM payload mapped until release_source has completed.
            let source = unsafe { provider.decode_worker_source(bytes) }
                .map_err(PgReportError::from_domain_error)?;
            unsafe {
                *output = WorkerTableScanSourceResult {
                    source: Box::into_raw(Box::new(source)).cast(),
                };
            }
            Ok(())
        };
        unsafe { (&mut *error).capture(operation) }
    }

    unsafe extern "C-unwind" fn open_stream(
        context: *mut c_void,
        source: *mut c_void,
        request: *const TableScanWorkerStreamRequest<'_>,
        output: *mut c_void,
        error: *mut CallbackErrorReport,
    ) -> u32 {
        let operation = || {
            let request = unsafe { &*request };
            let work_ids = request.work_ids.as_slice();
            let provider = unsafe { Self::provider(context) };
            let source = unsafe { &*source.cast::<P::WorkerSource>() };
            let stream = provider
                .open_worker_stream(
                    source,
                    work_ids,
                    WorkerStreamOptions::new(request.maximum_batch_rows),
                )
                .map_err(PgReportError::from_domain_error)?;
            unsafe {
                output
                    .cast::<FFI_ArrowArrayStream>()
                    .write(export::stream(stream, request.stream_error));
            }
            Ok(())
        };
        unsafe { (&mut *error).capture(operation) }
    }
}
