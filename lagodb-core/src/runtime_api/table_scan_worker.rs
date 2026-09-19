//! Exact-build ABI for reconstructing table-scan sources in parallel workers.

use std::ffi::{c_char, c_void};
use std::mem::size_of;
use std::ptr;

use super::{CallbackErrorReport, TableScanRoutes, TableScanTaskMetrics};

pub const WORKER_SOURCE_READY: u32 = 0;
pub const WORKER_SOURCE_UNSUPPORTED: u32 = 1;
pub const WORKER_SOURCE_FAILED: u32 = 2;

/// Stable identity of one provider-owned unit of source work.
///
/// The provider assigns dense identifiers while it builds the immutable worker
/// payload. The query engine only routes these identifiers; it never inspects
/// provider task contents.
pub type SourceWorkId = u32;

/// Provider-owned bytes and work inventory produced from one planned scan.
///
/// `payload` is an opaque release handle. `data` borrows storage owned by that
/// handle until `release_prepared_source` returns. The engine copies the bytes
/// once into its statement-owned parallel protocol buffer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PreparedWorkerSourceResult {
    pub struct_size: u32,
    pub payload: *mut c_void,
    pub data: *const u8,
    pub data_len: usize,
    pub work_count: u32,
    pub task_metrics: TableScanTaskMetrics,
}

impl Default for PreparedWorkerSourceResult {
    fn default() -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            payload: ptr::null_mut(),
            data: ptr::null(),
            data_len: 0,
            work_count: 0,
            task_metrics: TableScanTaskMetrics::default(),
        }
    }
}

/// Borrowed versioned payload supplied to a worker backend. The engine keeps
/// this immutable range mapped until the decoded source is released; a
/// provider may therefore retain a view into it under the decode callback's
/// safety contract.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanWorkerSourceRequest {
    pub struct_size: u32,
    pub data: *const u8,
    pub data_len: usize,
}

impl TableScanWorkerSourceRequest {
    #[must_use]
    pub fn new(data: &[u8]) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            data: data.as_ptr(),
            data_len: data.len(),
        }
    }
}

/// Opaque worker-local source reconstructed from one provider payload.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WorkerTableScanSourceResult {
    pub struct_size: u32,
    pub source: *mut c_void,
}

impl Default for WorkerTableScanSourceResult {
    fn default() -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            source: ptr::null_mut(),
        }
    }
}

/// Work assigned to one execution-plan partition in a worker backend.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanWorkerStreamRequest {
    pub struct_size: u32,
    pub work_ids: *const SourceWorkId,
    pub work_id_count: usize,
    pub maximum_batch_rows: u64,
    pub stream_error: *mut CallbackErrorReport,
}

impl TableScanWorkerStreamRequest {
    #[must_use]
    pub fn new(
        work_ids: &[SourceWorkId],
        maximum_batch_rows: u64,
        stream_error: *mut CallbackErrorReport,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            work_ids: work_ids.as_ptr(),
            work_id_count: work_ids.len(),
            maximum_batch_rows,
            stream_error,
        }
    }
}

pub type PrepareTableScanWorkerSource = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    bound: *mut c_void,
    planned: *mut c_void,
    output: *mut PreparedWorkerSourceResult,
    error: *mut CallbackErrorReport,
) -> u32;

pub type ReleasePreparedTableScanWorkerSource = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    payload: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

pub type DecodeTableScanWorkerSource = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    request: *const TableScanWorkerSourceRequest,
    output: *mut WorkerTableScanSourceResult,
    error: *mut CallbackErrorReport,
) -> u32;

/// Populate caller-owned `FFI_ArrowArrayStream` storage.
pub type OpenTableScanWorkerStream = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    source: *mut c_void,
    request: *const TableScanWorkerStreamRequest,
    stream: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

pub type ReleaseTableScanWorkerSource = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    source: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

/// Optional parallel-worker facet for a serial table-scan route.
///
/// This descriptor is registered in the same provider transaction as its
/// serial [`super::TableScanDescriptor`]. It owns no planning policy: it only
/// serializes an already planned task inventory and reconstructs worker-local
/// I/O state from that immutable payload.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanWorkerDescriptor {
    struct_size: u32,
    access_method_name: *const c_char,
    foreign_data_wrapper_name: *const c_char,
    context: *mut c_void,
    prepare_source: Option<PrepareTableScanWorkerSource>,
    release_prepared_source: Option<ReleasePreparedTableScanWorkerSource>,
    decode_source: Option<DecodeTableScanWorkerSource>,
    open_stream: Option<OpenTableScanWorkerStream>,
    release_source: Option<ReleaseTableScanWorkerSource>,
}

const fn size_of_u32<T>() -> u32 {
    let size = size_of::<T>();
    assert!(size <= u32::MAX as usize, "runtime ABI type exceeds u32");
    size as u32
}

impl TableScanWorkerDescriptor {
    /// # Safety
    ///
    /// Callbacks, context, and route strings must remain live for the backend
    /// lifetime. Handles must be released exactly once by their matching
    /// callback, and every callback must contain PostgreSQL errors and Rust
    /// panics at this ABI boundary.
    #[must_use]
    pub const unsafe fn new(
        routes: TableScanRoutes,
        context: *mut c_void,
        prepare_source: PrepareTableScanWorkerSource,
        release_prepared_source: ReleasePreparedTableScanWorkerSource,
        decode_source: DecodeTableScanWorkerSource,
        open_stream: OpenTableScanWorkerStream,
        release_source: ReleaseTableScanWorkerSource,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            access_method_name: routes.access_method_name(),
            foreign_data_wrapper_name: routes.foreign_data_wrapper_name(),
            context,
            prepare_source: Some(prepare_source),
            release_prepared_source: Some(release_prepared_source),
            decode_source: Some(decode_source),
            open_stream: Some(open_stream),
            release_source: Some(release_source),
        }
    }

    #[inline]
    pub const fn struct_size(&self) -> u32 {
        self.struct_size
    }

    #[inline]
    pub const fn access_method_name(&self) -> *const c_char {
        self.access_method_name
    }

    #[inline]
    pub const fn foreign_data_wrapper_name(&self) -> *const c_char {
        self.foreign_data_wrapper_name
    }

    #[inline]
    pub const fn context(&self) -> *mut c_void {
        self.context
    }

    #[inline]
    pub const fn prepare_source(&self) -> Option<PrepareTableScanWorkerSource> {
        self.prepare_source
    }

    #[inline]
    pub const fn release_prepared_source(
        &self,
    ) -> Option<ReleasePreparedTableScanWorkerSource> {
        self.release_prepared_source
    }

    #[inline]
    pub const fn decode_source(&self) -> Option<DecodeTableScanWorkerSource> {
        self.decode_source
    }

    #[inline]
    pub const fn open_stream(&self) -> Option<OpenTableScanWorkerStream> {
        self.open_stream
    }

    #[inline]
    pub const fn release_source(&self) -> Option<ReleaseTableScanWorkerSource> {
        self.release_source
    }
}
