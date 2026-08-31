//! Exact-build query-source ABI shared by the runtime and provider DSOs.
//!
//! The ABI transports only PostgreSQL-owned plan data, fixed-layout values,
//! opaque prepared handles, and an Apache Arrow C Stream written into caller
//! storage. Rust ownership and Arrow/DataFusion objects never cross directly.

use std::ffi::c_void;

use pgrx::pg_sys;

use crate::query_contract::SourceId;

use super::FfiErrorRecord;

pub const QUERY_SOURCE_NOT_OWNED: u32 = 0;
pub const QUERY_SOURCE_UNSUPPORTED: u32 = 1;
pub const QUERY_SOURCE_PLANNED: u32 = 2;
pub const QUERY_SOURCE_FAILED: u32 = 3;

/// S1M's sole source projection: produce visible rows without user columns.
pub const QUERY_SOURCE_PROJECTION_COUNT_ROWS: u32 = 1;

/// Borrowed planner facts for one relation leaf.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QuerySourcePlanningRequest {
    pub struct_size: u32,
    pub projection_kind: u32,
    pub source_index: usize,
    pub root: *mut pg_sys::PlannerInfo,
    pub relation: *mut pg_sys::RelOptInfo,
    pub range_table_index: pg_sys::Index,
    pub range_table_entry: *mut pg_sys::RangeTblEntry,
}

/// PostgreSQL-owned result produced by a successful source planning callback.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlannedQuerySource {
    pub struct_size: u32,
    pub plan_data: *mut pg_sys::List,
    pub estimated_rows: f64,
    pub estimated_scan_bytes: f64,
}

impl Default for PlannedQuerySource {
    fn default() -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            plan_data: std::ptr::null_mut(),
            estimated_rows: 0.0,
            estimated_scan_bytes: 0.0,
        }
    }
}

/// Begin-time request for reconstructing one provider-owned prepared source.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QuerySourcePrepareRequest {
    pub struct_size: u32,
    pub source_index: usize,
    pub plan_data: *mut pg_sys::List,
}

/// Per-run serial stream limits supplied by the engine.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QuerySourceStreamRequest {
    pub struct_size: u32,
    pub maximum_batch_rows: u64,
    /// Engine-owned error slot that remains live until stream release.
    pub stream_error: *mut FfiErrorRecord,
}

pub type PlanQuerySource = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    request: *const QuerySourcePlanningRequest,
    output: *mut PlannedQuerySource,
    error: *mut FfiErrorRecord,
) -> u32;

pub type PrepareQuerySource = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    request: *const QuerySourcePrepareRequest,
    prepared: *mut *mut c_void,
    error: *mut FfiErrorRecord,
) -> u32;

/// Populate an `arrow_array::ffi_stream::FFI_ArrowArrayStream` in caller-owned
/// storage. The core ABI stays Arrow-independent, so the storage is opaque here.
pub type OpenQuerySourceStream = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    prepared: *mut c_void,
    request: *const QuerySourceStreamRequest,
    stream: *mut c_void,
    error: *mut FfiErrorRecord,
) -> u32;

pub type ReleasePreparedQuerySource = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    prepared: *mut c_void,
    error: *mut FfiErrorRecord,
) -> u32;

/// One provider's serial source-leaf capability.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QuerySourceDescriptor {
    struct_size: u32,
    context: *mut c_void,
    plan_source: Option<PlanQuerySource>,
    prepare_source: Option<PrepareQuerySource>,
    open_serial_stream: Option<OpenQuerySourceStream>,
    release_prepared: Option<ReleasePreparedQuerySource>,
}

const fn size_of_u32<T>() -> u32 {
    let size = std::mem::size_of::<T>();
    assert!(size <= u32::MAX as usize, "runtime ABI type exceeds u32");
    size as u32
}

impl QuerySourcePlanningRequest {
    #[must_use]
    pub fn count_rows(
        source_index: usize,
        root: *mut pg_sys::PlannerInfo,
        relation: *mut pg_sys::RelOptInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            projection_kind: QUERY_SOURCE_PROJECTION_COUNT_ROWS,
            source_index,
            root,
            relation,
            range_table_index,
            range_table_entry,
        }
    }
}

impl QuerySourcePrepareRequest {
    #[must_use]
    pub fn new(source: SourceId, plan_data: *mut pg_sys::List) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            source_index: source.index(),
            plan_data,
        }
    }
}

impl QuerySourceStreamRequest {
    #[must_use]
    pub fn new(maximum_batch_rows: u64, stream_error: *mut FfiErrorRecord) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            maximum_batch_rows,
            stream_error,
        }
    }
}

impl QuerySourceDescriptor {
    /// Construct a raw query-source descriptor.
    ///
    /// # Safety
    ///
    /// Every callback must uphold the runtime ABI contract, keep `context`
    /// live for the backend lifetime, contain PostgreSQL errors and Rust
    /// panics, and produce prepared/stream values that remain valid under the
    /// engine's closed-world single-backend-thread `Send + Sync` adaptation.
    #[must_use]
    pub const unsafe fn new(
        context: *mut c_void,
        plan_source: PlanQuerySource,
        prepare_source: PrepareQuerySource,
        open_serial_stream: OpenQuerySourceStream,
        release_prepared: ReleasePreparedQuerySource,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            context,
            plan_source: Some(plan_source),
            prepare_source: Some(prepare_source),
            open_serial_stream: Some(open_serial_stream),
            release_prepared: Some(release_prepared),
        }
    }

    /// Construct an arbitrary raw descriptor layout for exact-build ABI
    /// integration or conformance testing.
    ///
    /// # Safety
    ///
    /// A descriptor that is accepted by the runtime must satisfy every
    /// callback, lifetime, panic-containment, and single-thread contract from
    /// [`Self::new`]. Supplying an intentionally invalid layout is only valid
    /// when it is passed synchronously to runtime validation and never used.
    #[must_use]
    pub const unsafe fn from_raw_parts(
        struct_size: u32,
        context: *mut c_void,
        plan_source: Option<PlanQuerySource>,
        prepare_source: Option<PrepareQuerySource>,
        open_serial_stream: Option<OpenQuerySourceStream>,
        release_prepared: Option<ReleasePreparedQuerySource>,
    ) -> Self {
        Self {
            struct_size,
            context,
            plan_source,
            prepare_source,
            open_serial_stream,
            release_prepared,
        }
    }

    #[inline]
    pub const fn struct_size(&self) -> u32 {
        self.struct_size
    }

    #[inline]
    pub const fn context(&self) -> *mut c_void {
        self.context
    }

    #[inline]
    pub const fn plan_source(&self) -> Option<PlanQuerySource> {
        self.plan_source
    }

    #[inline]
    pub const fn prepare_source(&self) -> Option<PrepareQuerySource> {
        self.prepare_source
    }

    #[inline]
    pub const fn open_serial_stream(&self) -> Option<OpenQuerySourceStream> {
        self.open_serial_stream
    }

    #[inline]
    pub const fn release_prepared(&self) -> Option<ReleasePreparedQuerySource> {
        self.release_prepared
    }
}
