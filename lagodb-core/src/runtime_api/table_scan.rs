//! Exact-build table-scan ABI shared by the runtime and provider DSOs.
//!
//! The ABI transports only PostgreSQL-owned plan data, fixed-layout values,
//! opaque prepared handles, and an Apache Arrow C Stream written into caller
//! storage. Rust ownership and Arrow/DataFusion objects never cross directly.

use std::ffi::c_void;
use std::mem::size_of;
use std::ptr;

use pgrx::pg_sys;

use crate::query_contract::ScanId;

use super::CallbackErrorReport;

pub const TABLE_SCAN_NOT_OWNED: u32 = 0;
pub const TABLE_SCAN_UNSUPPORTED: u32 = 1;
pub const TABLE_SCAN_PLANNED: u32 = 2;
pub const TABLE_SCAN_FAILED: u32 = 3;

/// Produce visible rows without materializing user columns.
pub const TABLE_SCAN_PROJECTION_ROW_COUNT: u32 = 1;
/// Produce the listed PostgreSQL user attributes in request order.
pub const TABLE_SCAN_PROJECTION_COLUMNS: u32 = 2;

/// Borrowed planner facts for one relation leaf.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanPlanningRequest {
    pub struct_size: u32,
    pub projection_kind: u32,
    pub projected_attnos: *const pg_sys::AttrNumber,
    pub projected_attno_count: usize,
    /// Optional complete scan predicate. The typed provider adapter applies
    /// provider-aware conservative negotiation; exact execution remains owned
    /// by the query engine.
    pub predicate_expression: *mut pg_sys::Node,
    pub effective_user_id: pg_sys::Oid,
    pub scan_index: usize,
    pub root: *mut pg_sys::PlannerInfo,
    pub relation: *mut pg_sys::RelOptInfo,
    pub range_table_index: pg_sys::Index,
    pub range_table_entry: *mut pg_sys::RangeTblEntry,
}

/// PostgreSQL-owned result produced by a successful scan planning callback.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlannedTableScanResult {
    pub struct_size: u32,
    pub plan_data: *mut pg_sys::List,
    /// Encoded negotiated `PredicateFragment`, or NIL when no pruning is safe.
    pub pruning_fragment: *mut pg_sys::List,
    /// PostgreSQL `List<Expr>` aligned with the fragment's runtime layout.
    pub pruning_binding_exprs: *mut pg_sys::List,
    /// Provider-approved exact or widened expression used for EXPLAIN.
    pub pruning_expression: *mut pg_sys::Expr,
    pub estimated_rows: f64,
    pub estimated_scan_bytes: f64,
}

impl Default for PlannedTableScanResult {
    fn default() -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            plan_data: ptr::null_mut(),
            pruning_fragment: ptr::null_mut(),
            pruning_binding_exprs: ptr::null_mut(),
            pruning_expression: ptr::null_mut(),
            estimated_rows: 0.0,
            estimated_scan_bytes: 0.0,
        }
    }
}

/// Begin-time request for reconstructing one provider-owned prepared scan.
/// `plan_data` is borrowed read-only for the synchronous callback.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanPrepareRequest {
    pub struct_size: u32,
    pub scan_index: usize,
    pub plan_data: *const pg_sys::List,
    pub runtime_values: *const TableScanRuntimeValue,
    pub runtime_value_count: usize,
}

/// Borrowed runtime datum passed only for the synchronous prepare callback.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanRuntimeValue {
    pub datum: pg_sys::Datum,
    pub is_null: bool,
}

/// Per-run serial stream limits supplied by the engine.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanStreamRequest {
    pub struct_size: u32,
    pub maximum_batch_rows: u64,
    /// Engine-owned error slot that remains live until stream release.
    pub stream_error: *mut CallbackErrorReport,
}

pub type PlanTableScan = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    request: *const TableScanPlanningRequest,
    output: *mut PlannedTableScanResult,
    error: *mut CallbackErrorReport,
) -> u32;

pub type PrepareTableScan = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    request: *const TableScanPrepareRequest,
    prepared: *mut *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

/// Export the prepared scan's Arrow schema into caller-owned
/// `FFI_ArrowSchema` storage. The core ABI deliberately keeps that storage
/// opaque so it does not acquire an Arrow dependency.
pub type GetPreparedTableScanSchema = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    prepared: *mut c_void,
    schema: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

/// Populate an `arrow_array::ffi_stream::FFI_ArrowArrayStream` in caller-owned
/// storage. The core ABI stays Arrow-independent, so the storage is opaque here.
pub type OpenTableScanStream = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    prepared: *mut c_void,
    request: *const TableScanStreamRequest,
    stream: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

pub type ReleasePreparedTableScan = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    prepared: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

/// One provider's serial table-scan capability.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanDescriptor {
    struct_size: u32,
    context: *mut c_void,
    plan_scan: Option<PlanTableScan>,
    prepare_scan: Option<PrepareTableScan>,
    get_prepared_schema: Option<GetPreparedTableScanSchema>,
    open_serial_stream: Option<OpenTableScanStream>,
    release_prepared: Option<ReleasePreparedTableScan>,
}

const fn size_of_u32<T>() -> u32 {
    let size = size_of::<T>();
    assert!(size <= u32::MAX as usize, "runtime ABI type exceeds u32");
    size as u32
}

impl TableScanPlanningRequest {
    #[must_use]
    pub fn row_count(
        scan_index: usize,
        predicate_expression: *mut pg_sys::Node,
        effective_user_id: pg_sys::Oid,
        root: *mut pg_sys::PlannerInfo,
        relation: *mut pg_sys::RelOptInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            projection_kind: TABLE_SCAN_PROJECTION_ROW_COUNT,
            projected_attnos: ptr::null(),
            projected_attno_count: 0,
            predicate_expression,
            effective_user_id,
            scan_index,
            root,
            relation,
            range_table_index,
            range_table_entry,
        }
    }

    #[must_use]
    pub fn columns(
        scan_index: usize,
        projected_attnos: &[pg_sys::AttrNumber],
        predicate_expression: *mut pg_sys::Node,
        effective_user_id: pg_sys::Oid,
        root: *mut pg_sys::PlannerInfo,
        relation: *mut pg_sys::RelOptInfo,
        range_table_index: pg_sys::Index,
        range_table_entry: *mut pg_sys::RangeTblEntry,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            projection_kind: TABLE_SCAN_PROJECTION_COLUMNS,
            projected_attnos: projected_attnos.as_ptr(),
            projected_attno_count: projected_attnos.len(),
            predicate_expression,
            effective_user_id,
            scan_index,
            root,
            relation,
            range_table_index,
            range_table_entry,
        }
    }
}

impl TableScanPrepareRequest {
    #[must_use]
    pub fn new(
        scan: ScanId,
        plan_data: *const pg_sys::List,
        runtime_values: &[TableScanRuntimeValue],
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            scan_index: scan.index(),
            plan_data,
            runtime_values: runtime_values.as_ptr(),
            runtime_value_count: runtime_values.len(),
        }
    }
}

impl TableScanStreamRequest {
    #[must_use]
    pub fn new(
        maximum_batch_rows: u64,
        stream_error: *mut CallbackErrorReport,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            maximum_batch_rows,
            stream_error,
        }
    }
}

impl TableScanDescriptor {
    /// Construct a raw table-scan descriptor.
    ///
    /// # Safety
    ///
    /// Every callback must uphold the runtime ABI contract, keep `context`
    /// live for the backend lifetime, contain PostgreSQL errors and Rust
    /// panics, and produce prepared/stream values that remain valid under the
    /// engine's current-thread serial `Send + Sync` adaptation.
    #[must_use]
    pub const unsafe fn new(
        context: *mut c_void,
        plan_scan: PlanTableScan,
        prepare_scan: PrepareTableScan,
        get_prepared_schema: GetPreparedTableScanSchema,
        open_serial_stream: OpenTableScanStream,
        release_prepared: ReleasePreparedTableScan,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            context,
            plan_scan: Some(plan_scan),
            prepare_scan: Some(prepare_scan),
            get_prepared_schema: Some(get_prepared_schema),
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
        plan_scan: Option<PlanTableScan>,
        prepare_scan: Option<PrepareTableScan>,
        get_prepared_schema: Option<GetPreparedTableScanSchema>,
        open_serial_stream: Option<OpenTableScanStream>,
        release_prepared: Option<ReleasePreparedTableScan>,
    ) -> Self {
        Self {
            struct_size,
            context,
            plan_scan,
            prepare_scan,
            get_prepared_schema,
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
    pub const fn plan_scan(&self) -> Option<PlanTableScan> {
        self.plan_scan
    }

    #[inline]
    pub const fn prepare_scan(&self) -> Option<PrepareTableScan> {
        self.prepare_scan
    }

    #[inline]
    pub const fn get_prepared_schema(&self) -> Option<GetPreparedTableScanSchema> {
        self.get_prepared_schema
    }

    #[inline]
    pub const fn open_serial_stream(&self) -> Option<OpenTableScanStream> {
        self.open_serial_stream
    }

    #[inline]
    pub const fn release_prepared(&self) -> Option<ReleasePreparedTableScan> {
        self.release_prepared
    }
}
