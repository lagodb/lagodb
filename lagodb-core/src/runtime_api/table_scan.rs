//! Exact-build table-scan ABI shared by the runtime and provider DSOs.
//!
//! The ABI transports only PostgreSQL-owned plan data, fixed-layout values,
//! opaque bound/task handles, and an Apache Arrow C Stream written into caller
//! storage. Rust ownership and Arrow/DataFusion objects never cross directly.

use std::ffi::{CStr, c_char, c_void};
use std::mem::size_of;
use std::ptr;

use pgrx::pg_sys;

use super::{CallbackErrorReport, TableScanRuntimePredicate};

pub const TABLE_SCAN_UNSUPPORTED: u32 = 0;
pub const TABLE_SCAN_PLANNED: u32 = 1;
pub const TABLE_SCAN_FAILED: u32 = 2;

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
    /// Provider-approved exact or widened expression used for EXPLAIN.
    pub pruning_expression: *mut pg_sys::Expr,
    pub rows_read: f64,
    pub bytes_read: f64,
    pub startup_cost: f64,
}

impl Default for PlannedTableScanResult {
    fn default() -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            plan_data: ptr::null_mut(),
            pruning_expression: ptr::null_mut(),
            rows_read: 0.0,
            bytes_read: 0.0,
            startup_cost: 0.0,
        }
    }
}

/// Begin-time request for binding one provider-owned scan to its statement
/// snapshot and Arrow schema without planning physical file tasks.
/// `plan_data` is borrowed read-only for the synchronous callback.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanBindRequest {
    pub struct_size: u32,
    pub plan_data: *const pg_sys::List,
}

/// Statement binding returned by the provider.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BoundTableScanResult {
    pub struct_size: u32,
    pub bound: *mut c_void,
}

/// Result of negotiating one concrete predicate against a statement-bound
/// provider snapshot. A zero support code means unsupported.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanPredicateResult {
    pub struct_size: u32,
    pub support: u32,
    /// Provider-owned immutable predicate artifact. Null only when support is
    /// zero; ownership transfers to the runtime until `release_predicate`.
    pub predicate: *mut c_void,
}

impl Default for TableScanPredicateResult {
    fn default() -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            support: 0,
            predicate: ptr::null_mut(),
        }
    }
}

impl Default for BoundTableScanResult {
    fn default() -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            bound: ptr::null_mut(),
        }
    }
}

/// Provider-reported facts derived exactly from one run's planned task list.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableScanTaskMetrics {
    /// Number of provider task values in this run's physical plan.
    pub planned_tasks: u64,
    /// Number of distinct primary data files referenced by those tasks.
    pub planned_files: u64,
    /// Sum of primary data-file byte ranges represented by those tasks.
    pub planned_bytes: u64,
}

/// Output of run-local physical task planning.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlannedTableScanTasks {
    pub struct_size: u32,
    pub planned: *mut c_void,
    pub metrics: TableScanTaskMetrics,
}

/// Per-run physical task-planning inputs established after DataFusion has
/// finished building any fixed runtime filters.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanTaskPlanningRequest {
    pub struct_size: u32,
    /// Source-schema column positions requested from the provider in output
    /// order. The provider reader must expose exactly this projected schema.
    pub projected_columns: *const usize,
    pub projected_column_count: usize,
    /// Statement-stable provider predicate handles retained by the physical
    /// scan plan and borrowed for this synchronous callback.
    pub static_predicates: *const *const c_void,
    pub static_predicate_count: usize,
    /// Optional complete fixed predicate retained by this QueryRun.
    pub runtime_predicate: *const c_void,
}

impl Default for PlannedTableScanTasks {
    fn default() -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            planned: ptr::null_mut(),
            metrics: TableScanTaskMetrics::default(),
        }
    }
}

/// Per-run serial stream limits supplied by the engine.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanStreamRequest {
    pub struct_size: u32,
    pub maximum_batch_rows: u64,
    /// Engine-owned error slot that remains live until stream release.
    pub stream_error: *mut CallbackErrorReport,
    /// Optional evolving predicate slot. Its address remains stable until the
    /// Arrow stream release callback returns; the engine updates it only
    /// between serialized `get_next` calls.
    pub evolving_predicate: *const TableScanRuntimePredicate,
}

pub type PlanTableScan = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    request: *const TableScanPlanningRequest,
    output: *mut PlannedTableScanResult,
    error: *mut CallbackErrorReport,
) -> u32;

pub type BindTableScan = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    request: *const TableScanBindRequest,
    output: *mut BoundTableScanResult,
    error: *mut CallbackErrorReport,
) -> u32;

pub type NegotiateTableScanPredicate = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    bound: *mut c_void,
    predicate: *const TableScanRuntimePredicate,
    output: *mut TableScanPredicateResult,
    error: *mut CallbackErrorReport,
) -> u32;

pub type PlanTableScanTasks = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    bound: *mut c_void,
    request: *const TableScanTaskPlanningRequest,
    output: *mut PlannedTableScanTasks,
    error: *mut CallbackErrorReport,
) -> u32;

/// Export the bound scan's Arrow schema into caller-owned
/// `FFI_ArrowSchema` storage. The core ABI deliberately keeps that storage
/// opaque so it does not acquire an Arrow dependency.
pub type GetBoundTableScanSchema = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    bound: *mut c_void,
    schema: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

/// Populate an `arrow_array::ffi_stream::FFI_ArrowArrayStream` in caller-owned
/// storage. The core ABI stays Arrow-independent, so the storage is opaque here.
pub type OpenTableScanStream = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    bound: *mut c_void,
    planned: *mut c_void,
    request: *const TableScanStreamRequest,
    stream: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

pub type ReleasePlannedTableScan = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    planned: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

pub type ReleaseTableScanPredicate = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    predicate: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

pub type ReleaseBoundTableScan = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    bound: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

/// PostgreSQL storage objects served by one table-scan callback bundle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableScanRoutes {
    AccessMethod(&'static CStr),
    ForeignDataWrapper(&'static CStr),
    AccessMethodAndForeignDataWrapper {
        access_method: &'static CStr,
        foreign_data_wrapper: &'static CStr,
    },
}

impl TableScanRoutes {
    #[inline]
    pub const fn access_method(name: &'static CStr) -> Self {
        Self::AccessMethod(name)
    }

    #[inline]
    pub const fn foreign_data_wrapper(name: &'static CStr) -> Self {
        Self::ForeignDataWrapper(name)
    }

    #[inline]
    pub const fn access_method_and_foreign_data_wrapper(
        access_method: &'static CStr,
        foreign_data_wrapper: &'static CStr,
    ) -> Self {
        Self::AccessMethodAndForeignDataWrapper {
            access_method,
            foreign_data_wrapper,
        }
    }

    #[inline]
    const fn access_method_name(self) -> *const c_char {
        match self {
            Self::AccessMethod(name)
            | Self::AccessMethodAndForeignDataWrapper {
                access_method: name,
                ..
            } => name.as_ptr(),
            Self::ForeignDataWrapper(_) => ptr::null(),
        }
    }

    #[inline]
    const fn foreign_data_wrapper_name(self) -> *const c_char {
        match self {
            Self::ForeignDataWrapper(name)
            | Self::AccessMethodAndForeignDataWrapper {
                foreign_data_wrapper: name,
                ..
            } => name.as_ptr(),
            Self::AccessMethod(_) => ptr::null(),
        }
    }
}

/// One provider's serial table-scan capability and PostgreSQL storage routes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanDescriptor {
    struct_size: u32,
    access_method_name: *const c_char,
    foreign_data_wrapper_name: *const c_char,
    context: *mut c_void,
    plan_scan: Option<PlanTableScan>,
    bind_scan: Option<BindTableScan>,
    get_bound_schema: Option<GetBoundTableScanSchema>,
    negotiate_predicate: Option<NegotiateTableScanPredicate>,
    plan_scan_tasks: Option<PlanTableScanTasks>,
    open_serial_stream: Option<OpenTableScanStream>,
    release_predicate: Option<ReleaseTableScanPredicate>,
    release_planned: Option<ReleasePlannedTableScan>,
    release_bound: Option<ReleaseBoundTableScan>,
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

impl TableScanBindRequest {
    #[must_use]
    pub fn new(plan_data: *const pg_sys::List) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            plan_data,
        }
    }
}

impl TableScanTaskPlanningRequest {
    #[must_use]
    pub fn new(
        projected_columns: &[usize],
        static_predicates: &[*const c_void],
        runtime_predicate: *const c_void,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            projected_columns: projected_columns.as_ptr(),
            projected_column_count: projected_columns.len(),
            static_predicates: static_predicates.as_ptr(),
            static_predicate_count: static_predicates.len(),
            runtime_predicate,
        }
    }
}

impl TableScanStreamRequest {
    #[must_use]
    pub fn new(
        maximum_batch_rows: u64,
        stream_error: *mut CallbackErrorReport,
        evolving_predicate: *const TableScanRuntimePredicate,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            maximum_batch_rows,
            stream_error,
            evolving_predicate,
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
    /// panics, and produce bound/task/stream values that remain valid under the
    /// engine's current-thread serial `Send + Sync` adaptation. Route names
    /// must remain live for the backend lifetime.
    #[must_use]
    pub const unsafe fn new(
        routes: TableScanRoutes,
        context: *mut c_void,
        plan_scan: PlanTableScan,
        bind_scan: BindTableScan,
        get_bound_schema: GetBoundTableScanSchema,
        negotiate_predicate: NegotiateTableScanPredicate,
        plan_scan_tasks: PlanTableScanTasks,
        open_serial_stream: OpenTableScanStream,
        release_predicate: ReleaseTableScanPredicate,
        release_planned: ReleasePlannedTableScan,
        release_bound: ReleaseBoundTableScan,
    ) -> Self {
        Self {
            struct_size: size_of_u32::<Self>(),
            access_method_name: routes.access_method_name(),
            foreign_data_wrapper_name: routes.foreign_data_wrapper_name(),
            context,
            plan_scan: Some(plan_scan),
            bind_scan: Some(bind_scan),
            get_bound_schema: Some(get_bound_schema),
            negotiate_predicate: Some(negotiate_predicate),
            plan_scan_tasks: Some(plan_scan_tasks),
            open_serial_stream: Some(open_serial_stream),
            release_predicate: Some(release_predicate),
            release_planned: Some(release_planned),
            release_bound: Some(release_bound),
        }
    }

    /// Construct an arbitrary raw descriptor layout for exact-build ABI
    /// integration or conformance testing.
    ///
    /// # Safety
    ///
    /// A descriptor that is accepted by the runtime must satisfy every
    /// callback, lifetime, panic-containment, and single-thread contract from
    /// [`Self::new`], including backend-lifetime NUL-terminated route names.
    /// Supplying an intentionally invalid layout is only valid when it is
    /// passed synchronously to runtime validation and never used.
    #[must_use]
    pub const unsafe fn from_raw_parts(
        struct_size: u32,
        access_method_name: *const c_char,
        foreign_data_wrapper_name: *const c_char,
        context: *mut c_void,
        plan_scan: Option<PlanTableScan>,
        bind_scan: Option<BindTableScan>,
        get_bound_schema: Option<GetBoundTableScanSchema>,
        negotiate_predicate: Option<NegotiateTableScanPredicate>,
        plan_scan_tasks: Option<PlanTableScanTasks>,
        open_serial_stream: Option<OpenTableScanStream>,
        release_predicate: Option<ReleaseTableScanPredicate>,
        release_planned: Option<ReleasePlannedTableScan>,
        release_bound: Option<ReleaseBoundTableScan>,
    ) -> Self {
        Self {
            struct_size,
            access_method_name,
            foreign_data_wrapper_name,
            context,
            plan_scan,
            bind_scan,
            get_bound_schema,
            negotiate_predicate,
            plan_scan_tasks,
            open_serial_stream,
            release_predicate,
            release_planned,
            release_bound,
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
    pub const fn plan_scan(&self) -> Option<PlanTableScan> {
        self.plan_scan
    }

    #[inline]
    pub const fn bind_scan(&self) -> Option<BindTableScan> {
        self.bind_scan
    }

    #[inline]
    pub const fn get_bound_schema(&self) -> Option<GetBoundTableScanSchema> {
        self.get_bound_schema
    }

    #[inline]
    pub const fn negotiate_predicate(&self) -> Option<NegotiateTableScanPredicate> {
        self.negotiate_predicate
    }

    #[inline]
    pub const fn plan_scan_tasks(&self) -> Option<PlanTableScanTasks> {
        self.plan_scan_tasks
    }

    #[inline]
    pub const fn open_serial_stream(&self) -> Option<OpenTableScanStream> {
        self.open_serial_stream
    }

    #[inline]
    pub const fn release_predicate(&self) -> Option<ReleaseTableScanPredicate> {
        self.release_predicate
    }

    #[inline]
    pub const fn release_planned(&self) -> Option<ReleasePlannedTableScan> {
        self.release_planned
    }

    #[inline]
    pub const fn release_bound(&self) -> Option<ReleaseBoundTableScan> {
        self.release_bound
    }
}
