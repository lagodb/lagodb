//! Exact-build table-scan ABI shared by the runtime and provider DSOs.
//!
//! The ABI transports only PostgreSQL-owned plan data, fixed-layout values,
//! opaque bound/task handles, and an Apache Arrow C Stream written into caller
//! storage. Rust ownership and Arrow/DataFusion objects never cross directly.

use std::ffi::{CStr, c_char, c_void};
use std::ptr::{self, NonNull};

use pgrx::pg_sys;

use super::{AbiSlice, CallbackErrorReport, TableScanRuntimePredicate};

pub const TABLE_SCAN_UNSUPPORTED: u32 = 0;
pub const TABLE_SCAN_PLANNED: u32 = 1;
pub const TABLE_SCAN_FAILED: u32 = 2;

/// Borrowed planner facts for one relation leaf.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanPlanningRequest<'a> {
    /// PostgreSQL user attributes to produce in request order. Empty requests
    /// visible rows without materializing user columns.
    pub projected_attnos: AbiSlice<'a, pg_sys::AttrNumber>,
    /// Optional complete scan predicate. The typed provider adapter applies
    /// provider-aware conservative negotiation; exact execution remains owned
    /// by the query engine.
    pub predicate_expression: *mut pg_sys::Node,
    /// PostgreSQL `checkAsUser` identity. `InvalidOid` means use the current
    /// effective user; plans must retain that sentinel until execution bind.
    pub check_as_user_id: pg_sys::Oid,
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
/// `plan_data` is borrowed read-only for the synchronous callback. `estate`
/// must be the initialized executor owning that plan; providers may retain
/// owned read views in its query context, but must not retain borrowed handles.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanBindRequest {
    pub plan_data: *const pg_sys::List,
    /// Live executor owning the snapshot and provider read-view lifetime.
    pub estate: *mut pg_sys::EState,
}

/// Statement binding returned by the provider.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BoundTableScanResult {
    pub bound: *mut c_void,
}

/// Result of negotiating one concrete predicate against a statement-bound
/// provider snapshot. A zero support code means unsupported.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanPredicateResult {
    pub support: u32,
    /// Provider-owned immutable predicate artifact. Null only when support is
    /// zero; ownership transfers to the runtime until `release_predicate`.
    pub predicate: *mut c_void,
}

impl Default for TableScanPredicateResult {
    fn default() -> Self {
        Self {
            support: 0,
            predicate: ptr::null_mut(),
        }
    }
}

impl Default for BoundTableScanResult {
    fn default() -> Self {
        Self {
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
    pub planned: *mut c_void,
    pub metrics: TableScanTaskMetrics,
    /// Whether `exact_rows` is the exact output cardinality of this task plan,
    /// after its predicates and deletes. Zero denotes unavailable statistics.
    pub has_exact_rows: u8,
    pub exact_rows: u64,
}

/// Physical task-planning inputs. The engine may also request an unfiltered
/// plan before optimization to obtain exact aggregate statistics.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanTaskPlanningRequest<'a> {
    /// Source-schema column positions requested from the provider in output
    /// order. The provider reader must expose exactly this projected schema.
    pub projected_columns: AbiSlice<'a, usize>,
    /// Statement-stable provider predicate handles retained by the physical
    /// scan plan and borrowed for this synchronous callback.
    pub static_predicates: AbiSlice<'a, NonNull<c_void>>,
    /// Optional complete fixed predicate retained by this QueryRun.
    pub runtime_predicate: Option<NonNull<c_void>>,
    /// Nonzero requests an optional exact output row count. Zero skips this
    /// computation. This affects returned statistics, not the task inventory.
    pub request_exact_rows: u8,
}

impl Default for PlannedTableScanTasks {
    fn default() -> Self {
        Self {
            planned: ptr::null_mut(),
            metrics: TableScanTaskMetrics::default(),
            has_exact_rows: 0,
            exact_rows: 0,
        }
    }
}

/// Per-run stream limits supplied by the engine.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanStreamRequest {
    pub maximum_batch_rows: u64,
    /// Engine-owned error slot that remains live until stream release.
    pub stream_error: NonNull<CallbackErrorReport>,
    /// Optional evolving predicate slot. Its address remains stable until the
    /// Arrow stream release callback returns; the engine updates it only
    /// between serialized `get_next` calls.
    pub evolving_predicate: *const TableScanRuntimePredicate,
}

/// Plan a scan and export its projected Arrow schema into caller-owned
/// `FFI_ArrowSchema` storage. The caller initializes that storage as empty and
/// releases it on every outcome. Only `TABLE_SCAN_PLANNED` publishes a schema;
/// no bound scan or execution tasks are created by this callback.
pub type PlanTableScan = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    request: *const TableScanPlanningRequest<'_>,
    output: *mut PlannedTableScanResult,
    schema: *mut c_void,
    error: *mut CallbackErrorReport,
) -> u32;

/// Decide whether an arbitrary valid PostgreSQL foreign server belongs to this
/// provider. `owned` is written only when the callback returns
/// [`super::CALLBACK_OK`].
pub type OwnsForeignServer = unsafe extern "C-unwind" fn(
    context: *mut c_void,
    server_oid: pg_sys::Oid,
    owned: *mut bool,
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
    request: *const TableScanTaskPlanningRequest<'_>,
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

/// Stable internal routes served by one table-scan callback bundle. A foreign
/// route name identifies the callback bundle after planning; catalog ownership
/// is established separately by [`OwnsForeignServer`].
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
    pub(crate) const fn access_method_name(self) -> *const c_char {
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
    pub(crate) const fn foreign_data_wrapper_name(self) -> *const c_char {
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

/// Complete callback set used to construct a table-scan descriptor.
///
/// This is a Rust construction interface. The runtime ABI retains the flat
/// callback fields in [`TableScanDescriptor`]. Every callback is required.
pub struct TableScanCallbacks {
    pub owns_foreign_server: OwnsForeignServer,
    pub plan_scan: PlanTableScan,
    pub bind_scan: BindTableScan,
    pub get_bound_schema: GetBoundTableScanSchema,
    pub negotiate_predicate: NegotiateTableScanPredicate,
    pub plan_scan_tasks: PlanTableScanTasks,
    pub open_stream: OpenTableScanStream,
    pub release_predicate: ReleaseTableScanPredicate,
    pub release_planned: ReleasePlannedTableScan,
    pub release_bound: ReleaseBoundTableScan,
}

/// One provider's base table-scan capability and stable internal routes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanDescriptor {
    access_method_name: *const c_char,
    foreign_data_wrapper_name: *const c_char,
    context: *mut c_void,
    owns_foreign_server: Option<OwnsForeignServer>,
    plan_scan: Option<PlanTableScan>,
    bind_scan: Option<BindTableScan>,
    get_bound_schema: Option<GetBoundTableScanSchema>,
    negotiate_predicate: Option<NegotiateTableScanPredicate>,
    plan_scan_tasks: Option<PlanTableScanTasks>,
    open_stream: Option<OpenTableScanStream>,
    release_predicate: Option<ReleaseTableScanPredicate>,
    release_planned: Option<ReleasePlannedTableScan>,
    release_bound: Option<ReleaseBoundTableScan>,
}

impl TableScanBindRequest {
    #[must_use]
    pub fn new(plan_data: *const pg_sys::List, estate: *mut pg_sys::EState) -> Self {
        Self { plan_data, estate }
    }
}

impl<'a> TableScanTaskPlanningRequest<'a> {
    #[must_use]
    pub const fn new(
        projected_columns: &'a [usize],
        static_predicates: &'a [NonNull<c_void>],
        runtime_predicate: Option<NonNull<c_void>>,
    ) -> Self {
        Self {
            projected_columns: AbiSlice::new(projected_columns),
            static_predicates: AbiSlice::new(static_predicates),
            runtime_predicate,
            request_exact_rows: 0,
        }
    }
}

impl TableScanStreamRequest {
    #[must_use]
    pub fn new(
        maximum_batch_rows: u64,
        stream_error: NonNull<CallbackErrorReport>,
        evolving_predicate: *const TableScanRuntimePredicate,
    ) -> Self {
        Self {
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
    /// engine's current-thread `Send + Sync` adaptation. Route names
    /// must remain live for the backend lifetime.
    #[must_use]
    pub const unsafe fn new(
        routes: TableScanRoutes,
        context: *mut c_void,
        callbacks: TableScanCallbacks,
    ) -> Self {
        Self {
            access_method_name: routes.access_method_name(),
            foreign_data_wrapper_name: routes.foreign_data_wrapper_name(),
            context,
            owns_foreign_server: Some(callbacks.owns_foreign_server),
            plan_scan: Some(callbacks.plan_scan),
            bind_scan: Some(callbacks.bind_scan),
            get_bound_schema: Some(callbacks.get_bound_schema),
            negotiate_predicate: Some(callbacks.negotiate_predicate),
            plan_scan_tasks: Some(callbacks.plan_scan_tasks),
            open_stream: Some(callbacks.open_stream),
            release_predicate: Some(callbacks.release_predicate),
            release_planned: Some(callbacks.release_planned),
            release_bound: Some(callbacks.release_bound),
        }
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
    pub const fn owns_foreign_server(&self) -> Option<OwnsForeignServer> {
        self.owns_foreign_server
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
    pub const fn open_stream(&self) -> Option<OpenTableScanStream> {
        self.open_stream
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
