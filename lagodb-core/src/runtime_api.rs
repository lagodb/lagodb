//! Exact-build PostgreSQL rendezvous ABI for runtime-owned backend services.
//!
//! # Trust model
//!
//! This is an internal ABI between `lagodb-base` and provider DSOs built
//! from the same `lagodb-core` source tree. It is not a general C
//! plugin interface and does not attempt to make arbitrary addresses or
//! malformed foreign allocations safe to inspect.
//!
//! Raw ABI callers must provide complete, initialized, correctly aligned
//! values of the declared `#[repr(C)]` types. Pointer/count pairs must describe
//! live arrays of the exact element type, callback and C-string pointers must
//! be valid, and registered callbacks and contexts must remain live for the
//! backend lifetime. Every runtime API type is an exact-build contract: the
//! runtime and every consuming extension must be built from the same source
//! tree and installed together. The build and install flow guarantees this;
//! it is the safety precondition for every cross-DSO read, and the runtime
//! does not verify it.
//!
//! Descriptors carry no per-type layout fields. The only compatibility check
//! is the [`SdkVersion`] a provider compares once when it connects. It is a
//! diagnostic for packages from different `lagodb-core` releases, so every
//! release must bump the workspace version. Like PostgreSQL's magic block,
//! which compares only the major version, it does not detect DSOs built from
//! different revisions under one version; installing such a mix violates the
//! contract above.

use std::ffi::{CStr, c_char, c_void};
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::OnceLock;

use pgrx::pg_sys::{self, ffi::pg_guard_ffi_boundary};

use crate::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceReport, TableMaintenanceStats,
};
mod abi_slice;
mod error;
mod planning;
mod registration;
mod storage_volume;
mod table_copy;
mod table_scan;
mod table_scan_predicate;
mod table_scan_worker;
mod version;

pub use abi_slice::AbiSlice;
pub use error::*;
pub use planning::*;
pub use registration::*;
pub use storage_volume::*;
pub use table_copy::TableCopyCallbacks;
pub use table_scan::*;
pub use table_scan_predicate::*;
pub use table_scan_worker::*;
pub use version::SdkVersion;

pub const RUNTIME_API_RENDEZVOUS: &CStr = c"lagodb.runtime_api";
pub const FORMAT_NAME_CAPACITY: usize = 32;

pub const STAGE_WORKER_WAKEUP_OK: u32 = 0;
pub const STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND: u32 = 1;
pub const STAGE_WORKER_WAKEUP_INVALID_REQUEST: u32 = 2;
pub const STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED: u32 = 3;

pub const REGISTER_OK: u32 = 0;
pub const REGISTER_INVALID_DESCRIPTOR: u32 = 1;
pub const REGISTER_DUPLICATE_NAME: u32 = 2;
pub const REGISTER_DUPLICATE_ACCESS_METHOD: u32 = 3;
pub const REGISTER_OUTSIDE_PROVIDER_BOOTSTRAP: u32 = 4;
pub const REGISTER_PROVIDER_LIBRARY_MISMATCH: u32 = 5;
pub const REGISTER_DUPLICATE_TABLE_SCAN_ROUTE: u32 = 6;

pub const UTILITY_ROUTE_PASS_THROUGH: u8 = 0;
pub const UTILITY_ROUTE_CONSUMED: u8 = 1;
/// A matching table consumer used only when no primary consumer claims COPY.
/// This is a predicate result; successful execution still returns CONSUMED.
pub const UTILITY_ROUTE_FALLBACK: u8 = 2;

pub const OBJECT_ACCESS_POST_CREATE: u32 = 1 << 0;
pub const OBJECT_ACCESS_DROP: u32 = 1 << 1;
pub const OBJECT_ACCESS_POST_ALTER: u32 = 1 << 2;
pub const OBJECT_ACCESS_NAMESPACE_SEARCH: u32 = 1 << 3;
pub const OBJECT_ACCESS_FUNCTION_EXECUTE: u32 = 1 << 4;
pub const OBJECT_ACCESS_TRUNCATE: u32 = 1 << 5;
pub const OBJECT_ACCESS_EVENTS_KNOWN: u32 = OBJECT_ACCESS_POST_CREATE
    | OBJECT_ACCESS_DROP
    | OBJECT_ACCESS_POST_ALTER
    | OBJECT_ACCESS_NAMESPACE_SEARCH
    | OBJECT_ACCESS_FUNCTION_EXECUTE
    | OBJECT_ACCESS_TRUNCATE;

#[must_use]
pub fn object_access_event_mask(
    access: pg_sys::ObjectAccessType::Type,
) -> Option<u32> {
    match access {
        pg_sys::ObjectAccessType::OAT_POST_CREATE => Some(OBJECT_ACCESS_POST_CREATE),
        pg_sys::ObjectAccessType::OAT_DROP => Some(OBJECT_ACCESS_DROP),
        pg_sys::ObjectAccessType::OAT_POST_ALTER => Some(OBJECT_ACCESS_POST_ALTER),
        pg_sys::ObjectAccessType::OAT_NAMESPACE_SEARCH => {
            Some(OBJECT_ACCESS_NAMESPACE_SEARCH)
        }
        pg_sys::ObjectAccessType::OAT_FUNCTION_EXECUTE => {
            Some(OBJECT_ACCESS_FUNCTION_EXECUTE)
        }
        pg_sys::ObjectAccessType::OAT_TRUNCATE => Some(OBJECT_ACCESS_TRUNCATE),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectAccessFilter {
    event_mask: u32,
    class_id: pg_sys::Oid,
}

impl ObjectAccessFilter {
    #[must_use]
    pub const fn new(event_mask: u32) -> Self {
        assert!(
            event_mask != 0 && event_mask & !OBJECT_ACCESS_EVENTS_KNOWN == 0,
            "object-access filter contains no events or unknown event bits"
        );
        Self {
            event_mask,
            class_id: pg_sys::InvalidOid,
        }
    }

    #[must_use]
    pub const fn for_class(mut self, class_id: pg_sys::Oid) -> Self {
        self.class_id = class_id;
        self
    }

    pub const fn event_mask(self) -> u32 {
        self.event_mask
    }

    pub const fn class_id(self) -> pg_sys::Oid {
        self.class_id
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RuntimeMaintenanceConfig {
    pub enabled: u8,
    pub _padding: [u8; 3],
    pub actor_threads: i32,
    pub batch_items: i32,
    pub retry_base_ms: i32,
    pub retry_max_ms: i32,
    pub retry_max_attempts: i32,
    pub request_timeout_ms: i32,
    pub shutdown_timeout_ms: i32,
    pub vacuum_max_input_objects: i32,
    pub vacuum_max_input_mb: i32,
    pub vacuum_max_group_objects: i32,
    pub vacuum_max_group_mb: i32,
}

const OPTION_VERBOSE: u32 = 1 << 0;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MaintenanceRequest {
    pub relation: pg_sys::Relation,
    pub mode: TableMaintenanceMode,
    pub option_flags: u32,
    pub max_input_objects: u64,
    pub max_input_bytes: u64,
    pub max_group_objects: u64,
    pub max_group_bytes: u64,
    pub command_time_ms: i64,
}

impl MaintenanceRequest {
    pub fn new(
        relation: pg_sys::Relation,
        mode: TableMaintenanceMode,
        options: TableMaintenanceOptions,
        budget: TableMaintenanceBudget,
        command_time: TableMaintenanceCommandTime,
    ) -> Self {
        Self {
            relation,
            mode,
            option_flags: u32::from(options.verbose) * OPTION_VERBOSE,
            max_input_objects: budget.max_input_objects,
            max_input_bytes: budget.max_input_bytes,
            max_group_objects: budget.max_group_objects,
            max_group_bytes: budget.max_group_bytes,
            command_time_ms: command_time.unix_epoch_ms(),
        }
    }

    pub fn options(self) -> TableMaintenanceOptions {
        TableMaintenanceOptions {
            verbose: self.option_flags & OPTION_VERBOSE != 0,
        }
    }

    pub fn budget(self) -> TableMaintenanceBudget {
        TableMaintenanceBudget {
            max_input_objects: self.max_input_objects,
            max_input_bytes: self.max_input_bytes,
            max_group_objects: self.max_group_objects,
            max_group_bytes: self.max_group_bytes,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MaintenanceReport {
    pub groups_rewritten: u64,
    pub input_objects: u64,
    pub input_bytes: u64,
    pub output_objects: u64,
    pub output_bytes: u64,
    pub snapshots_expired: u64,
    pub manifests_rewritten: u64,
    pub objects_scheduled_for_deletion: u64,
    pub cas_retries: u64,
}

impl From<TableMaintenanceReport> for MaintenanceReport {
    fn from(report: TableMaintenanceReport) -> Self {
        Self {
            groups_rewritten: report.groups_rewritten,
            input_objects: report.input_objects,
            input_bytes: report.input_bytes,
            output_objects: report.output_objects,
            output_bytes: report.output_bytes,
            snapshots_expired: report.snapshots_expired,
            manifests_rewritten: report.manifests_rewritten,
            objects_scheduled_for_deletion: report.objects_scheduled_for_deletion,
            cas_retries: report.cas_retries,
        }
    }
}

impl From<MaintenanceReport> for TableMaintenanceReport {
    fn from(report: MaintenanceReport) -> Self {
        // The runtime ABI carries only common counters. Start from Default so
        // the private provider-metrics collection remains valid and empty.
        let mut result = Self::default();
        result.groups_rewritten = report.groups_rewritten;
        result.input_objects = report.input_objects;
        result.input_bytes = report.input_bytes;
        result.output_objects = report.output_objects;
        result.output_bytes = report.output_bytes;
        result.snapshots_expired = report.snapshots_expired;
        result.manifests_rewritten = report.manifests_rewritten;
        result.objects_scheduled_for_deletion = report.objects_scheduled_for_deletion;
        result.cas_retries = report.cas_retries;
        result
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MaintenanceStats {
    pub format: [u8; FORMAT_NAME_CAPACITY],
    pub history_points: u64,
    pub current_content_objects: u64,
    pub current_content_bytes: u64,
    pub retained_content_objects: u64,
    pub retained_content_bytes: u64,
    pub current_data_objects: u64,
    pub current_data_bytes: u64,
    pub retained_data_objects: u64,
    pub retained_data_bytes: u64,
}

impl MaintenanceStats {
    pub fn try_from_stats(stats: TableMaintenanceStats) -> Option<Self> {
        let mut format = [0; FORMAT_NAME_CAPACITY];
        if let Some(value) = stats.format.as_deref() {
            let bytes = value.as_bytes();
            if bytes.len() >= FORMAT_NAME_CAPACITY || bytes.contains(&0) {
                return None;
            }
            format[..bytes.len()].copy_from_slice(bytes);
        }
        Some(Self {
            format,
            history_points: stats.history_points,
            current_content_objects: stats.current_content_objects,
            current_content_bytes: stats.current_content_bytes,
            retained_content_objects: stats.retained_content_objects,
            retained_content_bytes: stats.retained_content_bytes,
            current_data_objects: stats.current_data_objects,
            current_data_bytes: stats.current_data_bytes,
            retained_data_objects: stats.retained_data_objects,
            retained_data_bytes: stats.retained_data_bytes,
        })
    }

    pub fn into_stats(self, provider: String) -> TableMaintenanceStats {
        let length = self
            .format
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(FORMAT_NAME_CAPACITY);
        let format = (length != 0)
            .then(|| String::from_utf8_lossy(&self.format[..length]).into_owned());
        TableMaintenanceStats {
            provider,
            format,
            history_points: self.history_points,
            current_content_objects: self.current_content_objects,
            current_content_bytes: self.current_content_bytes,
            retained_content_objects: self.retained_content_objects,
            retained_content_bytes: self.retained_content_bytes,
            current_data_objects: self.current_data_objects,
            current_data_bytes: self.current_data_bytes,
            retained_data_objects: self.retained_data_objects,
            retained_data_bytes: self.retained_data_bytes,
        }
    }
}

pub type AccessMethodOidCallback = unsafe extern "C-unwind" fn() -> pg_sys::Oid;
/// Return CALLBACK_OK with an initialized report, or CALLBACK_FAILED with a
/// synchronous PostgreSQL-owned error payload. This callback must not report
/// to PostgreSQL or let a Rust panic cross the DSO boundary.
pub type ExecuteMaintenanceCallback = unsafe extern "C-unwind" fn(
    request: *const MaintenanceRequest,
    report: *mut MaintenanceReport,
    error: *mut CallbackErrorReport,
) -> u32;
/// Uses the same output/error contract as ExecuteMaintenanceCallback.
pub type InspectMaintenanceCallback = unsafe extern "C-unwind" fn(
    relation: pg_sys::Relation,
    stats: *mut MaintenanceStats,
    error: *mut CallbackErrorReport,
) -> u32;
pub type StageWorkerWakeupCallback = unsafe extern "C-unwind" fn(
    extension_name: *const c_char,
    worker_name: *const c_char,
) -> u32;

/// Exact-build table-provider descriptor published by an AM DSO.
///
/// This is the single runtime identity and operation record for one table AM.
/// Operation-specific callbacks remain grouped by name rather than being
/// exposed through parallel provider registries.
///
/// Fields are private: the constructor takes only `'static` borrows, and
/// PostgreSQL never unloads a library after `_PG_init`, so every pointer stays
/// valid for the backend lifetime in whichever DSO holds a copy.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableProvider {
    name: *const c_char,
    /// Stable catalog name of the table access method owned by this provider.
    ///
    /// Runtime registration uses this identity because access-method OIDs are
    /// database-local and may not be resolvable while extensions are preloaded.
    access_method_name: *const c_char,
    /// `None` for AMs that use their native TableAM insertion callbacks.
    copy: Option<&'static TableCopyCallbacks>,
    access_method_oid: AccessMethodOidCallback,
    execute_maintenance: ExecuteMaintenanceCallback,
    inspect_maintenance: InspectMaintenanceCallback,
}

impl TableProvider {
    #[must_use]
    pub const fn new(
        name: &'static CStr,
        access_method_name: &'static CStr,
        copy: Option<&'static TableCopyCallbacks>,
        access_method_oid: AccessMethodOidCallback,
        execute_maintenance: ExecuteMaintenanceCallback,
        inspect_maintenance: InspectMaintenanceCallback,
    ) -> Self {
        Self {
            name: name.as_ptr(),
            access_method_name: access_method_name.as_ptr(),
            copy,
            access_method_oid,
            execute_maintenance,
            inspect_maintenance,
        }
    }

    #[must_use]
    pub fn name(&self) -> &'static CStr {
        // SAFETY: `new` stores only `&'static CStr` pointers.
        unsafe { CStr::from_ptr(self.name) }
    }

    #[must_use]
    pub fn access_method_name(&self) -> &'static CStr {
        // SAFETY: `new` stores only `&'static CStr` pointers.
        unsafe { CStr::from_ptr(self.access_method_name) }
    }

    #[must_use]
    pub const fn copy(&self) -> Option<&'static TableCopyCallbacks> {
        self.copy
    }

    #[must_use]
    pub const fn access_method_oid(&self) -> AccessMethodOidCallback {
        self.access_method_oid
    }

    #[must_use]
    pub const fn execute_maintenance(&self) -> ExecuteMaintenanceCallback {
        self.execute_maintenance
    }

    #[must_use]
    pub const fn inspect_maintenance(&self) -> InspectMaintenanceCallback {
        self.inspect_maintenance
    }

    /// Whether `other` publishes the same identity and callbacks, as when one
    /// DSO registers the same provider again.
    #[must_use]
    pub fn is_same_provider(&self, other: &Self) -> bool {
        self.name() == other.name()
            && self.access_method_name() == other.access_method_name()
            && self.copy.map(ptr::from_ref) == other.copy.map(ptr::from_ref)
            && ptr::fn_addr_eq(self.access_method_oid, other.access_method_oid)
            && ptr::fn_addr_eq(self.execute_maintenance, other.execute_maintenance)
            && ptr::fn_addr_eq(self.inspect_maintenance, other.inspect_maintenance)
    }
}

/// Runtime-owned rendezvous function table.
///
/// The runtime publishes one static instance. Interpreting a slot as this type
/// relies on the exact-build install contract; clients first compare
/// `sdk_version` only to reject a runtime from a different release.
#[repr(C)]
#[derive(Debug)]
pub struct RuntimeApi {
    /// Must remain the first field: clients read it before trusting the rest
    /// of the layout.
    pub sdk_version: SdkVersion,
    pub register_provider:
        unsafe extern "C-unwind" fn(*const ProviderRegistration<'_>) -> u32,
    pub provider_for_am:
        unsafe extern "C-unwind" fn(pg_sys::Oid) -> *const TableProvider,
    pub customscan_mode: unsafe extern "C-unwind" fn() -> u32,
    pub maintenance_config:
        unsafe extern "C-unwind" fn(*mut RuntimeMaintenanceConfig),
    pub stage_worker_wakeup: StageWorkerWakeupCallback,
    pub load_tablespace_bindings: LoadTablespaceBindingsCallback,
}

unsafe extern "C" {
    fn find_rendezvous_variable(name: *const c_char) -> *mut *mut c_void;
}

/// Return the PostgreSQL rendezvous slot owned by the runtime extension.
///
/// # Safety
///
/// PostgreSQL must be initialized in the current backend process. The returned
/// slot is never null: `find_rendezvous_variable` enters the name with
/// `HASH_ENTER`, which raises an ERROR instead of returning null on allocation
/// failure. The slot is backend-lifetime memory. The rest of the table is
/// interpreted under the exact-build install contract, after the published
/// [`SdkVersion`] matches.
pub unsafe fn rendezvous_slot() -> *mut *mut c_void {
    unsafe { find_rendezvous_variable(RUNTIME_API_RENDEZVOUS.as_ptr()) }
}

static RUNTIME_API_CACHE: OnceLock<&'static RuntimeApi> = OnceLock::new();

#[derive(Debug, thiserror::Error)]
pub enum RuntimeApiError {
    #[error(
        "LagoDB runtime API is not published; load the lagodb_base extension before loading provider extensions"
    )]
    Unavailable,
    #[error(
        "LagoDB runtime version {runtime} does not match provider SDK version {provider}; install lagodb_base and every provider extension from the same release"
    )]
    Incompatible {
        runtime: SdkVersion,
        provider: SdkVersion,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RuntimeRegistrationError {
    #[error("runtime already has a different provider with this name")]
    DuplicateProviderName,
    #[error("runtime already has a table provider for this access method")]
    DuplicateAccessMethod,
    #[error("runtime already has a table-scan provider for this storage route")]
    DuplicateTableScanRoute,
    #[error("runtime rejected an invalid provider registration")]
    InvalidProviderRegistration,
    #[error(
        "provider registration is only allowed during runtime bootstrap; add its library to lagodb.provider_libraries and restart PostgreSQL"
    )]
    OutsideProviderBootstrap,
    #[error(
        "provider registered a library name different from the library being loaded"
    )]
    ProviderLibraryMismatch,
    #[error("runtime returned unknown {operation} registration status {status}")]
    UnknownStatus {
        operation: &'static str,
        status: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WorkerWakeupError {
    #[error("LagoDB runtime is not loaded through shared_preload_libraries")]
    RuntimeNotPreloaded,
    #[error("worker locator is not registered in the current database")]
    WorkerLocatorNotFound,
    #[error("invalid LagoDB worker wakeup request")]
    InvalidRequest,
    #[error("LagoDB runtime returned unknown worker wakeup status {0}")]
    UnknownStatus(u32),
}

#[derive(Clone, Copy, Debug)]
pub struct RuntimeClient {
    api: &'static RuntimeApi,
}

impl RuntimeClient {
    pub fn connect() -> Result<Self, RuntimeApiError> {
        if let Some(api) = RUNTIME_API_CACHE.get() {
            return Ok(Self { api: *api });
        }
        Self::connect_uncached()
    }

    fn connect_uncached() -> Result<Self, RuntimeApiError> {
        // SAFETY: PostgreSQL returns a non-null backend-lifetime slot.
        let published = unsafe { *rendezvous_slot() };
        // SAFETY: a published runtime table starts with `SdkVersion` in every
        // release, so the leading bytes are readable before the layout is
        // trusted.
        let Some(&runtime) = (unsafe { published.cast::<SdkVersion>().as_ref() })
        else {
            return Err(RuntimeApiError::Unavailable);
        };
        if runtime != SdkVersion::CURRENT {
            return Err(RuntimeApiError::Incompatible {
                runtime,
                provider: SdkVersion::CURRENT,
            });
        }
        // SAFETY: the exact-build install contract guarantees this runtime
        // was built from the same source tree, so the table has this layout.
        // The version match above only rules out a different release.
        let api = unsafe { &*published.cast::<RuntimeApi>() };
        let _ = RUNTIME_API_CACHE.set(api);
        Ok(Self { api })
    }

    /// Register one complete provider-owned hook and maintenance batch.
    ///
    /// # Safety
    ///
    /// Every descriptor reachable from `registration` must satisfy the
    /// module-level internal ABI contract. On success, callback functions and
    /// opaque contexts must remain valid until backend exit.
    pub unsafe fn register_provider(
        self,
        registration: &ProviderRegistration<'_>,
    ) -> Result<(), RuntimeRegistrationError> {
        let status = unsafe { (self.api.register_provider)(registration) };
        match status {
            REGISTER_OK => Ok(()),
            REGISTER_INVALID_DESCRIPTOR => {
                Err(RuntimeRegistrationError::InvalidProviderRegistration)
            }
            REGISTER_DUPLICATE_NAME => {
                Err(RuntimeRegistrationError::DuplicateProviderName)
            }
            REGISTER_DUPLICATE_ACCESS_METHOD => {
                Err(RuntimeRegistrationError::DuplicateAccessMethod)
            }
            REGISTER_DUPLICATE_TABLE_SCAN_ROUTE => {
                Err(RuntimeRegistrationError::DuplicateTableScanRoute)
            }
            REGISTER_OUTSIDE_PROVIDER_BOOTSTRAP => {
                Err(RuntimeRegistrationError::OutsideProviderBootstrap)
            }
            REGISTER_PROVIDER_LIBRARY_MISMATCH => {
                Err(RuntimeRegistrationError::ProviderLibraryMismatch)
            }
            status => Err(RuntimeRegistrationError::UnknownStatus {
                operation: "provider",
                status,
            }),
        }
    }

    pub fn provider_for_am(
        self,
        access_method_oid: pg_sys::Oid,
    ) -> Option<&'static TableProvider> {
        // SAFETY: the runtime callback has pg_guard and returns a
        // backend-lifetime descriptor. The closure contains only the FFI call,
        // so a PostgreSQL ERROR unwinds the caller's Rust resources normally.
        let provider = unsafe {
            pg_guard_ffi_boundary(|| (self.api.provider_for_am)(access_method_oid))
        };
        unsafe { provider.as_ref() }
    }

    pub fn customscan_mode(self) -> u32 {
        unsafe { (self.api.customscan_mode)() }
    }

    pub fn maintenance_config(self) -> RuntimeMaintenanceConfig {
        let mut config = MaybeUninit::uninit();
        unsafe {
            (self.api.maintenance_config)(config.as_mut_ptr());
            config.assume_init()
        }
    }

    pub fn stage_worker_wakeup(
        self,
        extension_name: &CStr,
        worker_name: &CStr,
    ) -> Result<(), WorkerWakeupError> {
        // SAFETY: worker notification runs on a PostgreSQL backend or worker
        // main thread, and both strings outlive this synchronous callback. The
        // barrier lets a catalog ERROR unwind caller-owned transaction state.
        let status = unsafe {
            pg_guard_ffi_boundary(|| {
                (self.api.stage_worker_wakeup)(
                    extension_name.as_ptr(),
                    worker_name.as_ptr(),
                )
            })
        };
        match status {
            STAGE_WORKER_WAKEUP_OK => Ok(()),
            STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND => {
                Err(WorkerWakeupError::WorkerLocatorNotFound)
            }
            STAGE_WORKER_WAKEUP_INVALID_REQUEST => {
                Err(WorkerWakeupError::InvalidRequest)
            }
            STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED => {
                Err(WorkerWakeupError::RuntimeNotPreloaded)
            }
            status => Err(WorkerWakeupError::UnknownStatus(status)),
        }
    }
}
