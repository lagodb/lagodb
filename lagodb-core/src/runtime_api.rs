//! Exact-build PostgreSQL rendezvous ABI for runtime-owned backend services.
//!
//! # Trust model
//!
//! This is an internal ABI between `lagodb-base` and provider DSOs built
//! against the same `lagodb-core` SDK release. It is not a general C
//! plugin interface and does not attempt to make arbitrary addresses or
//! malformed foreign allocations safe to inspect.
//!
//! Raw ABI callers must provide complete, initialized, correctly aligned
//! values of the declared `#[repr(C)]` types. Pointer/count pairs must describe
//! live arrays of the exact element type, callback and C-string pointers must
//! be valid, and registered callbacks and contexts must remain live for the
//! backend lifetime. Size fields reject layouts built from a different SDK
//! after those memory-safety preconditions hold; they do not validate an
//! otherwise invalid pointer. Every runtime API type is an exact-build
//! contract: the runtime and every consuming extension must be rebuilt
//! together after an ABI change.

use std::ffi::{CStr, c_char, c_void};
use std::mem::MaybeUninit;
use std::sync::OnceLock;

use pgrx::pg_sys;

use crate::table_maintenance::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMode,
    TableMaintenanceOptions, TableMaintenanceReport, TableMaintenanceStats,
};
mod planning;
mod registration;
mod storage_volume;

pub use planning::*;
pub use registration::*;
pub use storage_volume::{
    ResolveStorageVolumeRouteCallback, StorageVolumeRouteLookupError,
    StorageVolumeRouteOutput, VOLUME_ROUTE_ERROR, VOLUME_ROUTE_INVALID_REQUEST,
    VOLUME_ROUTE_NOT_FOUND, VOLUME_ROUTE_OK,
};

pub const RUNTIME_API_RENDEZVOUS: &CStr = c"lagodb.runtime_api";
// The provider descriptor includes capability flags so the router can reject
// unsupported compound operations before any provider performs irreversible
// work.
pub const FORMAT_NAME_CAPACITY: usize = 32;

pub const STAGE_WORKER_WAKEUP_OK: u32 = 0;
pub const STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND: u32 = 1;
pub const STAGE_WORKER_WAKEUP_INVALID_REQUEST: u32 = 2;
pub const STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED: u32 = 3;

pub const PROVIDER_CAPABILITY_ANALYZE: u32 = 1 << 0;
pub const PROVIDER_CAPABILITIES_KNOWN: u32 = PROVIDER_CAPABILITY_ANALYZE;

pub const REGISTER_OK: u32 = 0;
pub const REGISTER_INVALID_DESCRIPTOR: u32 = 1;
pub const REGISTER_DUPLICATE_NAME: u32 = 2;
pub const REGISTER_DUPLICATE_ACCESS_METHOD: u32 = 3;
pub const REGISTER_OUTSIDE_PROVIDER_BOOTSTRAP: u32 = 4;
pub const REGISTER_PROVIDER_LIBRARY_MISMATCH: u32 = 5;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct AbiHeader {
    pub struct_size: u32,
}

pub const UTILITY_ROUTE_PASS_THROUGH: u8 = 0;
pub const UTILITY_ROUTE_CONSUMED: u8 = 1;

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
const OPTION_ANALYZE: u32 = 1 << 1;
const OPTION_SKIP_LOCKED: u32 = 1 << 2;
const OPTION_PROCESS_MAIN: u32 = 1 << 3;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MaintenanceRequest {
    pub relation: pg_sys::Relation,
    pub mode: u32,
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
        let mut option_flags = 0;
        option_flags |= u32::from(options.verbose) * OPTION_VERBOSE;
        option_flags |= u32::from(options.analyze) * OPTION_ANALYZE;
        option_flags |= u32::from(options.skip_locked) * OPTION_SKIP_LOCKED;
        option_flags |= u32::from(options.process_main) * OPTION_PROCESS_MAIN;
        Self {
            relation,
            mode: match mode {
                TableMaintenanceMode::Routine => 0,
                TableMaintenanceMode::Full => 1,
            },
            option_flags,
            max_input_objects: budget.max_input_objects,
            max_input_bytes: budget.max_input_bytes,
            max_group_objects: budget.max_group_objects,
            max_group_bytes: budget.max_group_bytes,
            command_time_ms: command_time.unix_epoch_ms(),
        }
    }

    pub fn mode(self) -> Option<TableMaintenanceMode> {
        match self.mode {
            0 => Some(TableMaintenanceMode::Routine),
            1 => Some(TableMaintenanceMode::Full),
            _ => None,
        }
    }

    pub fn options(self) -> TableMaintenanceOptions {
        TableMaintenanceOptions {
            verbose: self.option_flags & OPTION_VERBOSE != 0,
            analyze: self.option_flags & OPTION_ANALYZE != 0,
            skip_locked: self.option_flags & OPTION_SKIP_LOCKED != 0,
            process_main: self.option_flags & OPTION_PROCESS_MAIN != 0,
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
pub type ExecuteCallback = unsafe extern "C-unwind" fn(
    request: *const MaintenanceRequest,
    report: *mut MaintenanceReport,
);
pub type InspectCallback = unsafe extern "C-unwind" fn(
    relation: pg_sys::Relation,
    stats: *mut MaintenanceStats,
);
pub type StageWorkerWakeupCallback = unsafe extern "C-unwind" fn(
    extension_name: *const c_char,
    worker_name: *const c_char,
) -> u32;

/// Exact-build maintenance-provider descriptor published by an AM DSO.
///
/// Values are constructed by the core provider adapter. Callback and string
/// pointers are required to be valid under the module-level internal ABI
/// contract and remain live for the PostgreSQL backend lifetime.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MaintenanceProvider {
    pub struct_size: u32,
    pub name: *const c_char,
    /// Stable catalog name of the table access method owned by this provider.
    ///
    /// Runtime registration uses this identity because access-method OIDs are
    /// database-local and may not be resolvable while extensions are preloaded.
    pub access_method_name: *const c_char,
    pub capability_flags: u32,
    pub access_method_oid: AccessMethodOidCallback,
    pub execute: ExecuteCallback,
    pub inspect: InspectCallback,
}

/// Runtime-owned rendezvous function table.
///
/// The runtime publishes one static instance. Provider clients may only interpret a
/// slot as this type after matching the rendezvous name and exact size under
/// the module-level internal ABI contract.
#[repr(C)]
#[derive(Debug)]
pub struct RuntimeApi {
    pub struct_size: u32,
    pub register_provider:
        unsafe extern "C-unwind" fn(*const ProviderRegistration) -> u32,
    pub has_providers: unsafe extern "C-unwind" fn() -> u8,
    pub provider_for_am:
        unsafe extern "C-unwind" fn(pg_sys::Oid) -> *const MaintenanceProvider,
    pub customscan_mode: unsafe extern "C-unwind" fn() -> u32,
    pub maintenance_config:
        unsafe extern "C-unwind" fn(*mut RuntimeMaintenanceConfig),
    pub stage_worker_wakeup: StageWorkerWakeupCallback,
    pub resolve_storage_volume_route: ResolveStorageVolumeRouteCallback,
}

unsafe extern "C" {
    fn find_rendezvous_variable(name: *const c_char) -> *mut *mut c_void;
}

/// Return the PostgreSQL rendezvous slot owned by the runtime extension.
///
/// # Safety
///
/// PostgreSQL must be initialized in the current backend process. The returned
/// slot is backend-lifetime memory; callers must validate any published pointer
/// and exact ABI size before dereferencing it.
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
        "incompatible LagoDB runtime API size {actual_size}; expected exactly {expected_size}"
    )]
    Incompatible {
        actual_size: u32,
        expected_size: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RuntimeRegistrationError {
    #[error("runtime already has a different provider with this name")]
    DuplicateProviderName,
    #[error("runtime already has a maintenance provider for this access method")]
    DuplicateAccessMethod,
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
        let slot = unsafe { rendezvous_slot() };
        if slot.is_null() {
            return Err(RuntimeApiError::Unavailable);
        }
        let published = unsafe { *slot };
        let Some(header) = (unsafe { published.cast::<AbiHeader>().as_ref() }) else {
            return Err(RuntimeApiError::Unavailable);
        };
        let expected_size = u32::try_from(std::mem::size_of::<RuntimeApi>())
            .expect("runtime API size exceeds u32");
        if header.struct_size != expected_size {
            return Err(RuntimeApiError::Incompatible {
                actual_size: header.struct_size,
                expected_size,
            });
        }
        let api = unsafe { &*published.cast::<RuntimeApi>() };
        let _ = RUNTIME_API_CACHE.set(api);
        Ok(Self { api })
    }

    /// Register one complete provider-owned hook and maintenance batch.
    ///
    /// # Safety
    ///
    /// `registration` and every non-null pointer reachable from it must satisfy
    /// the module-level internal ABI contract. In particular, descriptor
    /// pointer/count pairs must identify live, correctly aligned arrays of the
    /// exact current descriptor types. On success, callback functions and
    /// opaque contexts must remain valid until backend exit.
    pub unsafe fn register_provider(
        self,
        registration: &ProviderRegistration,
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

    pub fn has_providers(self) -> bool {
        unsafe { (self.api.has_providers)() != 0 }
    }

    pub fn provider_for_am(
        self,
        access_method_oid: pg_sys::Oid,
    ) -> Option<&'static MaintenanceProvider> {
        let provider = unsafe { (self.api.provider_for_am)(access_method_oid) };
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
        let status = unsafe {
            (self.api.stage_worker_wakeup)(
                extension_name.as_ptr(),
                worker_name.as_ptr(),
            )
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

/// Borrow the provider name carried by a validated descriptor.
///
/// # Safety
///
/// A non-null `provider.name` must point to a live NUL-terminated C string.
pub unsafe fn provider_name(provider: &MaintenanceProvider) -> Option<&CStr> {
    (!provider.name.is_null()).then(|| unsafe { CStr::from_ptr(provider.name) })
}

/// Borrow the access-method name carried by a validated descriptor.
///
/// # Safety
///
/// A non-null `provider.access_method_name` must point to a live
/// NUL-terminated C string.
pub unsafe fn provider_access_method_name(
    provider: &MaintenanceProvider,
) -> Option<&CStr> {
    (!provider.access_method_name.is_null())
        .then(|| unsafe { CStr::from_ptr(provider.access_method_name) })
}
