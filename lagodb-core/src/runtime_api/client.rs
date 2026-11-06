//! Runtime rendezvous table and the provider-side client that calls it.

use std::ffi::{CStr, c_char, c_void};
use std::mem::MaybeUninit;
use std::sync::OnceLock;

use pgrx::pg_sys::{self, ffi::pg_guard_ffi_boundary};

use super::{
    LoadTablespaceBindingsCallback, ProviderRegistration,
    REGISTER_DUPLICATE_ACCESS_METHOD, REGISTER_DUPLICATE_NAME,
    REGISTER_DUPLICATE_TABLE_SCAN_ROUTE, REGISTER_INVALID_DESCRIPTOR, REGISTER_OK,
    REGISTER_OUTSIDE_PROVIDER_BOOTSTRAP, REGISTER_PROVIDER_LIBRARY_MISMATCH,
    RuntimeMaintenanceConfig, RuntimeRegistrationError, SdkVersion, TableProvider,
    TablespaceBindingsOutput,
};

pub const RUNTIME_API_RENDEZVOUS: &CStr = c"lagodb.runtime_api";

pub const STAGE_WORKER_WAKEUP_OK: u32 = 0;
pub const STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND: u32 = 1;
pub const STAGE_WORKER_WAKEUP_INVALID_REQUEST: u32 = 2;
pub const STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED: u32 = 3;

pub type StageWorkerWakeupCallback = unsafe extern "C-unwind" fn(
    extension_name: *const c_char,
    worker_name: *const c_char,
) -> u32;

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
    unsafe { pg_sys::find_rendezvous_variable(RUNTIME_API_RENDEZVOUS.as_ptr()) }
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

    pub(crate) fn load_tablespace_bindings(
        self,
        output: &mut TablespaceBindingsOutput,
    ) -> u32 {
        // SAFETY: synchronous call with a live output reference.
        unsafe { (self.api.load_tablespace_bindings)(output) }
    }
}
