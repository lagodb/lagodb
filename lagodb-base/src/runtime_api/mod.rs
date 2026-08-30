//! Publisher of the unified runtime API.
//!
//! Registration transaction ownership and maintenance-provider storage live in
//! focused submodules. This module only assembles the exact-build runtime
//! function table and publishes it through PostgreSQL's rendezvous variable.

mod maintenance;
mod registration;
mod storage_volume;

use std::ffi::{CStr, c_char, c_void};

use lagodb_core::runtime_api::{
    RuntimeApi, STAGE_WORKER_WAKEUP_INVALID_REQUEST,
    STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND, STAGE_WORKER_WAKEUP_OK,
    STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED, rendezvous_slot,
};
use pgrx::{pg_guard, pg_sys};

use crate::{gucs, lifecycle, object_access, process_utility, registry, worker};
use storage_volume::resolve_storage_volume_route;

#[pg_guard]
unsafe extern "C-unwind" fn customscan_mode() -> u32 {
    gucs::customscan_mode_code()
}

#[pg_guard]
unsafe extern "C-unwind" fn stage_worker_wakeup(
    extension_name: *const c_char,
    worker_name: *const c_char,
) -> u32 {
    if worker::ensure_preloaded().is_err() {
        return STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED;
    }
    if extension_name.is_null() || worker_name.is_null() {
        return STAGE_WORKER_WAKEUP_INVALID_REQUEST;
    }
    // SAFETY: the runtime ABI requires both non-null inputs to point to live,
    // NUL-terminated strings for this synchronous call.
    let extension_name = unsafe { CStr::from_ptr(extension_name) };
    // SAFETY: the same runtime ABI string contract applies to `worker_name`.
    let worker_name = unsafe { CStr::from_ptr(worker_name) };
    if extension_name.is_empty()
        || worker_name.is_empty()
        || worker_name.to_bytes().len() > worker::MAX_WORKER_NAME_BYTES
    {
        return STAGE_WORKER_WAKEUP_INVALID_REQUEST;
    }
    let Ok(worker_name) = worker_name.to_str() else {
        return STAGE_WORKER_WAKEUP_INVALID_REQUEST;
    };
    let Some(worker_id) = registry::resolve_worker_id(extension_name, worker_name)
        .unwrap_or_else(|error| error.report())
    else {
        return STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND;
    };
    lifecycle::request_wakeup(worker_id);
    STAGE_WORKER_WAKEUP_OK
}

static RUNTIME_API: RuntimeApi = RuntimeApi {
    struct_size: std::mem::size_of::<RuntimeApi>() as u32,
    register_provider: registration::register_provider,
    has_providers: maintenance::has_providers,
    provider_for_am: maintenance::provider_for_am,
    customscan_mode,
    maintenance_config: maintenance::maintenance_config,
    stage_worker_wakeup,
    resolve_storage_volume_route,
};

pub(crate) fn init() {
    // SAFETY: PostgreSQL owns the rendezvous slot and returns its backend-local
    // address for the static name defined by the core runtime ABI.
    let slot = unsafe { rendezvous_slot() };
    assert!(
        !slot.is_null(),
        "PostgreSQL returned a null rendezvous slot"
    );
    // SAFETY: `slot` was checked non-null and points to PostgreSQL's
    // backend-lifetime rendezvous value.
    let published = unsafe { *slot };
    if !published.is_null()
        && published
            != (&RUNTIME_API as *const RuntimeApi)
                .cast_mut()
                .cast::<c_void>()
    {
        panic!("a different LagoDB runtime API is already published");
    }
    // SAFETY: the published pointer targets a process-static function table;
    // PostgreSQL retains the pointer only for this backend's lifetime.
    unsafe {
        *slot = (&RUNTIME_API as *const RuntimeApi)
            .cast_mut()
            .cast::<c_void>();
    }
    // SAFETY: PostgreSQL exposes binary-upgrade state as a backend-global flag.
    if unsafe { pg_sys::IsBinaryUpgrade } {
        return;
    }
    object_access::init();
    process_utility::init();
}
