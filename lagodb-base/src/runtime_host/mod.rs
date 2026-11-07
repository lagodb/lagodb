//! Host implementation and publisher for `lagodb_core::runtime_api`.
//!
//! Registration transaction ownership lives in a focused submodule. Capability
//! registries and their ABI callbacks remain with the subsystems that own them;
//! this module assembles the exact-build runtime function table and publishes
//! it through PostgreSQL's rendezvous variable. It also owns
//! `lagodb.customscan_mode`, which no base subsystem reads: only providers do,
//! through the table.

mod customscan_mode;
mod registration;

use std::ffi::c_void;

use lagodb_core::runtime_api::{RuntimeApi, SdkVersion, rendezvous_slot};

use crate::storage::volume_config::load_tablespace_bindings;
use crate::worker::stage_named_worker_wakeup;
use crate::{maintenance, table_provider_registry};
use customscan_mode::customscan_mode;

static RUNTIME_API: RuntimeApi = RuntimeApi {
    sdk_version: SdkVersion::CURRENT,
    register_provider: registration::register_provider,
    provider_for_am: table_provider_registry::provider_for_am,
    customscan_mode,
    maintenance_config: maintenance::maintenance_config,
    stage_worker_wakeup: stage_named_worker_wakeup,
    load_tablespace_bindings,
};

/// Define the runtime-owned GUCs on every load of the runtime library.
pub(crate) fn define_gucs() {
    customscan_mode::define();
}

/// Publish the runtime ABI table through the rendezvous variable on every load.
pub(crate) fn publish() {
    // SAFETY: PostgreSQL owns the rendezvous slot and returns its non-null
    // backend-local address for the static name defined by the core runtime ABI.
    let slot = unsafe { rendezvous_slot() };
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
}
