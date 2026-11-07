//! Runtime maintenance configuration and SQL boundary.

mod gucs;
mod sql_api;

use lagodb_core::runtime_api::RuntimeMaintenanceConfig;
use pgrx::pg_guard;

/// Define the maintenance GUCs on every load of the runtime library.
pub(crate) fn define_gucs() {
    gucs::define();
}

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn maintenance_config(
    config: *mut RuntimeMaintenanceConfig,
) {
    // SAFETY: the exact-build ABI requires a live call-scoped output pointer
    // with exclusive access for this call.
    unsafe { config.write(gucs::runtime_config()) };
}
