//! Runtime maintenance configuration and SQL boundary.

mod route_policy;
mod sql_api;

pub(crate) use route_policy::MaintenanceRoutePolicy;

use lagodb_core::runtime_api::RuntimeMaintenanceConfig;
use pgrx::pg_guard;

use crate::gucs;

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn maintenance_config(
    config: *mut RuntimeMaintenanceConfig,
) {
    // SAFETY: `as_mut` validates the permitted null input before the output is
    // initialized; the runtime ABI gives exclusive access for this call.
    let Some(config) = (unsafe { config.as_mut() }) else {
        panic!("runtime maintenance config output pointer is null");
    };
    *config = gucs::maintenance_config();
}
