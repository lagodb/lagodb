//! PostgreSQL maintenance execution mechanisms, independent of runtime policy.
//!
//! The runtime selects commands and supplies locked-relation callbacks. Core
//! owns option parsing, relation-plan memory, PostgreSQL executors, and transaction /
//! snapshot transitions. Global hook ownership and parent delegation stay in
//! lagodb-base so provider DSOs never install another command lifecycle.

mod bridge;
mod executor;
mod plan;

pub use executor::{MaintenanceExecutor, VacuumCallbacks};
pub use plan::MaintenancePlan;

use std::ffi::{c_char, c_void};

use pgrx::pg_sys;

/// Classify a relation held open under the PostgreSQL execution lock.
pub type MaintenanceRouteCallback =
    unsafe extern "C-unwind" fn(pg_sys::Relation, *mut pg_sys::VacuumParams) -> bool;

/// Execute provider storage maintenance with the command's borrowed context.
pub type MaintenanceProviderCallback = unsafe extern "C-unwind" fn(
    pg_sys::Relation,
    *mut pg_sys::VacuumParams,
    *mut c_void,
);

/// Inspect catalog identity for admission without acquiring target locks.
pub type MaintenanceProbeCallback =
    unsafe extern "C-unwind" fn(pg_sys::Oid, c_char, pg_sys::bits32) -> bool;

/// Validated PostgreSQL options shared by command admission and execution.
pub struct MaintenanceCommand {
    pub(super) params: pg_sys::VacuumParams,
    pub(super) ring_size_kb: i32,
}

impl MaintenanceCommand {
    /// PostgreSQL VACOPT flags produced by command preparation.
    pub const fn options(&self) -> pg_sys::bits32 {
        self.params.options
    }
}
