//! PostgreSQL maintenance execution mechanisms, independent of runtime policy.
//!
//! The runtime selects commands and supplies the provider callback. Core owns
//! option parsing, relation-plan memory, the provider VACUUM FULL executor, and
//! transaction / snapshot transitions. Global hook ownership and parent
//! delegation stay in lagodb-base so provider DSOs never install another
//! command lifecycle.

mod bridge;
mod executor;
mod plan;

pub use executor::MaintenanceExecutor;
pub use plan::MaintenancePlan;

use std::ffi::c_void;

use pgrx::pg_sys;

/// Execute provider storage maintenance with the command's borrowed context.
pub type MaintenanceProviderCallback = unsafe extern "C-unwind" fn(
    pg_sys::Relation,
    *mut pg_sys::VacuumParams,
    *mut c_void,
);

/// Report whether a table access method belongs to a registered provider.
pub type MaintenanceProbeCallback = unsafe extern "C-unwind" fn(pg_sys::Oid) -> bool;

/// Validated PostgreSQL options shared by command admission and execution.
pub struct MaintenanceCommand {
    pub(super) params: pg_sys::VacuumParams,
}

impl MaintenanceCommand {
    /// PostgreSQL VACOPT flags produced by command preparation.
    pub const fn options(&self) -> pg_sys::bits32 {
        self.params.options
    }
}
