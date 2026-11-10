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
    /// Whether VACUUM processes the main relation. PostgreSQL 16 added
    /// `PROCESS_MAIN`; before that VACUUM always does.
    pub const fn processes_main(&self) -> bool {
        #[cfg(feature = "pg15")]
        let processes = true;
        #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
        let processes = self.params.options & pg_sys::VACOPT_PROCESS_MAIN != 0;
        processes
    }

    /// Whether VACUUM finishes by updating `datfrozenxid`. PostgreSQL 16 added
    /// `SKIP_DATABASE_STATS`; before that a user VACUUM always does.
    pub(super) const fn updates_database_stats(&self) -> bool {
        #[cfg(feature = "pg15")]
        let updates = true;
        #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
        let updates = self.params.options & pg_sys::VACOPT_SKIP_DATABASE_STATS == 0;
        updates
    }

    /// Whether the command also runs ANALYZE (`VACUUM ANALYZE`).
    pub(super) const fn analyzes(&self) -> bool {
        self.params.options & pg_sys::VACOPT_ANALYZE != 0
    }
}
