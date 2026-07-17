//! Format-neutral table-maintenance provider framework.
//!
//! This module is deliberately separate from [`crate::maintenance`], which is
//! the durable physical object queue.  Providers use this SPI to implement the
//! logical work behind PostgreSQL `VACUUM`; a provider may subsequently publish
//! exact-object work to the physical queue.

pub mod abi;
mod error;
#[cfg(feature = "pg17")]
mod full_router;
mod provider;
mod types;

#[cfg(feature = "pg17")]
pub(crate) use full_router::try_route_vacuum_full;

pub use error::TableMaintenanceError;
pub use provider::{
    LakebaseTableMaintenanceProvider, TableMaintenanceRequest,
    TableMaintenanceRouter, register_provider,
};

#[cfg(feature = "pg17")]
pub fn install_runtime_router() {
    crate::hooks::utility_hook::install_table_maintenance_router();
}

#[cfg(not(feature = "pg17"))]
pub fn install_runtime_router() {}
pub use types::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMetric,
    TableMaintenanceMode, TableMaintenanceOptions, TableMaintenanceReport,
    TableMaintenanceStats,
};
