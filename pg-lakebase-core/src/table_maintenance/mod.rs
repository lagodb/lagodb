//! Format-neutral table-maintenance provider framework.
//!
//! This module is deliberately separate from [`crate::maintenance`], which is
//! the durable physical object queue.  Providers use this SPI to implement the
//! logical work behind PostgreSQL `VACUUM`; a provider may subsequently publish
//! exact-object work to the physical queue.

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
pub use types::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMetric,
    TableMaintenanceMode, TableMaintenanceOptions, TableMaintenanceReport,
    TableMaintenanceStats,
};
