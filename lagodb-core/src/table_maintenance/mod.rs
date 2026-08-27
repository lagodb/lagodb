//! Format-neutral table-maintenance provider framework.
//!
//! This module is deliberately separate from [`crate::object_cleanup`], which is
//! the durable physical object queue.  Providers use this SPI to implement the
//! logical work behind PostgreSQL `VACUUM`; a provider may subsequently publish
//! exact-object work to the physical queue.

mod error;
mod provider;
mod types;

pub use error::TableMaintenanceError;
pub use provider::{
    LagodbTableMaintenanceProvider, TableMaintenanceRequest, TableMaintenanceRouter,
    register_provider,
};

pub use types::{
    TableMaintenanceBudget, TableMaintenanceCommandTime, TableMaintenanceMetric,
    TableMaintenanceMode, TableMaintenanceOptions, TableMaintenanceReport,
    TableMaintenanceStats,
};
