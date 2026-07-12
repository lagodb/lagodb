//! Format-neutral durable physical maintenance framework.

mod actor;
mod error;
mod gucs;
mod item;
mod repository;
mod runner;
mod target;
mod worker;

pub use error::{MaintenanceCatalogOperation, MaintenanceError};
pub use item::{MaintenanceContext, MaintenanceItemId, MaintenanceItemRef};
pub use repository::MaintenanceQueue;
pub use target::{ObjectTarget, ObjectTreeTarget};
pub use worker::init_worker_host;
