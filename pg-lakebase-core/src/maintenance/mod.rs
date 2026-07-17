//! Format-neutral durable physical maintenance framework.

mod actor;
mod error;
mod gucs;
mod item;
mod object_tree;
mod repository;
mod runner;
mod target;
mod worker;

pub use error::{MaintenanceCatalogOperation, MaintenanceError};
pub use item::{MaintenanceContext, MaintenanceItemId, MaintenanceItemRef};
pub use object_tree::{ObjectTreeObserver, ObjectTreeStats};
pub use repository::MaintenanceQueue;
pub use target::{ObjectTarget, ObjectTreeTarget};
pub use worker::run_database_worker;

pub(crate) use gucs::table_maintenance_budget;

/// Maximum exact-object rows one producer insertion call may publish. The
/// logical cleanup set may span any number of calls in the same transaction.
pub fn producer_batch_items() -> usize {
    gucs::producer_batch_items()
}
