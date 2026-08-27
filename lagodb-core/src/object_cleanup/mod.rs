//! Format-neutral durable physical object-cleanup framework.

mod actor;
mod error;
mod item;
mod object_tree;
mod repository;
mod runner;
mod target;
mod worker;

pub use error::{ObjectCleanupCatalogOperation, ObjectCleanupError};
pub use item::{ObjectCleanupContext, ObjectCleanupItemId, ObjectCleanupItemRef};
pub use object_tree::{ObjectTreeObserver, ObjectTreeStats};
pub use repository::ObjectCleanupQueue;
pub use target::{ObjectTarget, ObjectTreeTarget};
pub use worker::run_object_cleanup_worker;

use crate::maintenance_config::MaintenanceSettings;

/// Maximum exact-object rows one producer insertion call may publish. The
/// logical cleanup set may span any number of calls in the same transaction.
pub fn object_cleanup_batch_items() -> usize {
    MaintenanceSettings::load().batch_items()
}
