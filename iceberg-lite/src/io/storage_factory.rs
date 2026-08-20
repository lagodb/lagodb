//! Factory boundary for storage implementations owned by embedding applications.

use std::fmt::Debug;
use std::sync::Arc;

use super::{Storage, StorageConfig};
use crate::Result;

/// Constructs a storage implementation from catalog-supplied configuration.
pub trait StorageFactory: Debug + Send + Sync {
    /// Builds one storage instance, consuming all configuration and credentials.
    fn build(&self, config: StorageConfig) -> Result<Arc<dyn Storage>>;
}
