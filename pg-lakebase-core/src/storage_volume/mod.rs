//! Stable storage-volume identity shared by catalog and data-plane boundaries.

mod id;
mod route;

pub use id::{StorageVolumeId, StorageVolumeIdError};
pub use route::{StorageVolumeRoute, StorageVolumeRouteError};
