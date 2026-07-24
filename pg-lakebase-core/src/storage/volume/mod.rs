//! Storage-volume identity and object-routing types.

mod id;
mod route;

pub use id::{StorageVolumeId, StorageVolumeIdError};
pub use route::{StorageVolumeRoute, StorageVolumeRouteError};
