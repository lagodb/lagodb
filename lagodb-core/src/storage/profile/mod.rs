//! Shared PostgreSQL storage-profile configuration and scoped routing.

mod catalog;
mod config;
mod error;
mod resolver;
mod uri;
mod validation;

pub use catalog::{ScopedStorageProfile, StorageProfiles};
pub use config::{StorageProfileConfig, StorageServerOptions};
pub use error::StorageProfileError;
pub use resolver::{
    ResolvedStorageServer, StorageServerCatalog, StorageServerPolicy,
};
pub use uri::{
    ObjectScheme, ObjectUri, ObjectUriPrefix, StorageProvider, StorageScope,
};
