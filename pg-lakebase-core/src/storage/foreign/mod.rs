//! Backend-local foreign-table access to the shared object-storage service.
//!
//! The module owns the PostgreSQL catalog boundary, the per-backend cache, and
//! catalog invalidation lifecycle. The storage service sees one configured context per
//! socket and has no PostgreSQL catalog concepts.

mod cache;
mod catalog;
mod handle;
mod identity;
mod manager;

pub use catalog::{ForeignOption, ForeignOptionView, ForeignStoreOptions};
pub use handle::{ForeignStoreFile, ForeignStoreHandle};
pub use identity::ForeignStoreIdentity;
pub use manager::{
    ForeignStoreAcquireError, ForeignStoreConfigProvider, ForeignStoreManager,
};
