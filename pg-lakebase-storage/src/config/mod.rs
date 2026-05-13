//! Tunables split by **connection/backpressure** ([`StorageServerConfig`]) vs **cache semantics**
//! ([`StorageServiceConfig`]).
//!
//! Submodules keep each config type with its own builder methods and defaults; this module
//! re-exports everything so callers can continue to `use crate::config::...` without caring about
//! the internal file layout.

mod cleanup;
mod server;
mod service;

pub use cleanup::{
    CacheCleanupConfig, DEFAULT_CACHE_CLEANUP_BATCH_BYTES, DEFAULT_CACHE_CLEANUP_BATCH_ITEMS,
    DEFAULT_CACHE_CLEANUP_START_PERCENT, DEFAULT_CACHE_CLEANUP_TARGET_PERCENT,
};
pub use server::{
    StorageServerConfig, DEFAULT_CONNECTION_DRAIN_TIMEOUT, DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_IN_FLIGHT_REQUESTS,
    DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION, DEFAULT_MAX_PENDING_RESPONSES, DEFAULT_MAX_PENDING_RESPONSE_BYTES,
    DEFAULT_RESPONSE_WRITE_TIMEOUT,
};
pub use service::{StorageServiceConfig, DEFAULT_CACHE_TOUCH_GRANULARITY, DEFAULT_MAX_READ_SIZE};
