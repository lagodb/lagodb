//! Tunables split by **connection/backpressure** ([`StorageServerConfig`]) vs **cache geometry**
//! ([`StorageServiceConfig`]) vs **runtime-reloadable** ([`StorageRuntimeConfig`]).
//!
//! Cache runtime parameters (touch granularity, cleanup policy) live exclusively in
//! [`StorageRuntimeConfig`] and are hot-reloaded via [`StorageRuntime`].
//!
//! Submodules keep each config type with its own builder methods and defaults; this module
//! re-exports everything so callers can continue to `use crate::config::...` without caring about
//! the internal file layout.

mod cleanup;
pub(crate) mod runtime;
mod server;
pub(crate) mod service;

pub use crate::cache::{
    DEFAULT_CACHE_CLEANUP_BATCH_BYTES, DEFAULT_CACHE_CLEANUP_BATCH_ITEMS,
    DEFAULT_CACHE_CLEANUP_INTERVAL, DEFAULT_CACHE_CLEANUP_START_PERCENT,
    DEFAULT_CACHE_CLEANUP_TARGET_PERCENT,
};
pub use cleanup::CacheCleanupConfig;
pub(crate) use runtime::{CacheCleanupSnapshot, CacheRuntimeHandle};
pub use runtime::{
    CacheRuntimeConfig, RuntimeApplyReport, StorageRuntime, StorageRuntimeConfig,
};
pub use server::{
    DEFAULT_CONNECTION_DRAIN_TIMEOUT, DEFAULT_MAX_CONNECTIONS,
    DEFAULT_MAX_IN_FLIGHT_REQUESTS, DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION,
    DEFAULT_MAX_PENDING_RESPONSE_BYTES, DEFAULT_MAX_PENDING_RESPONSES,
    DEFAULT_RESPONSE_WRITE_TIMEOUT, StorageServerConfig,
};
pub use service::{
    DEFAULT_CACHE_TOUCH_GRANULARITY, DEFAULT_MAX_READ_SIZE, StorageServiceConfig,
};
