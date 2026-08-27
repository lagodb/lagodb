//! Snapshot of GUC values into plain Rust structs that can be sent to Tokio tasks.
//!
//! Configuration is split into two layers:
//!
//! - [`StorageWorkerStartupConfig`]: resources that cannot be safely changed at runtime
//!   (socket path, cache directory, Tokio thread pool size, etc.).
//! - [`StorageWorkerRuntimeConfig`]: parameters that the supervisor can hot-reload after
//!   a SIGHUP signal without restarting the background worker.
//!
//! [`StorageWorkerConfig::from_gucs`] must be called from the bgworker main thread
//! (the only Postgres-facing thread).  After construction the structs contain no
//! references to Postgres internals and are safe to move across thread boundaries.

use std::path::PathBuf;
use std::time::Duration;

use lagodb_core::storage::service::StorageEndpoint;
use lagodb_storage::{
    CacheCleanupConfig, CacheRuntimeConfig, StorageRuntimeConfig,
    StorageServerConfig, StorageServiceConfig,
};

use super::gucs;

/// Combined startup + runtime configuration, built once at worker start.
pub struct StorageWorkerConfig {
    pub startup: StorageWorkerStartupConfig,
    pub runtime: StorageWorkerRuntimeConfig,
}

/// Resources that require a PostgreSQL restart to change.
pub struct StorageWorkerStartupConfig {
    pub socket_path: PathBuf,
    pub cache_dir: PathBuf,
    pub server_config: StorageServerConfig,
    pub service_config: StorageServiceConfig,
    pub worker_threads: usize,
    pub log_channel_capacity: usize,
}

/// Parameters the supervisor can reload via SIGHUP without restarting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageWorkerRuntimeConfig {
    pub shutdown_timeout: Duration,
    /// Runtime configuration for the storage server (cache parameters).
    /// Pushed to `StorageRuntime::apply()` after SIGHUP.
    pub storage: StorageRuntimeConfig,
}

impl StorageWorkerConfig {
    /// Snapshot all storage-worker GUCs into plain Rust structs.
    ///
    /// # Safety
    ///
    /// Must be called from the bgworker main thread where `pg_sys::DataDir` is valid.
    pub fn from_gucs() -> Self {
        let endpoint = StorageEndpoint::from_config(
            gucs::enabled(),
            gucs::socket_path().map(PathBuf::from),
            gucs::cache_dir().map(PathBuf::from),
            gucs::backend_max_idle_connections(),
        )
        .expect(
            "PostgreSQL DataDir must be initialized before resolving storage paths",
        );
        let (_, socket_path, cache_dir) = endpoint.into_parts();

        Self {
            startup: StorageWorkerStartupConfig {
                socket_path,
                cache_dir,
                server_config: StorageServerConfig::default()
                    .with_max_connections(gucs::max_connections()),
                service_config: StorageServiceConfig::default()
                    .with_max_read_size(gucs::max_read_size()),
                worker_threads: gucs::worker_threads(),
                log_channel_capacity: gucs::log_channel_capacity(),
            },
            runtime: StorageWorkerRuntimeConfig::from_gucs(),
        }
    }
}

impl StorageWorkerRuntimeConfig {
    /// Re-read the Sighup-scoped GUCs into a fresh runtime config.
    ///
    /// Call this after `ProcessConfigFile(PGC_SIGHUP)` to pick up new values.
    pub fn from_gucs() -> Self {
        Self {
            shutdown_timeout: Duration::from_millis(gucs::shutdown_timeout_ms()),
            storage: StorageRuntimeConfig {
                cache: CacheRuntimeConfig {
                    touch_granularity: gucs::cache_touch_granularity(),
                    cleanup: CacheCleanupConfig {
                        max_cache_bytes: gucs::cache_max_bytes(),
                        cleanup_start_percent: gucs::cache_cleanup_start_percent(),
                        cleanup_target_percent: gucs::cache_cleanup_target_percent(),
                        cleanup_interval: gucs::cache_cleanup_interval(),
                        max_cleanup_batch_items: gucs::cache_cleanup_batch_items(),
                        max_cleanup_batch_bytes: gucs::cache_cleanup_batch_bytes(),
                    },
                },
            },
        }
    }
}
