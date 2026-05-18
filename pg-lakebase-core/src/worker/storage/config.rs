//! Snapshot of GUC values into a plain Rust struct that can be sent to Tokio tasks.
//!
//! [`StorageWorkerConfig::from_gucs`] must be called from the bgworker main thread
//! (the only Postgres-facing thread).  After construction the struct contains no
//! references to Postgres internals and is safe to move across thread boundaries.

use std::ffi::CStr;
use std::path::PathBuf;
use std::time::Duration;

use pg_lakebase_storage::{StorageServerConfig, StorageServiceConfig};
use pgrx::pg_sys;

use super::gucs;

#[derive(Clone)]
pub struct StorageWorkerConfig {
    pub socket_path: PathBuf,
    pub cache_dir: PathBuf,
    pub server_config: StorageServerConfig,
    pub service_config: StorageServiceConfig,
    pub worker_threads: usize,
    pub shutdown_timeout: Duration,
    pub log_channel_capacity: usize,
    /// `Some(d)` enables a periodic full-resync of the tablespace store
    /// reconciler every `d`. `None` disables the periodic resync; reconcile
    /// then runs only on syscache wake-up.
    pub tablespace_reconcile_interval: Option<Duration>,
}

impl StorageWorkerConfig {
    /// Snapshot all storage-worker GUCs into a plain Rust struct.
    ///
    /// # Safety
    ///
    /// Must be called from the bgworker main thread where `pg_sys::DataDir` is valid.
    pub fn from_gucs() -> Self {
        let data_dir = unsafe {
            CStr::from_ptr(pg_sys::DataDir)
                .to_string_lossy()
                .into_owned()
        };

        let base = PathBuf::from(&data_dir).join("pg_lakebase");

        Self {
            socket_path: gucs::socket_path()
                .map(PathBuf::from)
                .unwrap_or_else(|| base.join("storage.sock")),
            cache_dir: gucs::cache_dir()
                .map(PathBuf::from)
                .unwrap_or_else(|| base.join("storage-cache")),
            server_config: StorageServerConfig::default()
                .with_max_connections(gucs::max_connections()),
            service_config: StorageServiceConfig::default()
                .with_max_read_size(gucs::max_read_size()),
            worker_threads: gucs::worker_threads(),
            shutdown_timeout: Duration::from_millis(gucs::shutdown_timeout_ms()),
            log_channel_capacity: gucs::log_channel_capacity(),
            tablespace_reconcile_interval: gucs::tablespace_reconcile_interval(),
        }
    }
}
