//! Opinionated wiring from paths + config objects into a listening [`StorageServer`].
//!
//! # Design
//!
//! The builder collects three categories of input:
//!
//! 1. **Paths** — socket path, cache directory, optional DB path (constructor + `with_db_path`).
//! 2. **Config objects** — [`StorageServerConfig`] and [`StorageServiceConfig`] set via
//!    `with_server_config` / `with_service_config`. These config types have their own fluent
//!    builder methods; compose them before passing to the builder.
//! 3. **Runtime components** — request hooks (builder-specific concerns that don't belong in a
//!    serializable config struct).
//!
//! Backends are registered dynamically by clients after the server is running (via the
//! `RegisterStore` protocol message), so the builder does not accept backend configuration.
//!
//! [`Self::bind`] / [`Self::bind_with_index`] perform the async startup sequence: directory
//! creation, cache recovery, optional startup cleanup, staging wipe, and finally socket bind.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tracing::info;

use crate::backend::StoreRegistry;
use crate::cache::{CacheCleanupPolicy, CacheIndex, CacheManager, RedbCacheIndex};
use crate::config::{CacheCleanupConfig, StorageServerConfig, StorageServiceConfig};
use crate::error::StorageResult;
use crate::request::{
    RequestHooks, RequestObserver, RequestPolicy, TracingRequestObserver,
};
use crate::server::StorageServer;
use crate::service::StorageService;
use crate::staging::StagingArea;

#[cfg(test)]
mod tests;

/// Fluent constructor for [`StorageServer`] with default [`RedbCacheIndex`] or a supplied [`CacheIndex`].
///
/// # Example
///
/// ```ignore
/// use pg_lakebase_storage::{StorageServerBuilder, StorageServerConfig, StorageServiceConfig};
///
/// let server = StorageServerBuilder::new("/tmp/storage.sock", "/tmp/cache")
///     .with_server_config(
///         StorageServerConfig::default()
///             .with_max_connections(1024)
///             .with_max_in_flight_requests(256)
///     )
///     .with_service_config(
///         StorageServiceConfig::default()
///             .with_max_cache_bytes(100 * 1024 * 1024 * 1024)
///             .with_max_read_size(1024 * 1024)
///     )
///     .bind()
///     .await?;
/// ```
pub struct StorageServerBuilder {
    socket_path: PathBuf,
    cache_dir: PathBuf,
    db_path: Option<PathBuf>,
    service_config: StorageServiceConfig,
    server_config: StorageServerConfig,
    registry: StoreRegistry,
    request_hooks: RequestHooks,
}

// ---- Construction & paths -----------------------------------------------------------------------

impl StorageServerBuilder {
    /// Creates a builder with the required socket path and cache directory.
    pub fn new(socket_path: impl AsRef<Path>, cache_dir: impl AsRef<Path>) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
            cache_dir: cache_dir.as_ref().to_path_buf(),
            db_path: None,
            service_config: StorageServiceConfig::default(),
            server_config: StorageServerConfig::default(),
            registry: StoreRegistry::new(),
            request_hooks: RequestHooks::default(),
        }
    }

    /// Overrides the default redb database path (`<cache_dir>/db/index.redb`).
    pub fn with_db_path(mut self, db_path: impl AsRef<Path>) -> Self {
        self.db_path = Some(db_path.as_ref().to_path_buf());
        self
    }
}

// ---- Configuration (compose config objects, don't duplicate their methods) ----------------------

impl StorageServerBuilder {
    /// Installs a complete service config (read clamps, cache geometry, cleanup policy).
    ///
    /// Build the config with [`StorageServiceConfig::default()`] and its `with_*` methods,
    /// then pass it here.
    pub fn with_service_config(
        mut self,
        service_config: StorageServiceConfig,
    ) -> Self {
        self.service_config = service_config.normalized();
        self
    }

    /// Installs a complete server config (connection limits, backpressure, drain timeout).
    ///
    /// Build the config with [`StorageServerConfig::default()`] and its `with_*` methods,
    /// then pass it here.
    pub fn with_server_config(mut self, server_config: StorageServerConfig) -> Self {
        self.server_config = server_config.normalized();
        self
    }
}

// ---- Request hooks ------------------------------------------------------------------------------

impl StorageServerBuilder {
    /// Installs a request observer (metrics, tracing, audit).
    pub fn with_request_observer<O>(mut self, observer: O) -> Self
    where
        O: RequestObserver,
    {
        self.request_hooks = self.request_hooks.with_observer(observer);
        self
    }

    /// Installs the built-in [`TracingRequestObserver`].
    pub fn with_tracing_request_observer(self) -> Self {
        self.with_request_observer(TracingRequestObserver)
    }

    /// Installs a shared (Arc'd) request observer.
    pub fn with_shared_request_observer(
        mut self,
        observer: Arc<dyn RequestObserver>,
    ) -> Self {
        self.request_hooks = self.request_hooks.with_shared_observer(observer);
        self
    }

    /// Installs a request policy (rate limiting, quota, access control).
    pub fn with_request_policy<P>(mut self, policy: P) -> Self
    where
        P: RequestPolicy,
    {
        self.request_hooks = self.request_hooks.with_policy(policy);
        self
    }

    /// Installs a shared (Arc'd) request policy.
    pub fn with_shared_request_policy(
        mut self,
        policy: Arc<dyn RequestPolicy>,
    ) -> Self {
        self.request_hooks = self.request_hooks.with_shared_policy(policy);
        self
    }
}

// ---- Bind (async startup) -----------------------------------------------------------------------

impl StorageServerBuilder {
    /// Binds the server with the default [`RedbCacheIndex`].
    ///
    /// Performs the full startup sequence: directory creation → cache recovery → optional
    /// startup cleanup → staging wipe → socket bind.
    pub async fn bind(self) -> StorageResult<StorageServer<RedbCacheIndex>> {
        let Self {
            socket_path,
            cache_dir,
            db_path,
            service_config,
            server_config,
            registry,
            request_hooks,
        } = self;
        let service_config = service_config.normalized();

        prepare_dirs(&socket_path, &cache_dir).await?;

        let db_path =
            db_path.unwrap_or_else(|| cache_dir.join("db").join("index.redb"));
        let index = RedbCacheIndex::open(db_path)?;
        start_server(
            socket_path,
            cache_dir,
            service_config,
            server_config.normalized(),
            registry,
            index,
            request_hooks,
        )
        .await
    }

    /// Binds the server with a caller-supplied [`CacheIndex`] implementation.
    ///
    /// Same startup sequence as [`Self::bind`] but skips redb database creation.
    pub async fn bind_with_index<I>(self, index: I) -> StorageResult<StorageServer<I>>
    where
        I: CacheIndex + 'static,
    {
        let Self {
            socket_path,
            cache_dir,
            db_path: _,
            service_config,
            server_config,
            registry,
            request_hooks,
        } = self;
        let service_config = service_config.normalized();

        prepare_dirs(&socket_path, &cache_dir).await?;
        start_server(
            socket_path,
            cache_dir,
            service_config,
            server_config.normalized(),
            registry,
            index,
            request_hooks,
        )
        .await
    }
}

// ---- Internal startup orchestration -------------------------------------------------------------

/// Creates parent directories for the socket and cache root.
async fn prepare_dirs(socket_path: &Path, cache_dir: &Path) -> StorageResult<()> {
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::create_dir_all(cache_dir).await?;
    Ok(())
}

/// Assembles and starts the server: cache manager → recover → cleanup → reaper → staging → bind.
async fn start_server<I>(
    socket_path: PathBuf,
    cache_dir: PathBuf,
    service_config: StorageServiceConfig,
    server_config: StorageServerConfig,
    registry: StoreRegistry,
    index: I,
    request_hooks: RequestHooks,
) -> StorageResult<StorageServer<I>>
where
    I: CacheIndex + 'static,
{
    let cleanup_policy = derive_cleanup_policy(&service_config.cache_cleanup);
    let cleanup_interval = service_config
        .cache_cleanup
        .clone()
        .normalized()
        .cleanup_interval;

    let mut cache_manager = CacheManager::new(cache_dir.clone(), index)
        .with_limits(service_config.small_object_limit, service_config.chunk_size)
        .with_touch_granularity(service_config.touch_granularity);

    let recovery = cache_manager.recover().await?;
    info!(
        objects_seen = recovery.objects_seen,
        orphan_complete_files = recovery.orphan_complete_files,
        orphan_partial_files = recovery.orphan_partial_files,
        resident_bytes = recovery.logical_usage_after.resident_bytes,
        "startup cache recovery complete",
    );
    if let Some(policy) = cleanup_policy {
        let cleanup = cache_manager.cleanup_capacity_only(policy).await?;
        info!(
            bytes_before = cleanup.bytes_before,
            bytes_after = cleanup.bytes_after,
            evicted_objects = cleanup.evicted_objects,
            bytes_evicted = cleanup.bytes_evicted,
            "startup capacity cleanup complete",
        );
        cache_manager = cache_manager.with_cleanup_policy(policy);
    }

    let cache = Arc::new(cache_manager);
    cache.spawn_large_fill_reaper();
    if let (Some(policy), Some(interval)) = (cleanup_policy, cleanup_interval) {
        cache.clone().spawn_cleanup_task(policy, interval);
    }

    let staging = Arc::new(StagingArea::new(cache_dir));
    staging.prepare_dirs().await?;
    staging.wipe().await?;
    info!(staging_dir = %staging.paths().staging_dir().display(), "staging directory wiped on startup");

    let service = Arc::new(StorageService::with_staging(
        registry,
        cache,
        staging,
        service_config,
    ));
    StorageServer::bind_with_config_and_hooks(
        socket_path,
        service,
        server_config,
        request_hooks,
    )
    .await
}

/// Derives a [`CacheCleanupPolicy`] from the user-facing [`CacheCleanupConfig`], returning
/// `None` when cleanup is entirely disabled (no capacity limit set).
fn derive_cleanup_policy(config: &CacheCleanupConfig) -> Option<CacheCleanupPolicy> {
    let config = config.clone().normalized();
    let max_cache_bytes = config.max_cache_bytes?;
    let mut policy = CacheCleanupPolicy::new(max_cache_bytes);
    policy.cleanup_start_ratio = f64::from(config.cleanup_start_percent) / 100.0;
    policy.cleanup_target_ratio = f64::from(config.cleanup_target_percent) / 100.0;
    policy.max_cleanup_batch_items = config.max_cleanup_batch_items.max(1);
    policy.max_cleanup_batch_bytes = config.max_cleanup_batch_bytes.max(1);
    Some(policy)
}
