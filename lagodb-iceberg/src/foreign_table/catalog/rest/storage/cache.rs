//! PostgreSQL-backend-local REST configured-storage cache.
//!
//! One entry owns the current configuration and service for a logical storage
//! route. Repeated table loads reuse that service. A changed response replaces
//! the entry; in-flight routes retain the prior service until their ordinary
//! lifetime ends. Idle entries expire and least-recently-used entries are
//! evicted at the capacity boundary, so a long-lived PostgreSQL backend cannot
//! retain an unbounded set of REST credentials. PostgreSQL storage profiles use
//! the separate foreign cache and never enter this cache.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lagodb_storage::{StorageError, StorageResult, StoreConfig};
use pg_lakebase_core::storage::service::{BackendStorageService, StorageEndpoint};
use pgrx::pg_sys;

use super::routes::ObjectProvider;

thread_local! {
    static CONFIGURED_STORAGE_CACHE: RefCell<ConfiguredStorageCache> =
        RefCell::new(ConfiguredStorageCache::new());
}

const MAX_CONFIGURED_STORAGE_ENTRIES: usize = 64;
const CONFIGURED_STORAGE_MAX_IDLE: Duration = Duration::from_secs(15 * 60);

/// Non-secret REST catalog binding that isolates backend-local route entries.
#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct CatalogStorageIdentity {
    catalog_server_oid: u32,
    effective_user: u32,
    catalog_name: Arc<str>,
}

impl CatalogStorageIdentity {
    pub(super) fn new(
        catalog_server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        catalog_name: Arc<str>,
    ) -> Self {
        Self {
            catalog_server_oid: u32::from(catalog_server_oid),
            effective_user: u32::from(effective_user),
            catalog_name,
        }
    }
}

/// Non-secret identity of one catalog-provided object-storage route.
///
/// Credentials and other mutable client behavior deliberately remain outside
/// this key. They are the entry version: changing any [`StoreConfig`] field
/// replaces the service stored under this stable route identity.
#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct ConfiguredStorageRouteId {
    catalog: CatalogStorageIdentity,
    socket_path: PathBuf,
    provider: ObjectProvider,
    object_endpoint: Option<String>,
    bucket: String,
    account: Option<String>,
    prefix: Option<String>,
}

impl ConfiguredStorageRouteId {
    pub(super) fn new(
        catalog: &CatalogStorageIdentity,
        endpoint: &StorageEndpoint,
        provider: ObjectProvider,
        bucket: &str,
        account: Option<&str>,
        prefix: Option<&str>,
        config: &StoreConfig,
    ) -> Self {
        let object_endpoint = match config {
            StoreConfig::S3(config) => config.endpoint.as_deref(),
            StoreConfig::S3Compatible(config) => Some(config.endpoint.as_str()),
            StoreConfig::Gcs(config) => config.base_url.as_deref(),
            StoreConfig::Azure(config) => config.endpoint.as_deref(),
        };
        Self {
            catalog: catalog.clone(),
            socket_path: endpoint.socket_path().to_path_buf(),
            provider,
            object_endpoint: object_endpoint.map(str::to_owned),
            bucket: bucket.to_owned(),
            account: account.map(str::to_owned),
            prefix: prefix.map(str::to_owned),
        }
    }
}

struct ConfiguredStorageCacheEntry {
    config: Arc<StoreConfig>,
    service: BackendStorageService,
    last_used: Instant,
}

/// Retains the current response-owned service for each logical storage route.
pub(super) struct ConfiguredStorageCache {
    entries: HashMap<ConfiguredStorageRouteId, ConfiguredStorageCacheEntry>,
}

impl ConfiguredStorageCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub(super) fn acquire(
        route_id: ConfiguredStorageRouteId,
        endpoint: &StorageEndpoint,
        config: StoreConfig,
    ) -> StorageResult<BackendStorageService> {
        CONFIGURED_STORAGE_CACHE.with(|cache| {
            let mut cache = cache.try_borrow_mut().map_err(|_| {
                StorageError::protocol(
                    "configured REST storage cache is already in use",
                )
            })?;
            cache.acquire_entry(route_id, endpoint, config)
        })
    }

    fn acquire_entry(
        &mut self,
        route_id: ConfiguredStorageRouteId,
        endpoint: &StorageEndpoint,
        config: StoreConfig,
    ) -> StorageResult<BackendStorageService> {
        let now = Instant::now();
        self.entries.retain(|_, entry| {
            now.duration_since(entry.last_used) <= CONFIGURED_STORAGE_MAX_IDLE
        });
        if let Some(entry) = self.entries.get_mut(&route_id)
            && entry.config.as_ref() == &config
        {
            entry.last_used = now;
            return Ok(entry.service.clone());
        }

        config.validate()?;
        let config = Arc::new(config);
        let service =
            BackendStorageService::for_configured(endpoint, Arc::clone(&config))?;
        if !self.entries.contains_key(&route_id)
            && self.entries.len() >= MAX_CONFIGURED_STORAGE_ENTRIES
            && let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(route, _)| route.clone())
        {
            self.entries.remove(&oldest);
        }
        self.entries.insert(
            route_id,
            ConfiguredStorageCacheEntry {
                config,
                service: service.clone(),
                last_used: now,
            },
        );
        Ok(service)
    }
}
