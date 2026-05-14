use std::path::Path;
use std::sync::Arc;

use tokio::task;

use super::kv::{CacheKv, KvTable};
use super::redb::RedbKv;
use super::tracking::RuntimeCacheTracking;
use crate::error::{StorageError, StorageResult};

#[derive(Clone)]
/// Generic implementation shared by persistent cache indexes backed by a transaction-capable ordered KV.
///
/// This is intentionally kept inside the private `persistent` module. The public entry point remains
/// [`RedbCacheIndex`]; `CacheKv` is an internal boundary, not a crate-level extension API.
pub struct PersistentCacheIndex<K: CacheKv> {
    kv: Arc<K>,
    tracking: Arc<RuntimeCacheTracking>,
}

/// Persistent [`crate::cache::index::CacheIndex`] backed by redb.
pub type RedbCacheIndex = PersistentCacheIndex<RedbKv>;

impl PersistentCacheIndex<RedbKv> {
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        Self::from_kv(RedbKv::open(path)?)
    }
}

impl<K: CacheKv> PersistentCacheIndex<K> {
    /// Builds the generic persistent index from an internal KV backend.
    ///
    /// Keep this private to the persistent module until custom KV backends become a supported API.
    pub(super) fn from_kv(kv: K) -> StorageResult<Self> {
        kv.ensure_tables(KvTable::ALL)?;

        Ok(Self {
            kv: Arc::new(kv),
            tracking: Arc::new(RuntimeCacheTracking::default()),
        })
    }

    pub(super) async fn run_kv<T>(
        &self,
        operation: impl FnOnce(&K) -> StorageResult<T> + Send + 'static,
    ) -> StorageResult<T>
    where
        T: Send + 'static,
    {
        let kv = self.kv.clone();
        task::spawn_blocking(move || operation(kv.as_ref()))
            .await
            .map_err(|error| {
                StorageError::cache_source(
                    "persistent cache index task failed",
                    error,
                )
            })?
    }

    pub(super) async fn run_tracked<T>(
        &self,
        operation: impl FnOnce(&K, &RuntimeCacheTracking) -> StorageResult<T>
        + Send
        + 'static,
    ) -> StorageResult<T>
    where
        T: Send + 'static,
    {
        let kv = self.kv.clone();
        let tracking = self.tracking.clone();
        task::spawn_blocking(move || operation(kv.as_ref(), tracking.as_ref()))
            .await
            .map_err(|error| {
                StorageError::cache_source(
                    "persistent cache index task failed",
                    error,
                )
            })?
    }

    pub(super) fn tracking(&self) -> &RuntimeCacheTracking {
        &self.tracking
    }
}
