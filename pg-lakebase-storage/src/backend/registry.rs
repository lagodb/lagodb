//! Named-backend registry that maps [`StoreId`] to a live [`ObjectBackend`].
//!
//! [`StoreRegistry`] is the central lookup table used by the service layer to resolve which
//! backend should service reads for a given store identifier.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use futures::stream::BoxStream;
use object_store::ObjectStore;

use super::config::StoreConfig;
use super::object_store::ObjectStoreBackend;
use super::probe::BackendProbe;
use super::{ConfiguredObjectBackend, ObjectBackend, StorageProbeResult};
use crate::error::{StorageError, StorageResult};
use crate::object::{ListEntry, ObjectInfo, ObjectLocation, StoreId};

/// Thread-safe registry of named backends.
///
/// Backends are registered under a [`StoreId`] and can be resolved by the service layer at
/// open-time. Registration bumps a monotonic generation counter so callers can detect
/// replacement.
#[derive(Clone, Default)]
pub struct StoreRegistry {
    inner: Arc<RwLock<StoreRegistryInner>>,
}

#[derive(Default)]
struct StoreRegistryInner {
    stores: HashMap<StoreId, Arc<RegisteredStore>>,
    next_generation: u64,
}

/// A backend that has been registered under a [`StoreId`] with a generation stamp.
pub struct RegisteredStore {
    id: StoreId,
    backend: Arc<dyn ObjectBackend>,
    generation: u64,
}

impl StoreRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return this registry after registering a shared backend under `id`.
    ///
    /// This is the builder-style counterpart to [`Self::register_shared_backend`] for callers
    /// that already own an [`Arc<dyn ObjectBackend>`].
    pub fn with_shared_backend(
        self,
        id: impl Into<String>,
        backend: Arc<dyn ObjectBackend>,
    ) -> StorageResult<Self> {
        self.register_shared_backend(id, backend)?;
        Ok(self)
    }

    fn read_inner(&self) -> RwLockReadGuard<'_, StoreRegistryInner> {
        self.inner
            .read()
            .expect("store registry rwlock poisoned; registered store state is no longer trustworthy")
    }

    fn write_inner(&self) -> RwLockWriteGuard<'_, StoreRegistryInner> {
        self.inner
            .write()
            .expect("store registry rwlock poisoned; registered store state is no longer trustworthy")
    }

    /// Register a concrete [`ObjectBackend`] under `id`.
    pub fn register_backend<B>(
        &self,
        id: impl Into<String>,
        backend: B,
    ) -> StorageResult<Option<Arc<RegisteredStore>>>
    where
        B: ObjectBackend + 'static,
    {
        self.register_shared_backend(id, Arc::new(backend))
    }

    /// Register a shared (already-`Arc`'d) backend under `id`.
    pub fn register_shared_backend(
        &self,
        id: impl Into<String>,
        backend: Arc<dyn ObjectBackend>,
    ) -> StorageResult<Option<Arc<RegisteredStore>>> {
        let id = StoreId::new(id)?;
        let mut inner = self.write_inner();
        inner.next_generation = inner.next_generation.saturating_add(1);
        let store = Arc::new(RegisteredStore {
            id: id.clone(),
            backend,
            generation: inner.next_generation,
        });
        Ok(inner.stores.insert(id, store))
    }

    /// Validate and register a [`StoreConfig`] under `id`.
    pub fn register_config(
        &self,
        id: impl Into<String>,
        config: StoreConfig,
    ) -> StorageResult<Option<Arc<RegisteredStore>>> {
        config.validate()?;
        self.register_backend(id, ConfiguredObjectBackend::new(config))
    }

    /// Register a raw [`ObjectStore`] under `id` (no bucket pinning).
    pub fn register_object_store<S>(
        &self,
        id: impl Into<String>,
        store: Arc<S>,
    ) -> StorageResult<Option<Arc<RegisteredStore>>>
    where
        S: ObjectStore + 'static,
    {
        let store: Arc<dyn ObjectStore> = store;
        self.register_backend(id, ObjectStoreBackend::new(store))
    }

    /// Register a raw [`ObjectStore`] pinned to a single `bucket` under `id`.
    pub fn register_object_store_bucket<S>(
        &self,
        id: impl Into<String>,
        store: Arc<S>,
        bucket: impl Into<String>,
    ) -> StorageResult<Option<Arc<RegisteredStore>>>
    where
        S: ObjectStore + 'static,
    {
        let store: Arc<dyn ObjectStore> = store;
        self.register_backend(id, ObjectStoreBackend::for_bucket(store, bucket))
    }

    /// Remove the backend registered under `id`, returning it if present.
    pub fn unregister(&self, id: &StoreId) -> Option<Arc<RegisteredStore>> {
        self.write_inner().stores.remove(id)
    }

    /// Convenience wrapper around [`Self::unregister`] that parses the id first.
    pub fn unregister_id(
        &self,
        id: impl Into<String>,
    ) -> StorageResult<Option<Arc<RegisteredStore>>> {
        let id = StoreId::new(id)?;
        Ok(self.unregister(&id))
    }

    /// Resolve the backend registered under `id`, or return `NotFound`.
    pub fn resolve(&self, id: &StoreId) -> StorageResult<Arc<RegisteredStore>> {
        self.read_inner()
            .stores
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::not_found(format!("store {id}")))
    }

    /// Check whether a backend is registered under `id`.
    pub fn contains(&self, id: &StoreId) -> bool {
        self.read_inner().stores.contains_key(id)
    }
}

impl fmt::Debug for StoreRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.read_inner();
        f.debug_struct("StoreRegistry")
            .field("stores", &inner.stores.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl RegisteredStore {
    pub fn id(&self) -> &StoreId {
        &self.id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub async fn head(&self, key: &ObjectLocation) -> StorageResult<ObjectInfo> {
        self.backend.head(key).await
    }

    pub async fn get_range(
        &self,
        key: &ObjectLocation,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        self.backend.get_range(key, range).await
    }

    pub async fn put_from_file(
        &self,
        key: &ObjectLocation,
        path: &std::path::Path,
        len: u64,
    ) -> StorageResult<ObjectInfo> {
        self.backend.put_from_file(key, path, len).await
    }

    /// Exercises list, create-only write, metadata/read-back, and delete against this registered
    /// backend. The probe bypasses local cache and staging state.
    pub async fn probe(
        &self,
        bucket: &str,
        root_prefix: &str,
    ) -> StorageResult<StorageProbeResult> {
        Ok(BackendProbe::new(
            self.backend.as_ref(),
            self.id.as_str(),
            bucket,
            root_prefix,
        )?
        .run()
        .await)
    }

    pub fn list(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> BoxStream<'static, StorageResult<ListEntry>> {
        self.backend.list(self.id.as_str(), bucket, prefix)
    }

    pub async fn delete(&self, key: &ObjectLocation) -> StorageResult<()> {
        self.backend.delete(key).await
    }

    pub fn delete_stream(
        &self,
        bucket: &str,
        keys: BoxStream<'static, StorageResult<String>>,
    ) -> BoxStream<'static, StorageResult<String>> {
        self.backend.delete_stream(self.id.as_str(), bucket, keys)
    }
}

impl fmt::Debug for RegisteredStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisteredStore")
            .field("id", &self.id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryObjectBackend;

    #[test]
    fn with_shared_backend_builds_registered_registry() {
        let registry = StoreRegistry::new()
            .with_shared_backend("store-a", Arc::new(MemoryObjectBackend::new()))
            .unwrap();

        assert!(registry.contains(&StoreId::new("store-a").unwrap()));
    }

    #[tokio::test]
    async fn registry_unregister_does_not_invalidate_resolved_store() {
        let registry = StoreRegistry::new();
        let backend = MemoryObjectBackend::new();
        let key = ObjectLocation::new("store-a", "bucket", "file").unwrap();
        backend.insert(key.clone(), b"stable".to_vec());
        registry.register_backend("store-a", backend).unwrap();

        let store = registry.resolve(key.store_id()).unwrap();
        let removed = registry.unregister(key.store_id());

        assert!(removed.is_some());
        assert!(registry.resolve(key.store_id()).is_err());
        let data = store.get_range(&key, 0..6).await.unwrap();
        assert_eq!(&data[..], b"stable");
    }
}
