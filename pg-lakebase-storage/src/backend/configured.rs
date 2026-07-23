//! Lazy per-bucket object-store client cache.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::ObjectStore;

use super::ObjectBackend;
use super::config::StoreConfig;
use super::object_store::ObjectStoreBackend;
use crate::error::{StorageError, StorageResult};
use crate::object::{ListEntry, ObjectInfo, ObjectLocation};

/// Backend that materializes and caches one object-store client per bucket.
pub struct ConfiguredObjectBackend {
    config: StoreConfig,
    stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl ConfiguredObjectBackend {
    pub fn new(config: StoreConfig) -> Self {
        Self {
            config,
            stores: RwLock::new(HashMap::new()),
        }
    }

    fn store_for_bucket(&self, bucket: &str) -> StorageResult<Arc<dyn ObjectStore>> {
        if let Some(store) = self
            .stores
            .read()
            .expect("configured object backend rwlock poisoned; bucket clients are no longer trustworthy")
            .get(bucket)
            .cloned()
        {
            return Ok(store);
        }

        let store = self.config.build_store(bucket)?;
        let mut stores = self
            .stores
            .write()
            .expect("configured object backend rwlock poisoned; bucket clients are no longer trustworthy");
        if let Some(existing) = stores.get(bucket).cloned() {
            return Ok(existing);
        }
        stores.insert(bucket.to_owned(), Arc::clone(&store));
        Ok(store)
    }
}

#[async_trait]
impl ObjectBackend for ConfiguredObjectBackend {
    async fn head(&self, key: &ObjectLocation) -> StorageResult<ObjectInfo> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket())
            .head(key)
            .await
    }

    async fn get_range(
        &self,
        key: &ObjectLocation,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket())
            .get_range(key, range)
            .await
    }

    async fn put_from_file(
        &self,
        key: &ObjectLocation,
        path: &std::path::Path,
        len: u64,
    ) -> StorageResult<ObjectInfo> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket())
            .put_from_file(key, path, len)
            .await
    }

    async fn put_if_absent(
        &self,
        key: &ObjectLocation,
        data: bytes::Bytes,
    ) -> StorageResult<ObjectInfo> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket())
            .put_if_absent(key, data)
            .await
    }

    fn list(
        &self,
        store_id: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> BoxStream<'static, StorageResult<ListEntry>> {
        match self.store_for_bucket(bucket) {
            Ok(store) => ObjectStoreBackend::for_bucket(store, bucket)
                .list(store_id, bucket, prefix),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn delete(&self, key: &ObjectLocation) -> StorageResult<()> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket())
            .delete(key)
            .await
    }

    fn delete_stream(
        &self,
        store_id: &str,
        bucket: &str,
        keys: BoxStream<'static, StorageResult<String>>,
    ) -> BoxStream<'static, StorageResult<String>> {
        match self.store_for_bucket(bucket) {
            Ok(store) => ObjectStoreBackend::for_bucket(store, bucket)
                .delete_stream(store_id, bucket, keys),
            Err(error) => {
                let template = error.to_string();
                keys.map(move |item| {
                    item.and_then(|_| {
                        Err(StorageError::configuration(template.clone()))
                    })
                })
                .boxed()
            }
        }
    }
}
