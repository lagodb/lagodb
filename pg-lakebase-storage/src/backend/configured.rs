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
use crate::object::{ListEntry, ObjectInfo, ObjectPath};

/// Backend that materializes and caches one object-store client per bucket.
pub struct ConfiguredObjectBackend {
    config: Arc<StoreConfig>,
    stores: RwLock<HashMap<String, BucketStores>>,
}

#[derive(Clone)]
struct BucketStores {
    default: Arc<dyn ObjectStore>,
    upload: Arc<dyn ObjectStore>,
}

impl ConfiguredObjectBackend {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        Self {
            config,
            stores: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn config(&self) -> &StoreConfig {
        &self.config
    }

    fn stores_for_bucket(&self, bucket: &str) -> StorageResult<BucketStores> {
        if let Some(stores) = self
            .stores
            .read()
            .expect("configured object backend rwlock poisoned; bucket clients are no longer trustworthy")
            .get(bucket)
            .cloned()
        {
            return Ok(stores);
        }

        let bucket_stores = BucketStores {
            default: self.config.build_store(bucket)?,
            upload: self.config.build_upload_store(bucket)?,
        };
        let mut stores = self
            .stores
            .write()
            .expect("configured object backend rwlock poisoned; bucket clients are no longer trustworthy");
        if let Some(existing) = stores.get(bucket).cloned() {
            return Ok(existing);
        }
        stores.insert(bucket.to_owned(), bucket_stores.clone());
        Ok(bucket_stores)
    }
}

#[async_trait]
impl ObjectBackend for ConfiguredObjectBackend {
    async fn head(&self, key: &ObjectPath) -> StorageResult<ObjectInfo> {
        let stores = self.stores_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(stores.default, key.bucket())
            .head(key)
            .await
    }

    async fn get_range(
        &self,
        key: &ObjectPath,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        let stores = self.stores_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(stores.default, key.bucket())
            .get_range(key, range)
            .await
    }

    async fn put_from_file(
        &self,
        key: &ObjectPath,
        path: &std::path::Path,
        len: u64,
    ) -> StorageResult<ObjectInfo> {
        let stores = self.stores_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(stores.upload, key.bucket())
            .put_from_file(key, path, len)
            .await
    }

    async fn put_if_absent(
        &self,
        key: &ObjectPath,
        data: bytes::Bytes,
    ) -> StorageResult<ObjectInfo> {
        let stores = self.stores_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(stores.default, key.bucket())
            .put_if_absent(key, data)
            .await
    }

    fn list(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> BoxStream<'static, StorageResult<ListEntry>> {
        match self.stores_for_bucket(bucket) {
            Ok(stores) => ObjectStoreBackend::for_bucket(stores.default, bucket)
                .list(bucket, prefix),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn delete(&self, key: &ObjectPath) -> StorageResult<()> {
        let stores = self.stores_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(stores.default, key.bucket())
            .delete(key)
            .await
    }

    fn delete_stream(
        &self,
        bucket: &str,
        keys: BoxStream<'static, StorageResult<String>>,
    ) -> BoxStream<'static, StorageResult<String>> {
        match self.stores_for_bucket(bucket) {
            Ok(stores) => ObjectStoreBackend::for_bucket(stores.default, bucket)
                .delete_stream(bucket, keys),
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
