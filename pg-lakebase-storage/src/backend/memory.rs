//! In-memory [`ObjectBackend`] used by tests and local embedding.

use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use tokio::io::AsyncReadExt;

use super::ObjectBackend;
use crate::error::{StorageError, StorageResult};
use crate::object::{ListEntry, ObjectInfo, ObjectLocation};

/// Thread-safe in-memory backend. Object payloads are stored verbatim keyed by [`ObjectLocation`].
///
/// Primarily intended for tests and local embedding; not suited to production traffic.
#[derive(Clone, Default)]
pub struct MemoryObjectBackend {
    objects: Arc<Mutex<HashMap<ObjectLocation, Vec<u8>>>>,
    head_calls: Arc<AtomicU64>,
}

impl MemoryObjectBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_objects(&self) -> MutexGuard<'_, HashMap<ObjectLocation, Vec<u8>>> {
        // This in-memory backend is primarily for tests and local embedding. A
        // poisoned lock means the object map may reflect a partially completed
        // mutation, so fail fast instead of serving possibly inconsistent data.
        self.objects
            .lock()
            .expect("memory object backend mutex poisoned; in-memory object state is no longer trustworthy")
    }

    pub fn insert(&self, key: ObjectLocation, data: impl Into<Vec<u8>>) {
        self.lock_objects().insert(key, data.into());
    }

    pub fn head_call_count(&self) -> u64 {
        self.head_calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl ObjectBackend for MemoryObjectBackend {
    async fn head(&self, key: &ObjectLocation) -> StorageResult<ObjectInfo> {
        self.head_calls.fetch_add(1, Ordering::Relaxed);
        let objects = self.lock_objects();
        let data = objects
            .get(key)
            .ok_or_else(|| StorageError::not_found(key.to_string()))?;
        Ok(ObjectInfo {
            size: data.len() as u64,
            etag: None,
        })
    }

    async fn get_range(
        &self,
        key: &ObjectLocation,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        let objects = self.lock_objects();
        let data = objects
            .get(key)
            .ok_or_else(|| StorageError::not_found(key.to_string()))?;
        let start = std::cmp::min(range.start as usize, data.len());
        let end = std::cmp::min(range.end as usize, data.len());
        if start > end {
            return Err(StorageError::backend(format!(
                "invalid range {range:?} for {key}"
            )));
        }
        Ok(bytes::Bytes::copy_from_slice(&data[start..end]))
    }

    async fn put_from_file(
        &self,
        key: &ObjectLocation,
        path: &Path,
        len: u64,
    ) -> StorageResult<ObjectInfo> {
        let mut file = tokio::fs::File::open(path).await.map_err(|error| {
            StorageError::io(format!("open staging file {}", path.display()), error)
        })?;
        let mut data = vec![0_u8; len as usize];
        file.read_exact(&mut data).await.map_err(|error| {
            StorageError::io(format!("read staging file {}", path.display()), error)
        })?;
        self.lock_objects().insert(key.clone(), data);
        Ok(ObjectInfo {
            size: len,
            etag: None,
        })
    }

    fn list(
        &self,
        store_id: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> BoxStream<'static, StorageResult<ListEntry>> {
        let prefix = prefix.unwrap_or("").to_string();
        let entries: Vec<StorageResult<ListEntry>> = {
            let objects = self.lock_objects();
            objects
                .iter()
                .filter(|(key, _)| {
                    key.store_id().as_str() == store_id
                        && key.bucket() == bucket
                        && key.key().starts_with(&prefix)
                })
                .map(|(key, value)| {
                    Ok(ListEntry {
                        key: key.key().to_string(),
                        size: value.len() as u64,
                        etag: None,
                        last_modified_ms: None,
                    })
                })
                .collect()
            // Lock dropped here; we materialised the snapshot under the mutex so the
            // returned stream does not hold any synchronous lock across awaits.
        };
        stream::iter(entries).boxed()
    }

    async fn delete(&self, key: &ObjectLocation) -> StorageResult<()> {
        self.lock_objects().remove(key);
        Ok(())
    }

    fn delete_stream(
        &self,
        store_id: &str,
        bucket: &str,
        keys: BoxStream<'static, StorageResult<String>>,
    ) -> BoxStream<'static, StorageResult<String>> {
        // Note: this implementation calls `ObjectLocation::new` which can reject keys with
        // invalid path components. `ObjectStoreBackend::delete_stream` does not have this
        // failure mode (it constructs `ObjectPath::from(key)` directly). The divergence is
        // intentional and harmless in practice: this Memory backend is for tests and local
        // embedding, the keys it sees come from `Self::list` (which never produces an
        // invalid component), and the `?` here gives a precise error if a test ever feeds
        // hand-crafted keys.
        let store_id = store_id.to_string();
        let bucket = bucket.to_string();
        let objects = self.objects.clone();
        keys.map(move |item| {
            let key = item?;
            let location = ObjectLocation::new(store_id.clone(), bucket.clone(), key.clone())?;
            objects
                .lock()
                .expect("memory object backend mutex poisoned; in-memory object state is no longer trustworthy")
                .remove(&location);
            // NotFound is suppressed: bulk delete is idempotent (mirrors the trait contract).
            Ok(key)
        })
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use futures::{StreamExt, TryStreamExt};

    use super::*;
    const TEST_STORE_ID: &str = "test-store";

    #[tokio::test]
    async fn list_filters_by_store_bucket_and_prefix() {
        let backend = MemoryObjectBackend::new();
        backend.insert(
            ObjectLocation::new("store-a", "bucket", "x/1").unwrap(),
            b"a".to_vec(),
        );
        backend.insert(
            ObjectLocation::new("store-a", "bucket", "x/2").unwrap(),
            b"b".to_vec(),
        );
        backend.insert(
            ObjectLocation::new("store-a", "bucket", "y/3").unwrap(),
            b"c".to_vec(),
        );
        backend.insert(
            ObjectLocation::new("store-b", "bucket", "x/1").unwrap(),
            b"d".to_vec(),
        );
        backend.insert(
            ObjectLocation::new("store-a", "other", "x/1").unwrap(),
            b"e".to_vec(),
        );

        let mut keys: Vec<String> = backend
            .list("store-a", "bucket", Some("x/"))
            .map_ok(|entry| entry.key)
            .try_collect()
            .await
            .unwrap();
        keys.sort();
        assert_eq!(keys, vec!["x/1".to_string(), "x/2".to_string()]);

        let mut all_keys: Vec<String> = backend
            .list("store-a", "bucket", None)
            .map_ok(|entry| entry.key)
            .try_collect()
            .await
            .unwrap();
        all_keys.sort();
        assert_eq!(
            all_keys,
            vec!["x/1".to_string(), "x/2".to_string(), "y/3".to_string()]
        );
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let backend = MemoryObjectBackend::new();
        let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "k").unwrap();
        backend.insert(key.clone(), b"v".to_vec());

        backend.delete(&key).await.unwrap();
        backend.delete(&key).await.unwrap();
    }

    #[tokio::test]
    async fn delete_stream_removes_keys_and_drains_already_missing() {
        let backend = MemoryObjectBackend::new();
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", "a").unwrap(),
            b"1".to_vec(),
        );
        backend.insert(
            ObjectLocation::new(TEST_STORE_ID, "bucket", "b").unwrap(),
            b"2".to_vec(),
        );

        let keys = stream::iter(vec![
            Ok("a".to_string()),
            Ok("missing".to_string()),
            Ok("b".to_string()),
        ])
        .boxed();
        let mut deleted: Vec<String> = backend
            .delete_stream(TEST_STORE_ID, "bucket", keys)
            .try_collect()
            .await
            .unwrap();
        deleted.sort();
        assert_eq!(
            deleted,
            vec!["a".to_string(), "b".to_string(), "missing".to_string()]
        );

        let remaining: Vec<String> = backend
            .list(TEST_STORE_ID, "bucket", None)
            .map_ok(|entry| entry.key)
            .try_collect()
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }
}
