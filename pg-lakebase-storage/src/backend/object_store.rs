//! Adapter that exposes any [`object_store::ObjectStore`] as an [`ObjectBackend`].

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{
    Error as ObjectStoreError, MultipartUpload, ObjectStore, ObjectStoreExt,
};
use tokio::io::AsyncReadExt;

use super::ObjectBackend;
use crate::error::{StorageError, StorageResult};
use crate::object::{ListEntry, ObjectInfo, ObjectLocation};

/// Adapter that exposes any [`ObjectStore`] client as an [`ObjectBackend`].
///
/// When constructed with [`ObjectStoreBackend::for_bucket`], the adapter is pinned to a single
/// bucket and reads targeting any other bucket surface as `NotFound`.
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    bucket: Option<String>,
}

impl ObjectStoreBackend {
    /// Wrap an [`ObjectStore`] that routes itself (e.g. a multi-bucket client).
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            bucket: None,
        }
    }

    /// Wrap an [`ObjectStore`] that is scoped to a single `bucket`.
    pub fn for_bucket(
        store: Arc<dyn ObjectStore>,
        bucket: impl Into<String>,
    ) -> Self {
        Self {
            store,
            bucket: Some(bucket.into()),
        }
    }

    fn location(&self, key: &ObjectLocation) -> StorageResult<ObjectPath> {
        if let Some(bucket) = &self.bucket
            && bucket != key.bucket()
        {
            return Err(StorageError::not_found(key.to_string()));
        }
        Ok(ObjectPath::from(key.key()))
    }

    /// Returns `true` when this backend is responsible for `bucket`.
    ///
    /// Bucket-pinned backends only serve their pinned bucket; unpinned backends accept any
    /// bucket and rely on the wrapped [`ObjectStore`] to route internally. Mirrors the bucket
    /// check inside [`Self::location`].
    fn services_bucket(&self, bucket: &str) -> bool {
        self.bucket.as_deref().is_none_or(|pinned| pinned == bucket)
    }

    fn prefix_path(&self, prefix: Option<&str>) -> Option<ObjectPath> {
        // `object_store::Path::from` accepts an empty string and produces an empty path, which is
        // distinct from `None` (= list whole bucket). We collapse `Some("")` to `None` so callers
        // can pass an empty string without surprise.
        match prefix {
            None | Some("") => None,
            Some(p) => Some(ObjectPath::from(p)),
        }
    }
}

#[async_trait]
impl ObjectBackend for ObjectStoreBackend {
    async fn head(&self, key: &ObjectLocation) -> StorageResult<ObjectInfo> {
        let location = self.location(key)?;
        let meta = self.store.head(&location).await.map_err(|error| {
            StorageError::backend_source(format!("head object {key}"), error)
        })?;
        Ok(ObjectInfo {
            size: meta.size,
            etag: meta.e_tag,
        })
    }

    async fn get_range(
        &self,
        key: &ObjectLocation,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes> {
        let location = self.location(key)?;
        let data = self
            .store
            .get_range(&location, range.clone())
            .await
            .map_err(|error| {
                StorageError::backend_source(
                    format!("read object range {range:?} for {key}"),
                    error,
                )
            })?;
        Ok(data)
    }

    async fn put_from_file(
        &self,
        key: &ObjectLocation,
        path: &Path,
        len: u64,
    ) -> StorageResult<ObjectInfo> {
        let location = self.location(key)?;
        let mut upload =
            self.store.put_multipart(&location).await.map_err(|error| {
                StorageError::backend_source(
                    format!("start multipart upload for {key}"),
                    error,
                )
            })?;

        if let Err(io_error) =
            stream_file_to_multipart(upload.as_mut(), path, len).await
        {
            // Best-effort abort so the backend does not accumulate an orphan multipart upload.
            let _ = upload.abort().await;
            return Err(StorageError::io(
                format!("upload staging file for {key}"),
                io_error,
            ));
        }

        let result = upload.complete().await.map_err(|error| {
            StorageError::backend_source(
                format!("complete multipart upload for {key}"),
                error,
            )
        })?;

        Ok(ObjectInfo {
            size: len,
            etag: result.e_tag,
        })
    }

    fn list(
        &self,
        _store_id: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> BoxStream<'static, StorageResult<ListEntry>> {
        if !self.services_bucket(bucket) {
            // The bucket is not served by this backend (bucket-pinned to a different bucket).
            // Mirroring `head`/`get_range` would yield NotFound, but list is a stream over a
            // possibly-empty namespace — an empty stream is the more accurate signal.
            return stream::empty().boxed();
        }
        let prefix = self.prefix_path(prefix);
        let bucket_label = bucket.to_string();
        self.store
            .list(prefix.as_ref())
            .map(move |result| match result {
                Ok(meta) => Ok(ListEntry {
                    key: meta.location.to_string(),
                    size: meta.size,
                    etag: meta.e_tag,
                    last_modified_ms: Some(meta.last_modified.timestamp_millis()),
                }),
                Err(error) => Err(StorageError::backend_source(
                    format!("list objects in bucket {bucket_label}"),
                    error,
                )),
            })
            .boxed()
    }

    async fn delete(&self, key: &ObjectLocation) -> StorageResult<()> {
        let location = self.location(key)?;
        match self.store.delete(&location).await {
            Ok(()) => Ok(()),
            Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(error) => Err(StorageError::backend_source(
                format!("delete object {key}"),
                error,
            )),
        }
    }

    fn delete_stream(
        &self,
        _store_id: &str,
        bucket: &str,
        keys: BoxStream<'static, StorageResult<String>>,
    ) -> BoxStream<'static, StorageResult<String>> {
        if !self.services_bucket(bucket) {
            // Drain the input as failures: a delete pipeline targeting a bucket this backend
            // does not serve is a configuration error worth surfacing.
            let bucket_label = bucket.to_string();
            return keys
                .map(move |item| match item {
                    Err(error) => Err(error),
                    Ok(_) => {
                        Err(StorageError::not_found(format!("bucket {bucket_label}")))
                    }
                })
                .boxed();
        }

        // `ObjectStore::delete_stream` takes a `BoxStream<'static, Result<Path, ObjectStoreError>>`;
        // build that adapter once and forward.
        let key_paths: BoxStream<'static, object_store::Result<ObjectPath>> = keys
            .map(|item| match item {
                Ok(key) => Ok(ObjectPath::from(key)),
                Err(error) => {
                    // Surface upstream errors back into the object_store stream as a generic
                    // backend error so the bulk-delete machinery propagates them. Using
                    // `Generic` avoids needing the original payload to roundtrip.
                    Err(ObjectStoreError::Generic {
                        store: "pg-lakebase-storage",
                        source: Box::new(std::io::Error::other(error.to_string())),
                    })
                }
            })
            .boxed();

        let bucket_label = bucket.to_string();
        self.store
            .delete_stream(key_paths)
            .map(move |result| match result {
                Ok(path) => Ok(path.to_string()),
                Err(ObjectStoreError::NotFound { path, .. }) => Ok(path),
                Err(error) => Err(StorageError::backend_source(
                    format!("delete object in bucket {bucket_label}"),
                    error,
                )),
            })
            .boxed()
    }
}

// ---- private helper: multipart streaming --------------------------------------------------------

/// Streams `path` into a backend multipart uploader in fixed-size parts.
///
/// Private to the `object_store` backend: staging uploads to an `ObjectStore`-backed target
/// go through [`ObjectStoreBackend::put_from_file`], which is the sole caller. Other backends
/// (e.g. [`crate::backend::MemoryObjectBackend`]) implement `put_from_file` directly without
/// a multipart abstraction.
///
/// Parts are uploaded **sequentially**. `object_store` allows the returned `UploadPart` futures
/// to be awaited in parallel for higher throughput, but doing so cleanly requires bounded
/// concurrency plus cancel-safe error handling against the multipart upload state. The
/// sequential path is enough for the current workload; the parallel variant is a later
/// performance task.
///
/// Each part is read straight into a `Vec<u8>` that is moved into `PutPayload`. That move is
/// zero-copy (`PutPayload::from(Vec<u8>)` reuses the underlying allocation as `Bytes`), so the
/// upload makes exactly one allocation per part and never copies the staged bytes again after
/// reading them off disk.
async fn stream_file_to_multipart<U: MultipartUpload + ?Sized>(
    upload: &mut U,
    path: &Path,
    expected_len: u64,
) -> std::io::Result<()> {
    /// 8 MiB: comfortably above the S3/R2 5 MiB minimum part size so multipart uploads are
    /// valid, and small enough that memory pressure per concurrent upload stays bounded.
    const MULTIPART_UPLOAD_CHUNK_BYTES: usize = 8 * 1024 * 1024;

    let mut file = tokio::fs::File::open(path).await?;
    let mut remaining = expected_len;
    while remaining > 0 {
        let part_len =
            std::cmp::min(remaining, MULTIPART_UPLOAD_CHUNK_BYTES as u64) as usize;
        let mut chunk = vec![0_u8; part_len];
        if let Err(error) = file.read_exact(&mut chunk).await {
            return Err(if error.kind() == std::io::ErrorKind::UnexpectedEof {
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "staging file ended before reported length during upload",
                )
            } else {
                error
            });
        }
        upload
            .put_part(object_store::PutPayload::from(chunk))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        remaining -= part_len as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    const TEST_STORE_ID: &str = "test-store";

    #[tokio::test]
    async fn adapts_object_store_bucket_reads() {
        let store = Arc::new(InMemory::new());
        let location = ObjectPath::from("path/file.txt");
        store
            .put(&location, b"hello object store".as_ref().into())
            .await
            .unwrap();

        let backend = ObjectStoreBackend::for_bucket(store, "bucket");
        let key =
            ObjectLocation::new(TEST_STORE_ID, "bucket", "path/file.txt").unwrap();

        let info = backend.head(&key).await.unwrap();
        assert_eq!(info.size, 18);
        let data = backend.get_range(&key, 6..12).await.unwrap();
        assert_eq!(&data[..], b"object");
    }

    #[tokio::test]
    async fn list_returns_objects_under_prefix_and_strips_bucket_label() {
        use futures::TryStreamExt;

        let store = Arc::new(InMemory::new());
        for path in ["a/1", "a/2", "b/3"] {
            store
                .put(&ObjectPath::from(path), b"x".as_ref().into())
                .await
                .unwrap();
        }
        let backend = ObjectStoreBackend::for_bucket(store, "bucket");

        let entries = backend.list(TEST_STORE_ID, "bucket", Some("a/"));
        let mut keys: Vec<String> = entries
            .map_ok(|entry| entry.key)
            .try_collect()
            .await
            .unwrap();
        keys.sort();
        assert_eq!(keys, vec!["a/1".to_string(), "a/2".to_string()]);

        let entries = backend.list(TEST_STORE_ID, "bucket", None);
        let mut keys: Vec<String> = entries
            .map_ok(|entry| entry.key)
            .try_collect()
            .await
            .unwrap();
        keys.sort();
        assert_eq!(
            keys,
            vec!["a/1".to_string(), "a/2".to_string(), "b/3".to_string()]
        );
    }

    #[tokio::test]
    async fn list_on_unrelated_pinned_bucket_yields_empty_stream() {
        use futures::TryStreamExt;

        let store = Arc::new(InMemory::new());
        store
            .put(&ObjectPath::from("only-here"), b"x".as_ref().into())
            .await
            .unwrap();
        let backend = ObjectStoreBackend::for_bucket(store, "bucket-a");

        let keys: Vec<String> = backend
            .list(TEST_STORE_ID, "bucket-b", None)
            .map_ok(|entry| entry.key)
            .try_collect()
            .await
            .unwrap();

        assert!(
            keys.is_empty(),
            "list against an unrelated bucket must be empty"
        );
    }

    #[tokio::test]
    async fn delete_existing_or_missing_succeeds_idempotently() {
        let store = Arc::new(InMemory::new());
        store
            .put(&ObjectPath::from("doomed"), b"bye".as_ref().into())
            .await
            .unwrap();

        let backend = ObjectStoreBackend::for_bucket(store, "bucket");
        let key = ObjectLocation::new(TEST_STORE_ID, "bucket", "doomed").unwrap();

        backend.delete(&key).await.unwrap();
        // Second delete is idempotent: backend disagreement on existed-vs-missing is hidden by
        // the trait contract.
        backend.delete(&key).await.unwrap();
    }

    #[tokio::test]
    async fn delete_stream_removes_listed_keys_and_passes_through_already_missing() {
        use futures::stream;
        use futures::{StreamExt, TryStreamExt};

        let store = Arc::new(InMemory::new());
        for path in ["a/1", "a/2", "a/3"] {
            store
                .put(&ObjectPath::from(path), b"x".as_ref().into())
                .await
                .unwrap();
        }
        let backend = ObjectStoreBackend::for_bucket(store.clone(), "bucket");

        let keys = stream::iter(
            [
                "a/1".to_string(),
                "a/2".to_string(),
                "a/never-existed".to_string(),
            ]
            .into_iter()
            .map(Ok),
        )
        .boxed();

        let mut deleted: Vec<String> = backend
            .delete_stream(TEST_STORE_ID, "bucket", keys)
            .try_collect()
            .await
            .unwrap();
        deleted.sort();
        assert_eq!(
            deleted,
            vec![
                "a/1".to_string(),
                "a/2".to_string(),
                "a/never-existed".to_string()
            ]
        );

        // Surviving object stays.
        let remaining: Vec<String> = backend
            .list(TEST_STORE_ID, "bucket", None)
            .map_ok(|entry| entry.key)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(remaining, vec!["a/3".to_string()]);
    }

    #[tokio::test]
    async fn put_from_file_streams_bytes_through_multipart_upload() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio::io::AsyncWriteExt;

        static NONCE: AtomicU64 = AtomicU64::new(0);

        let tmp = {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
            std::path::PathBuf::from("/tmp").join(format!(
                "pg-lakebase-storage-object-store-put-{}-{stamp}-{nonce}",
                std::process::id(),
            ))
        };
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let staging_path = tmp.join("staging");
        let mut file = tokio::fs::File::create(&staging_path).await.unwrap();
        let data = b"put_from_file-bytes".to_vec();
        file.write_all(&data).await.unwrap();
        file.flush().await.unwrap();

        let store = Arc::new(InMemory::new());
        let backend = ObjectStoreBackend::for_bucket(store.clone(), "bucket");
        let key =
            ObjectLocation::new(TEST_STORE_ID, "bucket", "uploaded.txt").unwrap();

        let info = backend
            .put_from_file(&key, &staging_path, data.len() as u64)
            .await
            .unwrap();

        assert_eq!(info.size, data.len() as u64);
        let readback = backend.get_range(&key, 0..info.size).await.unwrap();
        assert_eq!(readback, data);
    }
}
