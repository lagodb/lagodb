//! Staging-file subsystem.
//!
//! # What staging is
//!
//! A caller (the database) that wants to upload a new object creates a staging file locally
//! using [`StagingPathResolver`] to derive the path, writes bytes through regular filesystem
//! syscalls, and later asks the server to `Upload` (copy to the backend). The server is
//! entirely uninvolved in staging-file creation or writes — no handles, no per-file server
//! state, no heartbeats.
//!
//! # Why staging is local, not server-mediated
//!
//! Database transactions that drive this API can live for hours. Tying the staging file's
//! lifetime to a TCP-style handle or a connection would force the caller to keep one live
//! connection for the duration of the transaction. With local creation the staging file outlives
//! any particular connection, and `Upload` can be issued from any future connection against the
//! same `(store_id, bucket, key)`.
//!
//! # Lifecycle and cleanup ownership
//!
//! The database (caller) owns the staging directory's lifecycle:
//!
//! * After a successful upload, the caller decides whether to keep or delete the local copy
//!   (typically delete; the bytes already live in the backend).
//! * On a transaction abort, the caller knows exactly which keys it staged and unlinks them
//!   itself.
//! * On database startup / crash recovery, the caller removes or reconciles the staging
//!   directory before resuming.
//!
//! The server never creates staging files, never creates or wipes the staging root, and exposes
//! no abort verb. Cleanup happens entirely through ordinary filesystem syscalls issued by the
//! caller against paths derived from [`StagingPathResolver`].
//!
//! # Relationship to the cache invariants
//!
//! Upload does **not** touch the cache. The three cache invariants
//! (immutable size/etag per key, no generations, external invalidation only) therefore apply
//! to staging in the only way that matters: if a resident cached copy of the same
//! `(store, bucket, key)` exists when an Upload succeeds, the cached copy is left alone. If the
//! caller wants to read the just-uploaded content, they must call `InvalidateObjectCache`
//! explicitly before the next `Open`, which is the same contract used for any externally
//! modified object.

pub mod path;

use std::path::PathBuf;

use tracing::info;

use crate::backend::RegisteredStore;
use crate::error::{StorageError, StorageResult};
use crate::object::{ObjectInfo, ObjectLocation};

pub use path::StagingPathResolver;

/// Crate-internal uploader for caller-owned staging files.
///
/// The database creates, owns, and removes staging files directly through `StagingFile` /
/// filesystem APIs. The server only resolves the deterministic staging path and uploads the
/// closed file.
pub(crate) struct StagingUploader {
    paths: StagingPathResolver,
}

impl StagingUploader {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            paths: StagingPathResolver::new(root),
        }
    }

    /// Uploads the staging file for `key` via `store.put_from_file`.
    ///
    /// Upload copies a staging file into the backend. The staging file is left on disk regardless
    /// of whether the upload succeeds or fails — the database (caller) owns the staging
    /// directory's lifecycle and is responsible for unlinking files it no longer needs (after a
    /// successful upload, after a transaction abort before upload, or during crash recovery on
    /// database restart).
    ///
    /// **Failure** is also non-destructive: upload errors are frequently transient (network,
    /// throttling) and staged bytes may be GB-scale, so forcing a rewrite on every retry is
    /// unacceptable. The caller decides whether to retry upload, leave the file in place, or
    /// delete it.
    pub(crate) async fn upload(
        &self,
        key: &ObjectLocation,
        store: &RegisteredStore,
    ) -> StorageResult<ObjectInfo> {
        let path = self.paths.path_for(key)?;
        let metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::not_found(format!(
                    "staging file for {key}"
                )));
            }
            Err(error) => {
                return Err(StorageError::io(
                    format!("failed to stat staging file {}", path.display()),
                    error,
                ));
            }
        };
        let size = metadata.len();

        let info = store.put_from_file(key, &path, size).await?;
        info!(key = %key, size, "staging file uploaded");

        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::backend::{MemoryObjectBackend, ObjectBackend, StoreRegistry};
    use crate::client::StagingFile;

    static NONCE: AtomicU64 = AtomicU64::new(0);
    const TEST_STORE_ID: &str = "test-store";

    fn test_root(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let nonce = NONCE.fetch_add(1, Ordering::Relaxed);
        PathBuf::from("/tmp").join(format!(
            "pg-lakebase-storage-staging-test-{}-{stamp}-{nonce}-{label}",
            std::process::id()
        ))
    }

    fn test_key(key: &str) -> ObjectLocation {
        ObjectLocation::new(TEST_STORE_ID, "bucket", key).unwrap()
    }

    fn create_staging_file(root: PathBuf, key: &str, data: &[u8]) -> PathBuf {
        let resolver = StagingPathResolver::new(root);
        let mut staging_file =
            StagingFile::create(&resolver, TEST_STORE_ID, "bucket", key).unwrap();
        staging_file.write(data).unwrap();
        let path = staging_file.path().to_path_buf();
        drop(staging_file);
        path
    }

    fn memory_store() -> (StoreRegistry, MemoryObjectBackend) {
        let registry = StoreRegistry::new();
        let backend = MemoryObjectBackend::new();
        registry
            .register_shared_backend(
                TEST_STORE_ID,
                std::sync::Arc::new(backend.clone()),
            )
            .unwrap();
        (registry, backend)
    }

    #[tokio::test]
    async fn upload_copies_bytes_and_keeps_staging_file_for_caller_to_clean_up() {
        let root = test_root("upload");
        let staging = StagingUploader::new(root.clone());
        let key = test_key("upload.txt");
        let path = create_staging_file(root, "upload.txt", b"hello upload");

        let (registry, backend) = memory_store();
        let store = registry.resolve(key.store_id()).unwrap();

        let info = staging.upload(&key, &store).await.unwrap();

        assert_eq!(info.size, b"hello upload".len() as u64);
        // Upload never unlinks. The caller (database) decides when to remove the staging file.
        assert!(
            tokio::fs::try_exists(&path).await.unwrap(),
            "upload must not unlink the staging file; the database owns staging cleanup",
        );
        let readback = backend.get_range(&key, 0..info.size).await.unwrap();
        assert_eq!(&readback[..], b"hello upload");
    }

    #[tokio::test]
    async fn upload_preserves_staging_file_on_upload_failure_so_client_can_retry() {
        // Upload failures on GB-scale staging files must not force the client to rewrite the
        // whole staging file. The staging bytes stay on disk until the client decides to retry
        // upload or explicitly abort.
        use async_trait::async_trait;
        use std::ops::Range;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct FlakyBackend {
            attempts: AtomicUsize,
            succeed_on_attempt: usize,
            inner: MemoryObjectBackend,
        }

        #[async_trait]
        impl ObjectBackend for FlakyBackend {
            async fn head(&self, _key: &ObjectLocation) -> StorageResult<ObjectInfo> {
                Ok(ObjectInfo {
                    size: 0,
                    etag: None,
                })
            }
            async fn get_range(
                &self,
                key: &ObjectLocation,
                range: Range<u64>,
            ) -> StorageResult<bytes::Bytes> {
                self.inner.get_range(key, range).await
            }
            async fn put_from_file(
                &self,
                key: &ObjectLocation,
                path: &std::path::Path,
                len: u64,
            ) -> StorageResult<ObjectInfo> {
                let attempt = self.attempts.fetch_add(1, Ordering::AcqRel) + 1;
                if attempt < self.succeed_on_attempt {
                    return Err(StorageError::backend(format!(
                        "simulated transient failure on attempt {attempt}"
                    )));
                }
                self.inner.put_from_file(key, path, len).await
            }
            fn list(
                &self,
                _store_id: &str,
                _bucket: &str,
                _prefix: Option<&str>,
            ) -> futures::stream::BoxStream<
                'static,
                StorageResult<crate::object::ListEntry>,
            > {
                unreachable!("FlakyBackend does not participate in list")
            }
            async fn delete(&self, _key: &ObjectLocation) -> StorageResult<()> {
                unreachable!("FlakyBackend does not participate in delete")
            }
            fn delete_stream(
                &self,
                _store_id: &str,
                _bucket: &str,
                _keys: futures::stream::BoxStream<'static, StorageResult<String>>,
            ) -> futures::stream::BoxStream<'static, StorageResult<String>>
            {
                unreachable!("FlakyBackend does not participate in delete_stream")
            }
        }

        let root = test_root("upload-retry");
        let staging = StagingUploader::new(root.clone());
        let key = test_key("retry.txt");
        let path = create_staging_file(root, "retry.txt", b"retry-me");

        let inner = MemoryObjectBackend::new();
        let backend = Arc::new(FlakyBackend {
            attempts: AtomicUsize::new(0),
            succeed_on_attempt: 2,
            inner,
        });
        let registry = StoreRegistry::new();
        registry
            .register_shared_backend(TEST_STORE_ID, backend.clone())
            .unwrap();
        let store = registry.resolve(key.store_id()).unwrap();

        let first = staging.upload(&key, &store).await.unwrap_err();
        assert!(
            matches!(first, StorageError::Backend { .. }),
            "first attempt must bubble backend failure"
        );
        assert!(
            tokio::fs::try_exists(&path).await.unwrap(),
            "staging file must survive a failed upload so the client can retry without re-writing bytes",
        );

        let info = staging.upload(&key, &store).await.unwrap();
        assert_eq!(info.size, b"retry-me".len() as u64);
        assert!(
            tokio::fs::try_exists(&path).await.unwrap(),
            "successful retry must not unlink the staging file; the database owns staging cleanup",
        );

        let readback = backend.get_range(&key, 0..info.size).await.unwrap();
        assert_eq!(&readback[..], b"retry-me");
    }
}
