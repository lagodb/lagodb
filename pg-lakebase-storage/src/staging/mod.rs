//! Staging-file subsystem.
//!
//! # What staging is
//!
//! A client that wants to upload a new object asks the server for a staging path via
//! `StageCreate`, writes bytes into that local file through regular filesystem syscalls, and
//! later asks the server to either `Commit` (upload to the backend and unlink) or `Abort`
//! (just unlink). The server is entirely uninvolved between `StageCreate` and the terminal
//! `Commit` / `Abort` — no handles, no per-file server state, no heartbeats.
//!
//! # Why the server is path-handoff only
//!
//! Database transactions that drive this API can live for hours between writing a file and
//! committing. Tying the staging file's lifetime to a TCP-style handle or a connection would
//! force the caller to keep one live connection for the duration of the transaction. With
//! path-handoff the staging file outlives any particular connection, and `Commit` / `Abort`
//! can be issued from any future connection against the same `(store_id, bucket, key)`.
//!
//! # Lifecycle and cleanup
//!
//! Orphan staging files are cleaned up by exactly one mechanism: startup `wipe()` removes the
//! entire `<cache_dir>/staging/` tree. That is safe because:
//!
//! * a running server keeps no in-memory state about staged files, so nothing needs to be
//!   reconciled across a restart,
//! * client-side semantics (see `StagingFile` in `crate::client`) assume "write, then either
//!   commit or abort" — a crashed client that loses both must treat the file as gone, which
//!   matches the server wiping it on boot,
//! * the staging tree is never walked during normal cache cleanup or eviction, so staging
//!   files cannot compete with cache entries for lifetime guarantees.
//!
//! # Relationship to the cache invariants
//!
//! Commit does **not** touch the cache. The three cache invariants
//! (immutable size/etag per key, no generations, external invalidation only) therefore apply
//! to staging in the only way that matters: if a resident cached copy of the same
//! `(store, bucket, key)` exists when a Commit succeeds, the cached copy is left alone. If the
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

/// Door into the staging subsystem. Server code only ever talks to the staging area through this
/// façade — `create` / `commit` / `abort` / `wipe` map 1:1 to the three client-visible wire verbs
/// plus the startup sweep.
pub struct StagingArea {
    paths: StagingPathResolver,
}

impl StagingArea {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            paths: StagingPathResolver::new(root),
        }
    }

    pub fn paths(&self) -> &StagingPathResolver {
        &self.paths
    }

    /// Ensures the staging root exists. Called once at server startup before `wipe`.
    pub async fn prepare_dirs(&self) -> StorageResult<()> {
        tokio::fs::create_dir_all(self.paths.staging_dir()).await?;
        Ok(())
    }

    /// Clears the entire staging subtree. This is the only cleanup entry point: there is no
    /// online reconciliation, because staging files are not tracked in the cache index or any
    /// other server-side state. Callers (currently `StorageServerBuilder::bind_prepared`) must
    /// run `wipe` before accepting traffic so a client that reopens a staging key from a
    /// previous process cannot observe stale bytes.
    pub async fn wipe(&self) -> StorageResult<()> {
        let root = self.paths.staging_dir();
        match tokio::fs::remove_dir_all(&root).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StorageError::io(
                    format!("failed to wipe staging root {}", root.display()),
                    error,
                ));
            }
        }
        self.prepare_dirs().await
    }

    /// Creates an empty staging file for `key` and returns its absolute path.
    ///
    /// Uses `O_CREAT | O_EXCL` so a concurrent or duplicate `StageCreate` on the same key
    /// surfaces as an explicit `AlreadyExists` error rather than silently clobbering an
    /// in-progress write. The documented client contract is "one writer per key at a time";
    /// this turns that contract into a server-enforced invariant.
    pub async fn create(&self, key: &ObjectLocation) -> StorageResult<PathBuf> {
        let path = self.paths.path_for(key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(_file) => {
                info!(key = %key, path = %path.display(), "staging file created");
                Ok(path)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(StorageError::busy(format!(
                    "staging file for {key} already exists; abort or commit the existing staging before re-creating"
                )))
            }
            Err(error) => Err(StorageError::io(
                format!("failed to create staging file {}", path.display()),
                error,
            )),
        }
    }

    /// Uploads the staging file for `key` via `store.put_from_file`.
    ///
    /// **Success** unlinks the staging file: the bytes now live in the backend, so the local
    /// copy is redundant and leaving it around would only get in the way of the next
    /// `StageCreate` for the same key.
    ///
    /// **Failure** leaves the staging file on disk. Upload failures are frequently transient
    /// (network errors, throttling), and the staged bytes may be GB-scale — forcing the client
    /// to rewrite the file from scratch on every retry is unacceptable for the database
    /// transaction workflow this API is built for. The client decides:
    /// * call `commit` again (optionally after a backoff), or
    /// * call `abort` to discard the staged bytes and start over.
    ///
    /// Crashed clients that never do either eventually get their staging file removed by the
    /// next startup `wipe()`.
    pub async fn commit(
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
        info!(key = %key, size, "staging file committed");

        if let Err(error) = tokio::fs::remove_file(&path).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            // Upload already succeeded; the cleanup failure is best-effort diagnostic data.
            // The next startup `wipe()` will remove the file.
            tracing::warn!(
                key = %key,
                path = %path.display(),
                error = %error,
                "failed to remove staging file after successful commit; relying on startup wipe",
            );
        }

        Ok(info)
    }

    /// Unlinks the staging file for `key`. Missing files are treated as success so repeated
    /// aborts (from retries, duplicate client paths) are idempotent.
    pub async fn abort(&self, key: &ObjectLocation) -> StorageResult<()> {
        let path = self.paths.path_for(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                info!(key = %key, "staging file aborted");
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::io(
                format!("failed to remove staging file {}", path.display()),
                error,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::backend::{MemoryObjectBackend, ObjectBackend, StoreRegistry};

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

    async fn write_staging_bytes(path: &std::path::Path, data: &[u8]) {
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .await
            .unwrap();
        file.write_all(data).await.unwrap();
        file.flush().await.unwrap();
    }

    fn test_key(key: &str) -> ObjectLocation {
        ObjectLocation::new(TEST_STORE_ID, "bucket", key).unwrap()
    }

    #[tokio::test]
    async fn create_produces_empty_file_on_disk() {
        let staging = StagingArea::new(test_root("create"));
        staging.prepare_dirs().await.unwrap();

        let key = test_key("file.txt");
        let path = staging.create(&key).await.unwrap();

        assert!(tokio::fs::try_exists(&path).await.unwrap());
        let metadata = tokio::fs::metadata(&path).await.unwrap();
        assert_eq!(metadata.len(), 0);
    }

    #[tokio::test]
    async fn create_twice_for_same_key_returns_busy() {
        let staging = StagingArea::new(test_root("exclusive"));
        staging.prepare_dirs().await.unwrap();

        let key = test_key("exclusive");
        let _first = staging.create(&key).await.unwrap();
        let error = staging.create(&key).await.unwrap_err();

        assert!(
            matches!(error, StorageError::Busy { .. }),
            "expected busy, got {error:?}"
        );
    }

    #[tokio::test]
    async fn commit_uploads_bytes_and_unlinks_staging_file() {
        let staging = StagingArea::new(test_root("commit"));
        staging.prepare_dirs().await.unwrap();

        let key = test_key("commit.txt");
        let path = staging.create(&key).await.unwrap();
        write_staging_bytes(&path, b"hello commit").await;

        let registry = StoreRegistry::new();
        let backend = MemoryObjectBackend::new();
        registry
            .register_shared_backend(
                TEST_STORE_ID,
                std::sync::Arc::new(backend.clone()),
            )
            .unwrap();
        let store = registry.resolve(key.store_id()).unwrap();

        let info = staging.commit(&key, &store).await.unwrap();

        assert_eq!(info.size, b"hello commit".len() as u64);
        assert!(!tokio::fs::try_exists(&path).await.unwrap());
        let readback = backend.get_range(&key, 0..info.size).await.unwrap();
        assert_eq!(&readback[..], b"hello commit");
    }

    #[tokio::test]
    async fn commit_preserves_staging_file_on_upload_failure_so_client_can_retry() {
        // Commit failures on GB-scale staging files must not force the client to rewrite the
        // whole staging file. The staging bytes stay on disk until the client decides to retry
        // commit or explicitly abort.
        use crate::backend::ObjectBackend;
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

        let staging = StagingArea::new(test_root("commit-retry"));
        staging.prepare_dirs().await.unwrap();
        let key = test_key("retry.txt");
        let path = staging.create(&key).await.unwrap();
        write_staging_bytes(&path, b"retry-me").await;

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

        let first = staging.commit(&key, &store).await.unwrap_err();
        assert!(
            matches!(first, StorageError::Backend { .. }),
            "first attempt must bubble backend failure"
        );
        assert!(
            tokio::fs::try_exists(&path).await.unwrap(),
            "staging file must survive a failed commit so the client can retry without re-writing bytes",
        );

        let info = staging.commit(&key, &store).await.unwrap();
        assert_eq!(info.size, b"retry-me".len() as u64);
        assert!(
            !tokio::fs::try_exists(&path).await.unwrap(),
            "successful retry must unlink the staging file"
        );

        let readback = backend.get_range(&key, 0..info.size).await.unwrap();
        assert_eq!(&readback[..], b"retry-me");
    }

    #[tokio::test]
    async fn commit_failure_followed_by_abort_removes_staging_file() {
        // Client that decides not to retry after a commit failure can always fall back to abort.
        use crate::backend::ObjectBackend;
        use async_trait::async_trait;
        use std::ops::Range;
        use std::sync::Arc;

        struct AlwaysFailBackend;

        #[async_trait]
        impl ObjectBackend for AlwaysFailBackend {
            async fn head(&self, _key: &ObjectLocation) -> StorageResult<ObjectInfo> {
                Ok(ObjectInfo {
                    size: 0,
                    etag: None,
                })
            }
            async fn get_range(
                &self,
                _key: &ObjectLocation,
                _range: Range<u64>,
            ) -> StorageResult<bytes::Bytes> {
                Ok(bytes::Bytes::new())
            }
            async fn put_from_file(
                &self,
                _key: &ObjectLocation,
                _path: &std::path::Path,
                _len: u64,
            ) -> StorageResult<ObjectInfo> {
                Err(StorageError::backend("permanent failure"))
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
                unreachable!("AlwaysFailBackend does not participate in list")
            }
            async fn delete(&self, _key: &ObjectLocation) -> StorageResult<()> {
                unreachable!("AlwaysFailBackend does not participate in delete")
            }
            fn delete_stream(
                &self,
                _store_id: &str,
                _bucket: &str,
                _keys: futures::stream::BoxStream<'static, StorageResult<String>>,
            ) -> futures::stream::BoxStream<'static, StorageResult<String>>
            {
                unreachable!(
                    "AlwaysFailBackend does not participate in delete_stream"
                )
            }
        }

        let staging = StagingArea::new(test_root("commit-fail-abort"));
        staging.prepare_dirs().await.unwrap();
        let key = test_key("give-up.txt");
        let path = staging.create(&key).await.unwrap();
        write_staging_bytes(&path, b"give up").await;

        let registry = StoreRegistry::new();
        registry
            .register_shared_backend(TEST_STORE_ID, Arc::new(AlwaysFailBackend))
            .unwrap();
        let store = registry.resolve(key.store_id()).unwrap();

        let _ = staging.commit(&key, &store).await.unwrap_err();
        assert!(
            tokio::fs::try_exists(&path).await.unwrap(),
            "commit failure must leave staging file for client to decide"
        );

        staging.abort(&key).await.unwrap();
        assert!(!tokio::fs::try_exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn abort_is_idempotent_when_staging_file_is_missing() {
        let staging = StagingArea::new(test_root("abort"));
        staging.prepare_dirs().await.unwrap();
        let key = test_key("never-created");

        // Called against a non-existent file, abort still returns Ok.
        staging.abort(&key).await.unwrap();

        // And calling it twice in a row also succeeds.
        let created = staging.create(&key).await.unwrap();
        staging.abort(&key).await.unwrap();
        assert!(!tokio::fs::try_exists(&created).await.unwrap());
        staging.abort(&key).await.unwrap();
    }

    #[tokio::test]
    async fn wipe_removes_all_staging_files_and_recreates_root() {
        let staging = StagingArea::new(test_root("wipe"));
        staging.prepare_dirs().await.unwrap();

        let key_one = test_key("wipe/a.txt");
        let key_two = test_key("wipe/b.txt");
        let path_one = staging.create(&key_one).await.unwrap();
        let path_two = staging.create(&key_two).await.unwrap();
        write_staging_bytes(&path_one, b"one").await;
        write_staging_bytes(&path_two, b"two").await;

        staging.wipe().await.unwrap();

        assert!(!tokio::fs::try_exists(&path_one).await.unwrap());
        assert!(!tokio::fs::try_exists(&path_two).await.unwrap());
        // Root itself is preserved so subsequent `create` calls do not need to recreate it.
        assert!(
            tokio::fs::try_exists(staging.paths().staging_dir())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn wipe_is_safe_when_no_staging_files_exist_yet() {
        let staging = StagingArea::new(test_root("wipe-empty"));
        // Don't prepare_dirs() — simulate first boot with an empty tree.
        staging.wipe().await.unwrap();
        assert!(
            tokio::fs::try_exists(staging.paths().staging_dir())
                .await
                .unwrap()
        );
    }
}
