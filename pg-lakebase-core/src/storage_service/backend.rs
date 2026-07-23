//! PostgreSQL-backend-local access to the storage service.
//!
//! A backend uses one healthy foreground connection generation for every
//! distributed tablespace. Object identity remains request-local; the socket
//! is bound only to the cluster-local storage service endpoint.
//!
//! The cached socket is deliberately backend-scoped rather than transaction-
//! or `ResourceOwner`-scoped: PostgreSQL error unwinding drops open Rust file
//! objects, while a healthy connection remains reusable after abort. A local
//! transport/protocol failure poisons and closes its generation immediately;
//! the next service operation installs a new generation.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pg_lakebase_storage::{
    ExternalFdLease, ExternalFdPolicy, ListCursor, ListPage, ObjectInfo, StagingFile,
    StagingPathResolver, StorageClient, StorageError, StorageFile, StorageResult,
    UploadInfo,
};
use pgrx::pg_sys;

use super::StorageEndpoint;

thread_local! {
    static FOREGROUND_CONNECTION: RefCell<BackendConnectionManager> =
        const { RefCell::new(BackendConnectionManager::new()) };
}

/// Cloneable PostgreSQL-facing storage service handle.
///
/// This type stores only the stable endpoint. Each operation acquires the
/// current healthy backend-local connection generation; open file handles stay
/// bound to the generation that issued their server-side handle.
#[derive(Clone)]
pub struct BackendStorageService {
    socket_path: Arc<PathBuf>,
}

impl std::fmt::Debug for BackendStorageService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendStorageService")
            .field("socket_path", &self.socket_path)
            .finish()
    }
}

impl BackendStorageService {
    pub fn from_endpoint(endpoint: &StorageEndpoint) -> Self {
        Self {
            socket_path: Arc::new(endpoint.socket_path().to_path_buf()),
        }
    }

    pub fn open(
        &self,
        store_id: &str,
        bucket: &str,
        key: &str,
    ) -> StorageResult<StorageFile> {
        self.with_replay_safe_client(|client| client.open(store_id, bucket, key))
    }

    pub fn head(
        &self,
        store_id: &str,
        bucket: &str,
        key: &str,
    ) -> StorageResult<ObjectInfo> {
        self.with_replay_safe_client(|client| client.head(store_id, bucket, key))
    }

    pub fn upload(
        &self,
        store_id: &str,
        bucket: &str,
        key: &str,
    ) -> StorageResult<UploadInfo> {
        self.with_client(|client| client.upload(store_id, bucket, key))
    }

    pub fn create_staging_file(
        &self,
        resolver: &StagingPathResolver,
        store_id: &str,
        bucket: &str,
        key: &str,
    ) -> StorageResult<StagingFile> {
        StagingFile::create_with_fd_policy(
            resolver,
            store_id,
            bucket,
            key,
            &PostgresExternalFdPolicy,
        )
    }

    pub fn delete(
        &self,
        store_id: &str,
        bucket: &str,
        key: &str,
    ) -> StorageResult<()> {
        self.with_replay_safe_client(|client| client.delete(store_id, bucket, key))
    }

    pub fn delete_prefix(
        &self,
        store_id: &str,
        bucket: &str,
        prefix: &str,
    ) -> StorageResult<u64> {
        self.with_client(|client| client.delete_prefix(store_id, bucket, prefix))
    }

    pub fn list_page(
        &self,
        store_id: &str,
        bucket: &str,
        prefix: Option<&str>,
        cursor: Option<ListCursor>,
        page_size: u32,
    ) -> StorageResult<ListPage> {
        self.with_client(|client| {
            client.list_page(store_id, bucket, prefix, cursor, page_size)
        })
    }

    fn with_client<T>(
        &self,
        operation: impl FnOnce(&StorageClient) -> StorageResult<T>,
    ) -> StorageResult<T> {
        let client = self.acquire_client()?;
        operation(&client)
    }

    /// Runs an operation that is safe to replay once after a connection
    /// generation fails. Callers must not use this for operations whose first
    /// attempt can leave an ambiguous externally visible result.
    fn with_replay_safe_client<T>(
        &self,
        mut operation: impl FnMut(&StorageClient) -> StorageResult<T>,
    ) -> StorageResult<T> {
        let client = self.acquire_client()?;
        match operation(&client) {
            Err(_) if !client.is_usable() => {
                let replacement = self.acquire_client()?;
                operation(&replacement)
            }
            result => result,
        }
    }

    fn acquire_client(&self) -> StorageResult<StorageClient> {
        FOREGROUND_CONNECTION.with(|manager| {
            manager
                .try_borrow_mut()
                .map_err(|_| {
                    StorageError::protocol(
                        "backend storage connection manager is already in use",
                    )
                })?
                .acquire(&self.socket_path)
        })
    }
}

struct BackendConnectionManager {
    current: Option<CachedConnection>,
}

impl BackendConnectionManager {
    const fn new() -> Self {
        Self { current: None }
    }

    fn acquire(&mut self, socket_path: &Path) -> StorageResult<StorageClient> {
        self.acquire_with(socket_path, |path| {
            StorageClient::connect_with_fd_policy(
                path,
                Box::new(PostgresExternalFdPolicy),
            )
        })
    }

    fn acquire_with(
        &mut self,
        socket_path: &Path,
        connect: impl FnOnce(&Path) -> StorageResult<StorageClient>,
    ) -> StorageResult<StorageClient> {
        if self.current.as_ref().is_some_and(|current| {
            current.socket_path == socket_path && current.client.is_usable()
        }) {
            return Ok(self
                .current
                .as_ref()
                .expect("healthy cached connection checked above")
                .client
                .clone());
        }

        if let Some(stale) = self.current.take() {
            let _ = stale.client.invalidate();
        }

        let client = connect(socket_path)?;
        self.current = Some(CachedConnection {
            socket_path: socket_path.to_path_buf(),
            client: client.clone(),
        });
        Ok(client)
    }
}

struct CachedConnection {
    socket_path: PathBuf,
    client: StorageClient,
}

struct PostgresExternalFdPolicy;

impl ExternalFdPolicy for PostgresExternalFdPolicy {
    fn acquire(&self) -> StorageResult<Box<dyn ExternalFdLease>> {
        // SAFETY: BackendStorageService is used only by the PostgreSQL backend
        // main thread. AcquireExternalFD updates backend-local fd.c accounting.
        if unsafe { pg_sys::AcquireExternalFD() } {
            Ok(Box::new(PostgresExternalFdLease))
        } else {
            Err(StorageError::resource_exhausted(
                "PostgreSQL external file descriptor budget exhausted",
            ))
        }
    }
}

struct PostgresExternalFdLease;

impl ExternalFdLease for PostgresExternalFdLease {}

impl Drop for PostgresExternalFdLease {
    fn drop(&mut self) {
        // SAFETY: every lease is created only after AcquireExternalFD
        // succeeds. BackendStorageService and the surrounding PostgreSQL
        // extension remain confined to the owning backend thread.
        unsafe {
            pg_sys::ReleaseExternalFD();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixListener;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn socket_path(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "pg-lakebase-{name}-{}-{stamp}.sock",
            std::process::id()
        ))
    }

    #[test]
    fn manager_reuses_one_healthy_connection_generation() {
        let path = socket_path("reuse");
        let listener = UnixListener::bind(&path).unwrap();
        let accept = std::thread::spawn(move || listener.accept().unwrap().0);

        let mut manager = BackendConnectionManager::new();
        let first = manager
            .acquire_with(&path, |path| StorageClient::connect(path))
            .unwrap();
        let _accepted = accept.join().unwrap();
        let second = manager
            .acquire_with(&path, |path| StorageClient::connect(path))
            .unwrap();

        first.invalidate().unwrap();
        assert!(!second.is_usable());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn manager_reconnects_after_generation_is_poisoned() {
        let path = socket_path("reconnect");
        let listener = UnixListener::bind(&path).unwrap();
        let accept = std::thread::spawn(move || {
            let first = listener.accept().unwrap().0;
            drop(first);
            listener.accept().unwrap().0
        });

        let mut manager = BackendConnectionManager::new();
        let first = manager
            .acquire_with(&path, |path| StorageClient::connect(path))
            .unwrap();
        assert!(first.head("store", "bucket", "key").is_err());
        assert!(!first.is_usable());
        let second = manager
            .acquire_with(&path, |path| StorageClient::connect(path))
            .unwrap();
        let _accepted = accept.join().unwrap();

        assert!(!first.is_usable());
        assert!(second.is_usable());
        std::fs::remove_file(path).unwrap();
    }
}
