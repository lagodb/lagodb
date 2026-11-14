//! PostgreSQL-backend-local access to the runtime storage service.
//!
//! A backend caches one healthy foreground connection for each active managed
//! volume, foreign identity, or caller-owned configured generation. Every
//! socket is attached to one physical storage context before object operations begin.
//!
//! The cached socket is deliberately backend-scoped rather than transaction-
//! or `ResourceOwner`-scoped: PostgreSQL error unwinding drops open Rust file
//! objects, while a healthy connection remains reusable after abort. A local
//! transport/protocol failure poisons and closes it immediately; the next
//! replay-safe service operation attaches a replacement connection.

use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::storage::foreign::StorageIdentity;
use lagodb_storage::{
    ListSession, ObjectInfo, StagingFile, StagingPathResolver, StorageClient,
    StorageError, StorageFile, StorageProbeResult, StorageResult, StoreConfig,
    UploadInfo,
};

use super::StorageEndpoint;
use super::connection::{
    BackendAttach, BackendAttachedContext, BackendContextKey,
    PostgresExternalFdPolicy, acquire_attached_client, attached_context,
    configured_context,
};
use super::injection_points::StorageServiceInjectionPoints;

/// Cloneable PostgreSQL-facing storage service handle.
///
/// Clones share one backend-local attached context. Foreign and fresh configured
/// contexts are weakly interned, so their owners define socket/config lifetime.
/// Managed contexts remain strongly cached by their bounded volume ID. Every
/// kind of socket participates in the same bounded idle cache.
#[derive(Clone)]
pub struct BackendStorageService {
    context: Rc<BackendAttachedContext>,
}

// `Rc` intentionally keeps this general PostgreSQL-facing service `!Send +
// !Sync`. Upstream storage traits must adapt it at their private host boundary;
// core callers cannot move the service, its clients, or their Drop cleanup away
// from the owning backend thread.

impl fmt::Debug for BackendStorageService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendStorageService")
            .field("context_key", &self.context.key())
            .finish()
    }
}

impl BackendStorageService {
    pub fn for_managed(
        endpoint: &StorageEndpoint,
        volume_id: u64,
    ) -> StorageResult<Self> {
        let context = attached_context(
            endpoint.socket_path(),
            BackendContextKey::Managed(volume_id),
            BackendAttach::Managed(volume_id),
            endpoint.max_idle_connections(),
        )?;
        Ok(Self { context })
    }

    pub(crate) fn for_foreign(
        endpoint: &StorageEndpoint,
        identity: StorageIdentity,
        config: Arc<StoreConfig>,
    ) -> StorageResult<Self> {
        let context = attached_context(
            endpoint.socket_path(),
            BackendContextKey::Foreign(identity),
            BackendAttach::Configured(config),
            endpoint.max_idle_connections(),
        )?;
        Ok(Self { context })
    }

    /// Creates a fresh backend-local context for caller-owned credentials.
    ///
    /// Each call receives a new generation and never reuses a prior configured
    /// connection. The context and its credentials live exactly as long as the
    /// returned service and its clones.
    pub fn for_configured(
        endpoint: &StorageEndpoint,
        config: Arc<StoreConfig>,
    ) -> StorageResult<Self> {
        let context = configured_context(
            endpoint.socket_path(),
            config,
            endpoint.max_idle_connections(),
        )?;
        Ok(Self { context })
    }

    pub fn open(&self, bucket: &str, key: &str) -> StorageResult<StorageFile> {
        self.with_replay_safe_client(|client| {
            StorageServiceInjectionPoints::FOREGROUND_BEFORE_OPEN.run();
            client.open(bucket, key)
        })
    }

    pub fn head(&self, bucket: &str, key: &str) -> StorageResult<ObjectInfo> {
        self.with_replay_safe_client(|client| client.head(bucket, key))
    }

    pub fn upload(&self, bucket: &str, key: &str) -> StorageResult<UploadInfo> {
        // Upload is deliberately neither retried nor classified as Ambiguous. A missing or
        // unsuccessful reply fails the current database operation and transaction, even when a
        // lost reply follows successful remote publication. Raw-file writes do not promise to
        // roll back that external side effect, and no caller branches on a distinct uncertain
        // outcome. Keeping the original error is therefore the complete caller contract.
        self.with_client(|client| client.upload(bucket, key))
    }

    pub fn invalidate_object_cache(
        &self,
        bucket: &str,
        key: &str,
    ) -> StorageResult<bool> {
        self.with_client(|client| client.invalidate_object_cache(bucket, key))
    }

    pub fn probe_store(
        &self,
        bucket: &str,
        root_prefix: &str,
    ) -> StorageResult<StorageProbeResult> {
        self.with_client(|client| client.probe_store(bucket, root_prefix))
    }

    pub fn create_staging_file(
        &self,
        resolver: &StagingPathResolver,
        bucket: &str,
        key: &str,
    ) -> StorageResult<StagingFile> {
        let client = self.acquire_client()?;
        StagingFile::create_with_fd_policy(
            resolver,
            client.backend_identity(),
            bucket,
            key,
            &PostgresExternalFdPolicy,
        )
    }

    pub fn object_location(
        &self,
        bucket: &str,
        key: &str,
    ) -> StorageResult<lagodb_storage::ObjectLocation> {
        let client = self.acquire_client()?;
        lagodb_storage::ObjectLocation::new(
            client.backend_identity().clone(),
            bucket,
            key,
        )
    }

    pub fn delete(&self, bucket: &str, key: &str) -> StorageResult<()> {
        self.with_non_replayable_client("delete", |client| client.delete(bucket, key))
    }

    /// Deletes an object from a transaction-end callback; see
    /// [`Self::cleanup_client`].
    pub fn delete_cleanup(&self, bucket: &str, key: &str) -> StorageResult<()> {
        self.cleanup_client()?.delete_cleanup(bucket, key)
    }

    /// Deletes every object under `prefix` from a transaction-end callback;
    /// see [`Self::cleanup_client`]. When a prefix cannot finish within the
    /// cleanup timeout, how many of its objects were removed is unknown.
    pub fn delete_prefix_cleanup(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> StorageResult<u64> {
        self.cleanup_client()?.delete_prefix_cleanup(bucket, prefix)
    }

    pub fn delete_prefix(&self, bucket: &str, prefix: &str) -> StorageResult<u64> {
        self.with_non_replayable_client("delete prefix", |client| {
            client.delete_prefix(bucket, prefix)
        })
    }

    pub fn list_session(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        page_size: u32,
    ) -> StorageResult<ListSession> {
        let client = self.acquire_client()?;
        Ok(client.list_session(bucket, prefix, page_size))
    }

    fn with_client<T>(
        &self,
        operation: impl FnOnce(&StorageClient) -> StorageResult<T>,
    ) -> StorageResult<T> {
        let client = self.acquire_client()?;
        operation(&client)
    }

    /// The connection for a transaction-end callback, which PostgreSQL runs
    /// with interrupts held.
    ///
    /// As postgres_fdw's abort cleanup does, cleanup uses only this context's
    /// established connection, whose cleanup timeout bounds each request. It
    /// never connects or attaches, because neither is bounded by that timeout.
    /// No connection is usable when an earlier failure or cleanup timeout
    /// poisoned it, or when the idle cache
    /// (`lagodb.storage_backend_max_idle_connections`) reclaimed this context's
    /// healthy connection for another context. The cleanup then fails with an
    /// error the caller reports as a warning, and its objects remain behind.
    fn cleanup_client(&self) -> StorageResult<StorageClient> {
        self.context.reusable_client()?.ok_or_else(|| {
            StorageError::protocol(
                "transaction cleanup has no established storage connection",
            )
        })
    }

    /// Runs an operation whose externally visible result cannot be inferred
    /// after the request connection fails. The operation is never replayed.
    fn with_non_replayable_client<T>(
        &self,
        operation_name: &'static str,
        operation: impl FnOnce(&StorageClient) -> StorageResult<T>,
    ) -> StorageResult<T> {
        let client = self.acquire_client()?;
        match operation(&client) {
            Err(error) if !client.is_usable() => {
                Err(StorageError::ambiguous(operation_name, error.to_string()))
            }
            result => result,
        }
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
        acquire_attached_client(&self.context)
    }
}
