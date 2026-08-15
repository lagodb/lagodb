//! Provider-facing storage operations.

use std::rc::Rc;

use pg_lakebase_storage::{
    ListSession, ObjectInfo, StagingFile, StagingPathResolver, StorageFile,
    StorageResult, UploadInfo,
};
use pgrx::pg_sys;

use super::cache::StorageCacheEntry;
use super::identity::StorageIdentity;

/// Provider-facing handle for one operation-stable storage context.
///
/// Providers use typed object operations and never see storage connection or
/// attach details.
#[derive(Clone)]
pub struct StorageHandle {
    entry: Rc<StorageCacheEntry>,
}

impl StorageHandle {
    pub(crate) fn new(entry: Rc<StorageCacheEntry>) -> Self {
        Self { entry }
    }

    pub fn identity(&self) -> &StorageIdentity {
        &self.entry.identity
    }

    pub fn umid(&self) -> pg_sys::Oid {
        self.entry.umid
    }

    /// Opens an object for reading.
    ///
    /// OPEN itself may reconnect before a handle exists. Once returned, the
    /// [`StorageFile`] owns one server-side handle and READ failures are returned
    /// to the provider without reopening the key.
    pub fn open(&self, bucket: &str, key: &str) -> StorageResult<StorageFile> {
        self.entry.service.open(bucket, key)
    }

    /// Fetches object metadata.
    ///
    /// A transport failure during HEAD causes one reconnect, reattach,
    /// and replay. A server-reported object error is returned unchanged.
    pub fn head(&self, bucket: &str, key: &str) -> StorageResult<ObjectInfo> {
        self.entry.service.head(bucket, key)
    }

    /// Uploads the caller-owned staging file to the backend.
    ///
    /// Upload does not invalidate a cache residency for the same physical key.
    pub fn upload(&self, bucket: &str, key: &str) -> StorageResult<UploadInfo> {
        self.entry.service.upload(bucket, key)
    }

    /// Explicitly invalidates the caller-identified physical object.
    ///
    /// The storage cache does not detect same-key remote overwrites. A caller
    /// must invoke this operation when it knows that the object version
    /// changed; ordinary reads intentionally do not perform freshness probes.
    /// The request is not replayed after a connection failure.
    pub fn invalidate_object_cache(
        &self,
        bucket: &str,
        key: &str,
    ) -> StorageResult<bool> {
        self.entry.service.invalidate_object_cache(bucket, key)
    }

    pub fn create_staging_file(
        &self,
        resolver: &StagingPathResolver,
        bucket: &str,
        key: &str,
    ) -> StorageResult<StagingFile> {
        self.entry
            .service
            .create_staging_file(resolver, bucket, key)
    }

    /// Deletes one object.
    ///
    /// DELETE is not replayed after a transport failure because the remote
    /// result may already be committed. The returned error is typed as
    /// ambiguous so the provider can reconcile the object state explicitly.
    pub fn delete(&self, bucket: &str, key: &str) -> StorageResult<()> {
        self.entry.service.delete(bucket, key)
    }

    /// Starts a listing pinned to one storage connection generation.
    pub(crate) fn list_session(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        page_size: u32,
    ) -> StorageResult<ListSession> {
        self.entry.service.list_session(bucket, prefix, page_size)
    }
}
