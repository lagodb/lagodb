//! Object-bound storage access.
//!
//! [`ObjectAccess`] binds one authorized bucket/key to a configured storage
//! user mapping. It is an object capability, not a login, connection, or
//! transaction session. Providers receive raw storage primitives and own their
//! operation lifecycle.

use pg_lakebase_storage::{
    ListSession, ObjectInfo, StagingFile, StagingPathResolver, StorageError,
    StorageFile, StorageResult, UploadInfo,
};

use super::handle::StorageHandle;

/// Authorized access to one fixed object.
#[derive(Clone)]
pub struct ObjectAccess {
    store: StorageHandle,
    staging: StagingPathResolver,
    bucket: Box<str>,
    key: Box<str>,
}

/// Authorized access to one fixed object-key prefix.
///
/// Child object capabilities can only be created for keys inside the bound
/// prefix, so a connector cannot use a successfully checked storage scope to
/// escape into a sibling object key.
pub struct ObjectPrefixAccess {
    store: StorageHandle,
    staging: StagingPathResolver,
    bucket: Box<str>,
    prefix: Box<str>,
}

impl ObjectPrefixAccess {
    pub(crate) fn new(
        store: StorageHandle,
        staging: StagingPathResolver,
        bucket: &str,
        prefix: &str,
    ) -> Self {
        Self {
            store,
            staging,
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }

    /// Starts a connection-bound listing over this authorized prefix.
    pub fn listing(&self, page_size: u32) -> StorageResult<ListSession> {
        self.store.list_session(
            self.bucket.as_ref(),
            Some(self.prefix.as_ref()),
            page_size,
        )
    }

    pub fn object(&self, key: &str) -> StorageResult<ObjectAccess> {
        if !key.starts_with(self.prefix.as_ref()) {
            return Err(StorageError::invalid_path(
                "object key is outside the authorized prefix",
            ));
        }
        Ok(ObjectAccess::new(
            self.store.clone(),
            self.staging.clone(),
            self.bucket.as_ref(),
            key,
        ))
    }
}

impl ObjectAccess {
    pub(crate) fn new(
        store: StorageHandle,
        staging: StagingPathResolver,
        bucket: &str,
        key: &str,
    ) -> Self {
        Self {
            store,
            staging,
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    /// Opens the authorized object once. READ failures are not replayed because
    /// reopening a mutable key would establish a new residency.
    pub fn open(&self) -> StorageResult<StorageFile> {
        self.store.open(self.bucket.as_ref(), self.key.as_ref())
    }

    /// Returns metadata for the authorized object.
    pub fn head(&self) -> StorageResult<ObjectInfo> {
        self.store.head(self.bucket.as_ref(), self.key.as_ref())
    }

    /// Creates the caller-owned local staging file for this object.
    pub fn create_staging(&self) -> StorageResult<StagingFile> {
        self.store.create_staging_file(
            &self.staging,
            self.bucket.as_ref(),
            self.key.as_ref(),
        )
    }

    /// Uploads the staging file once without changing cache residency.
    pub fn upload(&self) -> StorageResult<UploadInfo> {
        self.store.upload(self.bucket.as_ref(), self.key.as_ref())
    }

    /// Deletes this authorized object.
    ///
    /// DELETE is deliberately exposed on the exact-object capability so a
    /// transaction resource can retain precisely the key created by a rolling
    /// writer without retaining broader prefix authority.
    pub fn delete(&self) -> StorageResult<()> {
        self.store.delete(self.bucket.as_ref(), self.key.as_ref())
    }

    /// Explicitly retires this object's cache residency.
    pub fn invalidate_cache(&self) -> StorageResult<bool> {
        self.store
            .invalidate_object_cache(self.bucket.as_ref(), self.key.as_ref())
    }
}
