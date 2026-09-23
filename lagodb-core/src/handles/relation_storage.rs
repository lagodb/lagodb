use super::{RelFileLocator, RelationHandle};
use crate::diag::PgError;
use crate::wrapper::PgWrapper;

impl RelFileLocator {
    /// Main-fork path, including PostgreSQL's temporary-backend prefix.
    pub fn path(self, backend: i32) -> String {
        PgWrapper::relation_path(self, backend)
    }

    /// Create the native main fork and register PostgreSQL abort cleanup.
    pub fn create_storage(self, persistence: u8) -> Result<(), PgError> {
        PgWrapper::create_relation_storage(self, persistence)
    }

    /// Keep a non-temporary main-fork file until a safe checkpoint completes.
    ///
    /// The caller must flush all deletion WAL for the retired generation before
    /// calling this method. PostgreSQL's MD handler then unlinks the file only
    /// after a checkpoint whose REDO point excludes those records. Temporary
    /// files have backend-specific paths and must be removed directly instead.
    pub fn defer_main_fork_unlink(self) -> Result<(), PgError> {
        PgWrapper::defer_relation_main_fork_unlink(self)
    }
}

impl RelationHandle<'_> {
    pub fn persistence(&self) -> u8 {
        // SAFETY: this handle owns a live relcache reference.
        unsafe { (*(*self.as_raw()).rd_rel).relpersistence as u8 }
    }

    pub fn storage_backend(&self) -> i32 {
        // SAFETY: PostgreSQL sets rd_backend for permanent and temporary
        // relations, including storage-less partitioned relations.
        unsafe { (*self.as_raw()).rd_backend }
    }

    /// Allocate a storage identity for an AM-owned logical relation.
    pub fn allocate_storage_locator(&self) -> Result<RelFileLocator, PgError> {
        PgWrapper::allocate_relation_locator(
            self.tablespace().resolved_oid(),
            self.persistence(),
        )
    }
}
