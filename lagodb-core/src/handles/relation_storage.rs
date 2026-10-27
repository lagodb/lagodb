use super::{RelFileLocator, RelationHandle};
use crate::diag::PgError;
use crate::wrapper::PgWrapper;

impl RelFileLocator {
    /// Main-fork path, including PostgreSQL's temporary-backend prefix.
    pub fn path(self, backend: i32) -> String {
        PgWrapper::relation_path(self, backend)
    }

    /// Create the native main fork and register PostgreSQL abort cleanup.
    ///
    /// The fork is WAL-logged for permanent and UNLOGGED relations, so it
    /// survives crash recovery and reaches standbys. A provider must not rely
    /// on an init fork to restore it.
    pub fn create_storage(self, persistence: u8) -> Result<(), PgError> {
        PgWrapper::create_relation_storage(self, persistence)
    }
}

impl RelationHandle<'_> {
    pub fn persistence(&self) -> u8 {
        // SAFETY: this handle owns a live relcache reference.
        unsafe { (*(*self.as_raw()).rd_rel).relpersistence as u8 }
    }

    pub fn storage_backend(&self) -> i32 {
        // SAFETY: PostgreSQL sets rd_backend for permanent and temporary
        // relations.
        unsafe { (*self.as_raw()).rd_backend }
    }
}
