use std::ffi::CStr;

use pgrx::pg_sys::ForkNumber;
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{PgTryBuilder, pg_sys};

use super::{PgRelFileLocator, RelFileLocator, RelationHandle};
use crate::diag::PgError;

// Manually declare log_smgrcreate because catalog/storage_xlog.h is not in
// pg_sys.
unsafe extern "C-unwind" {
    fn log_smgrcreate(
        rlocator: *const PgRelFileLocator,
        fork_number: ForkNumber::Type,
    );
}

impl RelFileLocator {
    /// Main-fork path, including PostgreSQL's temporary-backend prefix.
    pub fn path(self, backend: i32) -> String {
        // SAFETY: GetRelationPath only formats the supplied storage identity.
        // PG15-PG17 return a palloc string; PG18 returns an inline RelPathStr.
        unsafe {
            let path = pg_sys::GetRelationPath(
                self.db_oid,
                self.spc_oid,
                self.rel_number,
                backend,
                ForkNumber::MAIN_FORKNUM,
            );
            #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
            {
                let owned = CStr::from_ptr(path).to_string_lossy().into_owned();
                pg_sys::pfree(path.cast());
                owned
            }
            #[cfg(feature = "pg18")]
            {
                CStr::from_ptr(path.str_.as_ptr())
                    .to_string_lossy()
                    .into_owned()
            }
        }
    }

    /// Create the native main fork and register PostgreSQL abort cleanup.
    ///
    /// The fork is WAL-logged for permanent and UNLOGGED relations, so it
    /// survives crash recovery and reaches standbys. A provider must not rely
    /// on an init fork to restore it.
    pub fn create_storage(self, persistence: u8) -> Result<(), PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let pg_locator = self.to_raw();
                let storage =
                    pg_sys::RelationCreateStorage(pg_locator, persistence as _, true);
                pg_sys::smgrclose(storage);
                // PostgreSQL WAL-logs only a permanent main fork: an UNLOGGED
                // heap rebuilds its own from the init fork. Without one, this
                // record is what recreates the file after a crash or promotion.
                if persistence == pg_sys::RELPERSISTENCE_UNLOGGED {
                    pg_guard_ffi_boundary(|| {
                        log_smgrcreate(
                            &raw const pg_locator,
                            ForkNumber::MAIN_FORKNUM,
                        );
                    });
                }
                Ok(())
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
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
