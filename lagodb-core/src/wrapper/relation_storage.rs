use std::ffi::CStr;

use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::pg_sys::{ForkNumber, RelFileLocator as PgRelFileLocator};
use pgrx::{PgTryBuilder, pg_sys};

use super::PgWrapper;
use crate::diag::PgError;
use crate::handles::RelFileLocator;

// Manually declare log_smgrcreate because catalog/storage_xlog.h is not in
// pg_sys.
unsafe extern "C-unwind" {
    fn log_smgrcreate(
        rlocator: *const PgRelFileLocator,
        fork_number: ForkNumber::Type,
    );
}

impl PgWrapper {
    pub(crate) fn relation_path(locator: RelFileLocator, backend: i32) -> String {
        // SAFETY: GetRelationPath only formats the supplied storage identity.
        // PG17 returns a palloc string; PG18 returns an inline RelPathStr.
        unsafe {
            let path = pg_sys::GetRelationPath(
                locator.db_oid,
                locator.spc_oid,
                locator.rel_number,
                backend,
                ForkNumber::MAIN_FORKNUM,
            );
            #[cfg(any(feature = "pg16", feature = "pg17"))]
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

    pub(crate) fn create_relation_storage(
        locator: RelFileLocator,
        persistence: u8,
    ) -> Result<(), PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let pg_locator = PgRelFileLocator {
                    spcOid: locator.spc_oid,
                    dbOid: locator.db_oid,
                    relNumber: locator.rel_number,
                };
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

    pub(crate) fn make_catalog_changes_visible() -> Result<(), PgError> {
        unsafe {
            PgTryBuilder::new(|| {
                pg_sys::CommandCounterIncrement();
                Ok(())
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }
}
