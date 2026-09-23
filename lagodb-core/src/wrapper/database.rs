use std::ffi::CStr;

use pgrx::{PgTryBuilder, pg_sys};

use super::PgWrapper;
use crate::diag::PgError;

impl PgWrapper {
    pub(crate) fn lock_database(
        database_oid: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<(), PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::LockSharedObject(
                    pg_sys::DatabaseRelationId,
                    database_oid,
                    0,
                    lockmode,
                );
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    pub(crate) fn unlock_database(
        database_oid: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) {
        // SAFETY: callers release the same transaction-level database object
        // lock and mode acquired immediately before identity revalidation.
        unsafe {
            pg_sys::UnlockSharedObject(
                pg_sys::DatabaseRelationId,
                database_oid,
                0,
                lockmode,
            );
        }
    }

    pub(crate) fn database_path(
        database_oid: pg_sys::Oid,
        tablespace_oid: pg_sys::Oid,
    ) -> String {
        unsafe {
            let ptr = pg_sys::GetDatabasePath(database_oid, tablespace_oid);
            let path = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            pg_sys::pfree(ptr.cast());
            path
        }
    }
}
