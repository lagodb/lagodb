use std::ffi::CStr;
use std::ptr::null_mut;

use pgrx::pg_sys::{FileTag, ForkNumber, SyncRequestHandler, SyncRequestType};
use pgrx::{PgTryBuilder, pg_sys};

use super::PgWrapper;
use crate::diag::PgError;
use crate::handles::RelFileLocator;

impl PgWrapper {
    pub(crate) fn relation_path(locator: RelFileLocator, backend: i32) -> String {
        // SAFETY: GetRelationPath only formats the supplied storage identity
        // and returns an owned palloc string.
        unsafe {
            let pointer = pg_sys::GetRelationPath(
                locator.db_oid,
                locator.spc_oid,
                locator.rel_number,
                backend,
                ForkNumber::MAIN_FORKNUM,
            );
            let path = CStr::from_ptr(pointer).to_string_lossy().into_owned();
            pg_sys::pfree(pointer.cast());
            path
        }
    }

    pub(crate) fn allocate_relation_locator(
        tablespace: pg_sys::Oid,
        persistence: u8,
    ) -> Result<RelFileLocator, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let rel_number = pg_sys::GetNewRelFileNumber(
                    tablespace,
                    null_mut(),
                    persistence as _,
                );
                Ok(RelFileLocator {
                    spc_oid: tablespace,
                    db_oid: pg_sys::MyDatabaseId,
                    rel_number,
                })
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }

    pub(crate) fn create_relation_storage(
        locator: RelFileLocator,
        persistence: u8,
    ) -> Result<(), PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let storage = pg_sys::RelationCreateStorage(
                    pg_sys::RelFileLocator {
                        spcOid: locator.spc_oid,
                        dbOid: locator.db_oid,
                        relNumber: locator.rel_number,
                    },
                    persistence as _,
                    true,
                );
                pg_sys::smgrclose(storage);
                Ok(())
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }

    pub(crate) fn defer_relation_main_fork_unlink(
        locator: RelFileLocator,
    ) -> Result<(), PgError> {
        // SAFETY: the tag describes a non-temporary main-fork segment zero,
        // exactly as md.c's register_unlink_segment does. RegisterSyncRequest
        // copies it before returning; retryOnError ensures the request is queued.
        unsafe {
            PgTryBuilder::new(move || {
                let tag = FileTag {
                    handler: SyncRequestHandler::SYNC_HANDLER_MD as i16,
                    forknum: ForkNumber::MAIN_FORKNUM as i16,
                    rlocator: pg_sys::RelFileLocator {
                        spcOid: locator.spc_oid,
                        dbOid: locator.db_oid,
                        relNumber: locator.rel_number,
                    },
                    segno: 0,
                };
                pg_sys::RegisterSyncRequest(
                    &tag,
                    SyncRequestType::SYNC_UNLINK_REQUEST,
                    true,
                );
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
