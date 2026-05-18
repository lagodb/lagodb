use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, pg_sys};
use std::panic::AssertUnwindSafe;

impl PgWrapper {
    pub(crate) fn table_open(
        relation_id: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<pg_sys::Relation, PgError> {
        unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::table_open(relation_id, lockmode)))
                .catch_others(|err| Err(PgError::from_caught(err)))
                .execute()
        }
    }

    /// # Safety
    ///
    /// `relation` must be a valid relation opened with the matching lock mode.
    pub(crate) unsafe fn table_close(
        relation: pg_sys::Relation,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(relation);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::table_close(*relation, lockmode);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Check if a relation needs WAL logging.
    ///
    /// This is equivalent to PostgreSQL's `RelationNeedsWAL(rel)` macro.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `rel` is a valid pointer to a RelationData.
    pub(crate) unsafe fn relation_needs_wal(rel: pg_sys::Relation) -> bool {
        unsafe {
            let rd_rel = (*rel).rd_rel;
            if (*rd_rel).relpersistence != pg_sys::RELPERSISTENCE_PERMANENT as i8 {
                return false;
            }

            if Self::xlog_is_needed() {
                return true;
            }

            (*rel).rd_createSubid == 0 && (*rel).rd_firstRelfilelocatorSubid == 0
        }
    }
}
