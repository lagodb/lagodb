use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, pg_sys};
use std::panic::AssertUnwindSafe;

unsafe extern "C-unwind" {
    fn find_all_inheritors(
        parent_rel_id: pg_sys::Oid,
        lock_mode: i32,
        numparents: *mut *mut pg_sys::List,
    ) -> *mut pg_sys::List;
    fn find_inheritance_children(
        parent_rel_id: pg_sys::Oid,
        lock_mode: i32,
    ) -> *mut pg_sys::List;
    fn has_superclass(relation_id: pg_sys::Oid) -> bool;
}

impl PgWrapper {
    pub(crate) fn relation_open(
        relation_id: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<pg_sys::Relation, PgError> {
        PgTryBuilder::new(move || {
            Ok(unsafe { pg_sys::relation_open(relation_id, lockmode) })
        })
        .catch_others(|err| Err(PgError::from_caught(err)))
        .execute()
    }

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
    pub(crate) unsafe fn relation_close(
        relation: pg_sys::Relation,
        lockmode: pg_sys::LOCKMODE,
    ) {
        unsafe { pg_sys::relation_close(relation, lockmode) }
    }

    /// # Safety
    ///
    /// `relation` must be a live PostgreSQL relation in the current transaction.
    pub(crate) unsafe fn invalidate_relation_cache(
        relation: pg_sys::Relation,
    ) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(relation);
        // SAFETY: the caller keeps the relation alive throughout this call.
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CacheInvalidateRelcache(*relation);
                Ok(())
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
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

    pub(crate) unsafe fn alter_table_lookup_relation(
        stmt: *mut pg_sys::AlterTableStmt,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<pg_sys::Oid, PgError> {
        unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(pg_sys::AlterTableLookupRelation(stmt, lockmode))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }

    pub(crate) fn has_inheritance_children(
        parent_rel_id: pg_sys::Oid,
    ) -> Result<bool, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let children = pg_sys::ffi::pg_guard_ffi_boundary(|| {
                    find_inheritance_children(parent_rel_id, pg_sys::NoLock as _)
                });
                let has_children = !children.is_null();
                pg_sys::list_free(children);
                Ok(has_children)
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    pub(crate) fn has_inheritance_parents(
        relation_id: pg_sys::Oid,
    ) -> Result<bool, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::ffi::pg_guard_ffi_boundary(|| {
                    has_superclass(relation_id)
                }))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    pub(crate) fn find_all_inheritors(
        parent_rel_id: pg_sys::Oid,
        lockmode: pg_sys::LOCKMODE,
    ) -> Result<Vec<pg_sys::Oid>, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let relations = pg_sys::ffi::pg_guard_ffi_boundary(|| {
                    find_all_inheritors(parent_rel_id, lockmode, std::ptr::null_mut())
                });
                let len = pg_sys::list_length(relations);
                let mut result = Vec::with_capacity(len as usize);
                for index in 0..len {
                    result.push(pg_sys::list_nth_oid(relations, index));
                }
                pg_sys::list_free(relations);
                Ok(result)
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Return the extended-statistics object OIDs cached for a relation.
    ///
    /// # Safety
    ///
    /// `relation` must remain valid for the duration of the call.
    #[cfg(feature = "pg16")]
    pub(crate) unsafe fn relation_stat_ext_oids(
        relation: pg_sys::Relation,
    ) -> Result<Vec<pg_sys::Oid>, PgError> {
        let relation = AssertUnwindSafe(relation);
        unsafe {
            PgTryBuilder::new(move || {
                let statistics = pg_sys::RelationGetStatExtList(*relation);
                let len = pg_sys::list_length(statistics);
                let mut result = Vec::with_capacity(len as usize);
                for index in 0..len {
                    result.push(pg_sys::list_nth_oid(statistics, index));
                }
                pg_sys::list_free(statistics);
                Ok(result)
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}
