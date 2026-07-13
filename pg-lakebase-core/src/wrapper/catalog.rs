use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, pg_sys};
use std::panic::AssertUnwindSafe;

#[derive(Debug)]
struct CatalogIndexGuard {
    state: pg_sys::CatalogIndexState,
}

impl CatalogIndexGuard {
    /// Open catalog indexes for caller-owned update/index maintenance.
    ///
    /// Keeping this state outside the `CatalogTupleUpdateWithInfo` error
    /// boundary is deliberate: PostgreSQL reports `simple_heap_update`
    /// tuple-version conflicts with `ERROR`, and a caught ERROR skips the
    /// callee's normal control flow. If we used `CatalogTupleUpdate` directly,
    /// its internally-opened index state would not reach `CatalogCloseIndexes`.
    ///
    /// `CatalogTupleUpdateWithInfo` lets PostgreSQL still own catalog index
    /// insertion logic while this Rust guard owns the open/close lifecycle.
    unsafe fn open(relation: pg_sys::Relation) -> Result<Self, PgError> {
        let relation = AssertUnwindSafe(relation);
        let state = unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::CatalogOpenIndexes(*relation)))
                .catch_others(|err| Err(PgError::from_caught(err)))
                .execute()
        }?;

        Ok(Self { state })
    }

    #[inline]
    fn as_raw(&self) -> pg_sys::CatalogIndexState {
        self.state
    }
}

impl Drop for CatalogIndexGuard {
    fn drop(&mut self) {
        if self.state.is_null() {
            return;
        }

        let state = AssertUnwindSafe(self.state);
        let _ = unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogCloseIndexes(*state);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        };
    }
}

impl PgWrapper {
    /// Open all indexes belonging to a catalog relation for reuse across a
    /// bounded sequence of tuple writes.
    ///
    /// # Safety
    ///
    /// `relation` must be a valid, open catalog relation.
    pub(crate) unsafe fn catalog_open_indexes_raw(
        relation: pg_sys::Relation,
    ) -> Result<pg_sys::CatalogIndexState, PgError> {
        let relation = AssertUnwindSafe(relation);
        unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::CatalogOpenIndexes(*relation)))
                .catch_others(|err| Err(PgError::from_caught(err)))
                .execute()
        }
    }

    /// Close catalog index state returned by [`Self::catalog_open_indexes_raw`].
    ///
    /// # Safety
    ///
    /// `state` must be live catalog index state that has not already been closed.
    pub(crate) unsafe fn catalog_close_indexes_raw(
        state: pg_sys::CatalogIndexState,
    ) -> Result<(), PgError> {
        let state = AssertUnwindSafe(state);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogCloseIndexes(*state);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// # Safety
    ///
    /// `relation` and `tuple` must be valid PostgreSQL objects, and `tuple`
    /// must be suitable for insertion into `relation`.
    pub(crate) unsafe fn catalog_tuple_insert_raw(
        relation: pg_sys::Relation,
        tuple: pg_sys::HeapTuple,
    ) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(relation);
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleInsert(*relation, *tuple);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Insert one tuple while reusing caller-owned catalog index state.
    ///
    /// # Safety
    ///
    /// `relation`, `tuple`, and `index_state` must belong to the same live
    /// catalog relation.
    pub(crate) unsafe fn catalog_tuple_insert_with_info_raw(
        relation: pg_sys::Relation,
        tuple: pg_sys::HeapTuple,
        index_state: pg_sys::CatalogIndexState,
    ) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(relation);
        let tuple = AssertUnwindSafe(tuple);
        let index_state = AssertUnwindSafe(index_state);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleInsertWithInfo(*relation, *tuple, *index_state);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Update one catalog tuple while keeping catalog indexes current.
    ///
    /// This uses `CatalogTupleUpdateWithInfo` with caller-owned index state.
    /// PostgreSQL can report tuple-version conflicts from `simple_heap_update`
    /// as `ERROR`; keeping index state in `CatalogIndexGuard` ensures that a
    /// caught `ERROR` cannot skip `CatalogCloseIndexes`.
    ///
    /// # Safety
    ///
    /// `relation`, `otid`, and `tuple` must be valid PostgreSQL objects. `otid`
    /// must identify the existing catalog tuple version being replaced.
    pub(crate) unsafe fn catalog_tuple_update_raw(
        relation: pg_sys::Relation,
        otid: *mut pg_sys::ItemPointerData,
        tuple: pg_sys::HeapTuple,
    ) -> Result<(), PgError> {
        let index_guard = unsafe { CatalogIndexGuard::open(relation)? };
        let index_state = AssertUnwindSafe(index_guard.as_raw());
        let relation = AssertUnwindSafe(relation);
        let otid = AssertUnwindSafe(otid);
        let tuple = AssertUnwindSafe(tuple);
        let result = unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleUpdateWithInfo(
                    *relation,
                    *otid,
                    *tuple,
                    *index_state,
                );
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        };

        drop(index_guard);
        result
    }

    /// Catalog update variant that maps PostgreSQL tuple version conflicts to
    /// `Ok(false)`.
    ///
    /// Like `catalog_tuple_update_raw`, this uses `CatalogTupleUpdateWithInfo`
    /// with index state owned by `CatalogIndexGuard`, so cleanup remains under
    /// this wrapper's control after a caught conflict.
    ///
    /// # Safety
    ///
    /// Same requirements as `catalog_tuple_update_raw`.
    pub(crate) unsafe fn catalog_tuple_update_optimistic_raw(
        relation: pg_sys::Relation,
        otid: *mut pg_sys::ItemPointerData,
        tuple: pg_sys::HeapTuple,
    ) -> Result<bool, PgError> {
        let index_guard = unsafe { CatalogIndexGuard::open(relation)? };
        let index_state = AssertUnwindSafe(index_guard.as_raw());
        let relation = AssertUnwindSafe(relation);
        let otid = AssertUnwindSafe(otid);
        let tuple = AssertUnwindSafe(tuple);

        let result = unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleUpdateWithInfo(
                    *relation,
                    *otid,
                    *tuple,
                    *index_state,
                );
                Ok(true)
            })
            .catch_others(|err| {
                let error = PgError::from_caught(err);
                if error.is_tuple_concurrency_conflict() {
                    Ok(false)
                } else {
                    Err(error)
                }
            })
            .execute()
        };

        drop(index_guard);
        result
    }

    /// # Safety
    ///
    /// `relation` must be valid and `tid` must identify a tuple in that
    /// relation.
    pub(crate) unsafe fn catalog_tuple_delete_raw(
        relation: pg_sys::Relation,
        tid: *mut pg_sys::ItemPointerData,
    ) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(relation);
        let tid = AssertUnwindSafe(tid);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleDelete(*relation, *tid);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}
