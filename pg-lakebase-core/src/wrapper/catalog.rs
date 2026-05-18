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

    /// # Safety
    ///
    /// `relation`, `otid`, and `tuple` must be valid PostgreSQL objects. `otid`
    /// must identify the existing catalog tuple version being replaced.
    pub(crate) unsafe fn catalog_tuple_update_raw(
        relation: pg_sys::Relation,
        otid: *mut pg_sys::ItemPointerData,
        tuple: pg_sys::HeapTuple,
    ) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(relation);
        let otid = AssertUnwindSafe(otid);
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleUpdate(*relation, *otid, *tuple);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Catalog update variant that maps PostgreSQL tuple version conflicts to
    /// `Ok(false)`.
    ///
    /// This uses `CatalogTupleUpdateWithInfo`, not `CatalogTupleUpdate`.
    /// PostgreSQL exposes `simple_heap_update` tuple-version conflicts from
    /// this catalog path as an ERROR. Calling `CatalogTupleUpdate` directly
    /// would let that ERROR skip its internal `CatalogCloseIndexes` call,
    /// leaking the opened index relation. By opening indexes in
    /// `CatalogIndexGuard` and passing the state to PostgreSQL, index insertion
    /// remains PostgreSQL-owned while cleanup remains under this wrapper's
    /// control after the caught conflict.
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
