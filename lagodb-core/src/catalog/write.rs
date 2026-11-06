//! Catalog tuple writes that keep PostgreSQL's catalog indexes current.

use std::panic::AssertUnwindSafe;

use pgrx::{PgTryBuilder, pg_sys};

use super::CatalogRelation;
use crate::diag::PgError;
use crate::handles::{HeapTupleGuard, HeapTupleRef};

/// Result of a catalog tuple update operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogUpdateResult {
    /// The update succeeded.
    Success,
    /// The update failed because PostgreSQL reported a tuple version conflict.
    Conflict,
}

/// Rust-owned `CatalogIndexState`, closed on drop.
///
/// Keeping this state outside the `CatalogTuple*WithInfo` error boundary is
/// deliberate: PostgreSQL reports `simple_heap_update` tuple-version conflicts
/// with `ERROR`, and a caught ERROR skips the callee's normal control flow. If
/// we used `CatalogTupleUpdate` directly, its internally-opened index state
/// would not reach `CatalogCloseIndexes`. The `WithInfo` variants let
/// PostgreSQL still own catalog index insertion while this guard owns the
/// open/close lifecycle.
#[derive(Debug)]
struct CatalogIndexGuard {
    state: pg_sys::CatalogIndexState,
}

impl CatalogIndexGuard {
    fn open(relation: &CatalogRelation) -> Result<Self, PgError> {
        let relation = AssertUnwindSafe(relation.as_raw());
        // SAFETY: `relation` is an open catalog relation. CatalogOpenIndexes
        // pallocs its result, so a returned state is never NULL.
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
        // SAFETY: the state came from CatalogOpenIndexes and is closed once.
        unsafe { pg_sys::CatalogCloseIndexes(self.state) };
    }
}

impl CatalogRelation {
    /// Insert an owned heap tuple into this catalog relation.
    pub fn catalog_insert(&self, tuple: &HeapTupleGuard) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(self.as_raw());
        let tuple = AssertUnwindSafe(tuple.as_raw());
        // SAFETY: the guard owns a tuple formed for this catalog relation.
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleInsert(*relation, *tuple);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Open catalog indexes once for a bounded sequence of inserts.
    pub fn writer(&self) -> Result<CatalogWriter<'_>, PgError> {
        Ok(CatalogWriter {
            relation: self,
            indexes: CatalogIndexGuard::open(self)?,
        })
    }

    /// Update a catalog tuple and keep PostgreSQL catalog indexes current.
    pub fn catalog_update(
        &self,
        old_tuple: HeapTupleRef<'_>,
        new_tuple: &HeapTupleGuard,
    ) -> Result<(), PgError> {
        self.update_with_info(old_tuple, new_tuple)
    }

    /// Update a catalog tuple and report PostgreSQL tuple-version conflicts.
    pub fn catalog_update_optimistic(
        &self,
        old_tuple: HeapTupleRef<'_>,
        new_tuple: &HeapTupleGuard,
    ) -> Result<CatalogUpdateResult, PgError> {
        match self.update_with_info(old_tuple, new_tuple) {
            Ok(()) => Ok(CatalogUpdateResult::Success),
            Err(error) if error.is_tuple_concurrency_conflict() => {
                Ok(CatalogUpdateResult::Conflict)
            }
            Err(error) => Err(error),
        }
    }

    /// Delete a catalog tuple.
    pub fn catalog_delete(&self, tuple: HeapTupleRef<'_>) -> Result<(), PgError> {
        let mut tid = tuple.item_pointer_data();
        let relation = AssertUnwindSafe(self.as_raw());
        let tid = AssertUnwindSafe(&raw mut tid);
        // SAFETY: `tid` identifies a tuple returned by a scan of this relation.
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleDelete(*relation, *tid);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// `CatalogTupleUpdateWithInfo` under a Rust-owned index state; see
    /// [`CatalogIndexGuard`] for why the state stays outside the boundary.
    fn update_with_info(
        &self,
        old_tuple: HeapTupleRef<'_>,
        new_tuple: &HeapTupleGuard,
    ) -> Result<(), PgError> {
        let indexes = CatalogIndexGuard::open(self)?;
        let mut old_tid = old_tuple.item_pointer_data();
        let index_state = AssertUnwindSafe(indexes.as_raw());
        let relation = AssertUnwindSafe(self.as_raw());
        let old_tid = AssertUnwindSafe(&raw mut old_tid);
        let tuple = AssertUnwindSafe(new_tuple.as_raw());
        // SAFETY: `old_tid` identifies the replaced version of a tuple of
        // this relation, and `index_state` belongs to it.
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleUpdateWithInfo(
                    *relation,
                    *old_tid,
                    *tuple,
                    *index_state,
                );
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}

/// Reusable catalog index state for a bounded sequence of inserts.
#[derive(Debug)]
pub struct CatalogWriter<'a> {
    relation: &'a CatalogRelation,
    indexes: CatalogIndexGuard,
}

impl CatalogWriter<'_> {
    pub fn insert(&mut self, tuple: &HeapTupleGuard) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(self.relation.as_raw());
        let tuple = AssertUnwindSafe(tuple.as_raw());
        let index_state = AssertUnwindSafe(self.indexes.as_raw());
        // SAFETY: the index state was opened for this writer's relation.
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::CatalogTupleInsertWithInfo(*relation, *tuple, *index_state);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}
