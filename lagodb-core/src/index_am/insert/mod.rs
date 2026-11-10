//! Index insertion.
//!
//! PostgreSQL 15 and 16 have no `aminsertcleanup`: the session is only dropped with
//! `ii_Context`, and dropping is not a successful finish, so each `insert`
//! must be durable on its own.

mod callbacks;
mod unique;

use std::marker::PhantomData;

use pgrx::pg_sys;

use super::options::IndexOptions;
use super::predicate::IndexPredicateWrites;
use super::values::IndexValues;
use super::{IndexAccessMethod, IndexAmResult};
use crate::handles::{IndexInfoHandle, RelationHandle, ValidItemPointer};

pub(super) use callbacks::aminsert;
#[cfg(any(feature = "pg17", feature = "pg18"))]
pub(super) use callbacks::aminsertcleanup;
pub use unique::{HeapConflictProbe, HeapTupleWait};

/// The uniqueness check PostgreSQL requests for one insert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexUniqueCheck {
    No,
    Yes,
    /// Insert anyway and return `false` when a conflict may exist.
    Partial,
    /// Deferred recheck of an existing entry: check, do not insert.
    Existing,
}

impl IndexUniqueCheck {
    fn from_raw(raw: pg_sys::IndexUniqueCheck::Type) -> Self {
        match raw {
            pg_sys::IndexUniqueCheck::UNIQUE_CHECK_NO => Self::No,
            pg_sys::IndexUniqueCheck::UNIQUE_CHECK_YES => Self::Yes,
            pg_sys::IndexUniqueCheck::UNIQUE_CHECK_PARTIAL => Self::Partial,
            pg_sys::IndexUniqueCheck::UNIQUE_CHECK_EXISTING => Self::Existing,
            _ => unreachable!("IndexUniqueCheck has four PostgreSQL values"),
        }
    }
}

pub struct IndexInsertRow<'a> {
    pub values: IndexValues<'a>,
    pub heap_tid: ValidItemPointer,
    pub unique_check: IndexUniqueCheck,
    /// UPDATE did not change the indexed values (a hint).
    pub index_unchanged: bool,
}

/// State of the inserts made through one `IndexInfo`.
///
/// The session is dropped by a reset callback on its lifecycle context, which
/// PostgreSQL may run after deleting sibling and child contexts, so it owns
/// PostgreSQL contexts only as
/// [`OwnedBackendContext`](crate::backend::OwnedBackendContext)s.
pub trait IndexInsertSession: Sized + 'static {
    type AccessMethod: IndexAccessMethod;

    /// Runs in the session's lifecycle context; `insert` runs in the
    /// caller's, which may be reset after every row.
    fn begin(ctx: &IndexInsertContext<'_, Self::AccessMethod>)
    -> IndexAmResult<Self>;

    /// The result matters only for [`IndexUniqueCheck::Partial`]: `false`
    /// means PostgreSQL must recheck the row.
    fn insert(
        &mut self,
        ctx: &IndexInsertContext<'_, Self::AccessMethod>,
        row: IndexInsertRow<'_>,
    ) -> IndexAmResult<bool>;

    #[cfg(any(feature = "pg17", feature = "pg18"))]
    fn finish(
        &mut self,
        ctx: &IndexInsertCleanupContext<'_, Self::AccessMethod>,
    ) -> IndexAmResult<()> {
        let _ = ctx;
        Ok(())
    }
}

pub struct IndexInsertContext<'a, A> {
    index: RelationHandle<'a>,
    heap: RelationHandle<'a>,
    index_info: IndexInfoHandle<'a>,
    _access_method: PhantomData<fn() -> A>,
}

impl<'a, A: IndexAccessMethod> IndexInsertContext<'a, A> {
    pub(super) fn new(
        index: RelationHandle<'a>,
        heap: RelationHandle<'a>,
        index_info: IndexInfoHandle<'a>,
    ) -> Self {
        Self {
            index,
            heap,
            index_info,
            _access_method: PhantomData,
        }
    }

    #[inline]
    pub fn index(&self) -> &RelationHandle<'a> {
        &self.index
    }

    #[inline]
    pub fn options(&self) -> IndexOptions<'a, A> {
        IndexOptions::of_index(&self.index)
    }

    #[inline]
    pub fn heap(&self) -> &RelationHandle<'a> {
        &self.heap
    }

    #[inline]
    pub fn index_info(&self) -> &IndexInfoHandle<'a> {
        &self.index_info
    }

    #[inline]
    pub fn predicate_writes(&self) -> IndexPredicateWrites<'a> {
        IndexPredicateWrites::new(self.index.as_raw())
    }
}

#[cfg(any(feature = "pg17", feature = "pg18"))]
pub struct IndexInsertCleanupContext<'a, A> {
    index: RelationHandle<'a>,
    index_info: IndexInfoHandle<'a>,
    _access_method: PhantomData<fn() -> A>,
}

#[cfg(any(feature = "pg17", feature = "pg18"))]
impl<'a, A: IndexAccessMethod> IndexInsertCleanupContext<'a, A> {
    pub(super) fn new(
        index: RelationHandle<'a>,
        index_info: IndexInfoHandle<'a>,
    ) -> Self {
        Self {
            index,
            index_info,
            _access_method: PhantomData,
        }
    }

    #[inline]
    pub fn index(&self) -> &RelationHandle<'a> {
        &self.index
    }

    #[inline]
    pub fn options(&self) -> IndexOptions<'a, A> {
        IndexOptions::of_index(&self.index)
    }

    #[inline]
    pub fn index_info(&self) -> &IndexInfoHandle<'a> {
        &self.index_info
    }
}
