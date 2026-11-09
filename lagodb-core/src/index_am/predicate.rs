//! Serializable predicate locks for `ampredlocks` access methods. The calls
//! are no-ops outside serializable transactions, so call them
//! unconditionally at the points `nbtree` and `gist` do.

use std::marker::PhantomData;
use std::ptr;

use pgrx::pg_sys;

#[derive(Clone, Copy)]
pub struct IndexPredicateReads<'a> {
    index: pg_sys::Relation,
    snapshot: pg_sys::Snapshot,
    _scan: PhantomData<&'a pg_sys::IndexScanDescData>,
}

impl IndexPredicateReads<'_> {
    pub(super) fn new(index: pg_sys::Relation, snapshot: pg_sys::Snapshot) -> Self {
        Self {
            index,
            snapshot,
            _scan: PhantomData,
        }
    }

    pub fn lock_page(&self, block: pg_sys::BlockNumber) {
        // SAFETY: the scan's index relation and snapshot are live.
        unsafe { pg_sys::PredicateLockPage(self.index, block, self.snapshot) }
    }

    pub fn lock_relation(&self) {
        // SAFETY: as above.
        unsafe { pg_sys::PredicateLockRelation(self.index, self.snapshot) }
    }
}

#[derive(Clone, Copy)]
pub struct IndexPredicateWrites<'a> {
    index: pg_sys::Relation,
    _index: PhantomData<&'a pg_sys::RelationData>,
}

impl IndexPredicateWrites<'_> {
    pub(super) fn new(index: pg_sys::Relation) -> Self {
        Self {
            index,
            _index: PhantomData,
        }
    }

    pub fn check_conflict_in_page(&self, block: pg_sys::BlockNumber) {
        // SAFETY: the index relation is live for the callback.
        unsafe {
            pg_sys::CheckForSerializableConflictIn(self.index, ptr::null_mut(), block)
        }
    }

    /// Checks relation-level locks only.
    pub fn check_conflict_in_relation(&self) {
        // SAFETY: as above.
        unsafe {
            pg_sys::CheckForSerializableConflictIn(
                self.index,
                ptr::null_mut(),
                pg_sys::InvalidBlockNumber,
            )
        }
    }

    /// `PredicateLockPageSplit`.
    pub fn page_split(
        &self,
        old_block: pg_sys::BlockNumber,
        new_block: pg_sys::BlockNumber,
    ) {
        // SAFETY: as above.
        unsafe { pg_sys::PredicateLockPageSplit(self.index, old_block, new_block) }
    }

    /// `PredicateLockPageCombine`: move `old_block`'s locks to the page that
    /// absorbed it.
    pub fn page_combine(
        &self,
        old_block: pg_sys::BlockNumber,
        new_block: pg_sys::BlockNumber,
    ) {
        // SAFETY: as above.
        unsafe { pg_sys::PredicateLockPageCombine(self.index, old_block, new_block) }
    }
}
