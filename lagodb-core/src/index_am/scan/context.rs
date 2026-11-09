//! Borrowed views of an index scan descriptor.

use std::marker::PhantomData;
use std::ptr;
use std::slice;

use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::{PgMemoryContexts, pg_sys};

use super::contract::IndexScanSession;
use crate::handles::{RelationHandle, ScanKeyEntry, ScanKeyIter, SnapshotHandle};
use crate::index_am::options::IndexOptions;
use crate::index_am::predicate::IndexPredicateReads;

unsafe extern "C-unwind" {
    fn lagodb_index_scan_record_search(scan: pg_sys::IndexScanDesc);
}

pub struct IndexScanContext<'a, S> {
    scan: pg_sys::IndexScanDesc,
    _scan: PhantomData<&'a pg_sys::IndexScanDescData>,
    _session: PhantomData<fn() -> S>,
}

impl<S> Clone for IndexScanContext<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for IndexScanContext<'_, S> {}

impl<'a, S> IndexScanContext<'a, S> {
    /// # Safety
    ///
    /// `scan` must be the live descriptor of the current callback, and its
    /// index must belong to `S`'s access method.
    #[inline]
    pub(super) unsafe fn new(scan: pg_sys::IndexScanDesc) -> Self {
        Self {
            scan,
            _scan: PhantomData,
            _session: PhantomData,
        }
    }

    #[inline]
    pub(super) fn as_raw(&self) -> pg_sys::IndexScanDesc {
        self.scan
    }

    #[inline]
    fn desc(&self) -> &'a pg_sys::IndexScanDescData {
        // SAFETY: live for the callback.
        unsafe { &*self.scan }
    }

    #[inline]
    pub fn index(&self) -> RelationHandle<'a> {
        // SAFETY: PostgreSQL keeps the index open for the scan.
        unsafe { RelationHandle::from_raw(self.desc().indexRelation) }
    }

    /// The table, or `None` for a bitmap scan (`index_beginscan_bitmap`).
    #[inline]
    pub fn heap(&self) -> Option<RelationHandle<'a>> {
        let heap = self.desc().heapRelation;
        // SAFETY: a non-NULL heap relation stays open for the scan.
        (!heap.is_null()).then(|| unsafe { RelationHandle::from_raw(heap) })
    }

    #[inline]
    pub fn snapshot(&self) -> SnapshotHandle<'a> {
        // SAFETY: as above.
        unsafe { SnapshotHandle::from_raw(self.desc().xs_snapshot) }
    }

    #[inline]
    pub fn keys(&self) -> IndexScanKeys<'a> {
        let desc = self.desc();
        // SAFETY: RelationGetIndexScan allocated `numberOfKeys` keys.
        unsafe { IndexScanKeys::from_raw(desc.keyData, desc.numberOfKeys) }
    }

    #[inline]
    pub fn order_bys(&self) -> IndexScanKeys<'a> {
        let desc = self.desc();
        // SAFETY: RelationGetIndexScan allocated `numberOfOrderBys` keys.
        unsafe { IndexScanKeys::from_raw(desc.orderByData, desc.numberOfOrderBys) }
    }

    #[inline]
    pub fn want_index_tuple(&self) -> bool {
        self.desc().xs_want_itup
    }

    #[inline]
    pub fn ignore_killed_tuples(&self) -> bool {
        self.desc().ignore_killed_tuples
    }

    #[inline]
    pub fn xact_started_in_recovery(&self) -> bool {
        self.desc().xactStartedInRecovery
    }

    /// Count one index search (`pgstat_count_index_scan`, and `nsearches` on
    /// PostgreSQL 18). Call once per descent of the index, not per row.
    pub fn record_search(&self) {
        let scan = self.scan;
        // SAFETY: the bridge reads only the live descriptor.
        unsafe { pg_guard_ffi_boundary(|| lagodb_index_scan_record_search(scan)) }
    }

    #[inline]
    pub fn predicate_reads(&self) -> IndexPredicateReads<'a> {
        let desc = self.desc();
        IndexPredicateReads::new(desc.indexRelation, desc.xs_snapshot)
    }
}

impl<'a, S: IndexScanSession> IndexScanContext<'a, S> {
    #[inline]
    pub fn options(&self) -> IndexOptions<'a, S::AccessMethod> {
        IndexOptions::of_index(&self.index())
    }
}

#[derive(Clone, Copy)]
pub struct IndexScanKeys<'a> {
    keys: &'a [pg_sys::ScanKeyData],
}

impl<'a> IndexScanKeys<'a> {
    /// # Safety
    ///
    /// `keys` must hold `len` keys valid for `'a`, or `len` must be 0.
    unsafe fn from_raw(keys: *const pg_sys::ScanKeyData, len: i32) -> Self {
        let keys = if len > 0 {
            // SAFETY: upheld by the caller.
            unsafe { slice::from_raw_parts(keys, len as usize) }
        } else {
            // RelationGetIndexScan leaves the array NULL for zero keys.
            &[]
        };
        Self { keys }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[inline]
    pub fn iter(&self) -> ScanKeyIter<'a> {
        ScanKeyIter::new(self.keys)
    }

    #[inline]
    pub fn as_slice(&self) -> &'a [pg_sys::ScanKeyData] {
        self.keys
    }
}

impl<'a> IntoIterator for IndexScanKeys<'a> {
    type Item = ScanKeyEntry<'a>;
    type IntoIter = ScanKeyIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// `xs_hitupdesc`, initially the index's descriptor. An `amstorage` access
/// method declares the indexed types of returned columns here, as GiST does
/// with its `fetchTupdesc`.
pub struct ReturnedTupleDesc<'a> {
    scan: pg_sys::IndexScanDesc,
    scan_context: pg_sys::MemoryContext,
    _scan: PhantomData<&'a mut pg_sys::IndexScanDescData>,
}

impl ReturnedTupleDesc<'_> {
    pub(super) fn new(
        scan: pg_sys::IndexScanDesc,
        scan_context: pg_sys::MemoryContext,
    ) -> Self {
        Self {
            scan,
            scan_context,
            _scan: PhantomData,
        }
    }

    pub fn set_column_type(
        &mut self,
        column: usize,
        type_oid: pg_sys::Oid,
        typmod: i32,
    ) {
        // SAFETY: the descriptor is live; the copy lives in the scan's
        // lifecycle context, and `column` is checked against its width.
        unsafe {
            let scan = &mut *self.scan;
            let index_desc = (*scan.indexRelation).rd_att;
            assert!(
                column < (*index_desc).natts as usize,
                "returned column {column} is not an index column"
            );
            if scan.xs_hitupdesc == index_desc {
                scan.xs_hitupdesc = PgMemoryContexts::For(self.scan_context)
                    .switch_to(|_| pg_sys::CreateTupleDescCopy(index_desc));
            }
            pg_sys::TupleDescInitEntry(
                scan.xs_hitupdesc,
                (column + 1) as pg_sys::AttrNumber,
                ptr::null(),
                type_oid,
                typmod,
                0,
            );
        }
    }
}
