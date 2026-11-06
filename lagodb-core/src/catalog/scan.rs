//! System-table scans over an open [`CatalogRelation`].

use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::ptr::null_mut;

use pgrx::{PgTryBuilder, pg_sys};

use super::{CatalogRelation, CatalogScanKey, CatalogSnapshot};
use crate::diag::PgError;
use crate::handles::HeapTupleRef;

impl CatalogRelation {
    /// Begin a system catalog scan tied to this relation guard.
    pub fn begin_scan<'scan, const N: usize>(
        &'scan self,
        index_id: pg_sys::Oid,
        index_ok: bool,
        snapshot: CatalogSnapshot<'scan>,
        keys: [CatalogScanKey; N],
    ) -> Result<CatalogScan<'scan>, PgError> {
        let mut keys = keys.map(CatalogScanKey::into_data);
        let key = Self::scan_key_ptr(&mut keys);
        let heap_relation = AssertUnwindSafe(self.as_raw());
        let snapshot = snapshot.as_raw();
        // SAFETY: this guard keeps the relation open, and systable_beginscan
        // copies the keys into the scan descriptor before returning.
        let scan = unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::systable_beginscan(
                    *heap_relation,
                    index_id,
                    index_ok,
                    snapshot,
                    N as i32,
                    key,
                ))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }?;
        Ok(CatalogScan {
            scan,
            _relation: PhantomData,
        })
    }

    /// Begin a scan that explicitly guarantees index order.
    pub fn begin_ordered_scan<'scan, const N: usize>(
        &'scan self,
        index_id: pg_sys::Oid,
        snapshot: CatalogSnapshot<'scan>,
        keys: [CatalogScanKey; N],
    ) -> Result<CatalogOrderedScan<'scan>, PgError> {
        let index_relation = OrderedScanIndex::open(index_id)?;
        let mut keys = keys.map(CatalogScanKey::into_data);
        let key = Self::scan_key_ptr(&mut keys);
        let heap_relation = AssertUnwindSafe(self.as_raw());
        let raw_index = AssertUnwindSafe(index_relation.0);
        let snapshot = snapshot.as_raw();
        // SAFETY: both relations stay open for the scan's lifetime, and
        // systable_beginscan_ordered copies the keys before returning. On
        // ERROR, `index_relation` closes the index it opened.
        let scan = unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::systable_beginscan_ordered(
                    *heap_relation,
                    *raw_index,
                    snapshot,
                    N as i32,
                    key,
                ))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }?;
        Ok(CatalogOrderedScan {
            scan,
            _index_relation: index_relation,
            _relation: PhantomData,
        })
    }

    #[inline]
    fn scan_key_ptr(keys: &mut [pg_sys::ScanKeyData]) -> pg_sys::ScanKey {
        if keys.is_empty() {
            null_mut()
        } else {
            keys.as_mut_ptr()
        }
    }
}

/// RAII guard for system table scan - automatically ends scan when dropped.
#[derive(Debug)]
pub struct CatalogScan<'a> {
    scan: pg_sys::SysScanDesc,
    _relation: PhantomData<&'a CatalogRelation>,
}

impl CatalogScan<'_> {
    /// Get the raw scan descriptor.
    #[inline]
    pub fn as_raw(&self) -> pg_sys::SysScanDesc {
        self.scan
    }

    /// Fetch the next tuple from this system table scan.
    ///
    /// PostgreSQL invalidates the previously returned tuple on the next
    /// `systable_getnext` call, so this method requires `&mut self` to prevent
    /// callers from holding two scan tuple references at the same time.
    pub fn get_next(&mut self) -> Result<Option<HeapTupleRef<'_>>, PgError> {
        let scan = AssertUnwindSafe(self.scan);
        // SAFETY: this guard owns a live scan descriptor.
        let tuple = unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::systable_getnext(*scan)))
                .catch_others(|err| Err(PgError::from_caught(err)))
                .execute()
        }?;
        // SAFETY: the tuple stays valid until the next call through `&mut self`.
        Ok((!tuple.is_null()).then(|| unsafe { HeapTupleRef::from_raw(tuple) }))
    }
}

impl Drop for CatalogScan<'_> {
    fn drop(&mut self) {
        // SAFETY: the scan came from systable_beginscan and is ended once.
        unsafe { pg_sys::systable_endscan(self.scan) };
    }
}

/// RAII guard for a catalog scan with explicit index ordering.
#[derive(Debug)]
pub struct CatalogOrderedScan<'a> {
    scan: pg_sys::SysScanDesc,
    // Fields drop after `Drop::drop`, so the index closes once the scan ends.
    _index_relation: OrderedScanIndex,
    _relation: PhantomData<&'a CatalogRelation>,
}

impl CatalogOrderedScan<'_> {
    pub fn get_next(&mut self) -> Result<Option<HeapTupleRef<'_>>, PgError> {
        let scan = AssertUnwindSafe(self.scan);
        // SAFETY: this guard owns a live ordered scan descriptor.
        let tuple = unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::systable_getnext_ordered(
                    *scan,
                    pg_sys::ScanDirection::ForwardScanDirection,
                ))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }?;
        // SAFETY: the tuple stays valid until the next call through `&mut self`.
        Ok((!tuple.is_null()).then(|| unsafe { HeapTupleRef::from_raw(tuple) }))
    }
}

impl Drop for CatalogOrderedScan<'_> {
    fn drop(&mut self) {
        // SAFETY: the scan came from systable_beginscan_ordered and is ended
        // once; its index closes afterwards with `_index_relation`.
        unsafe { pg_sys::systable_endscan_ordered(self.scan) };
    }
}

/// Index relation opened with `AccessShareLock` for an ordered scan.
#[derive(Debug)]
struct OrderedScanIndex(pg_sys::Relation);

impl OrderedScanIndex {
    const LOCK_MODE: pg_sys::LOCKMODE = pg_sys::AccessShareLock as _;

    fn open(index_id: pg_sys::Oid) -> Result<Self, PgError> {
        // SAFETY: index_open raises a PG ERROR for a non-index OID.
        unsafe {
            PgTryBuilder::new(move || {
                Ok(Self(pg_sys::index_open(index_id, Self::LOCK_MODE)))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}

impl Drop for OrderedScanIndex {
    fn drop(&mut self) {
        // SAFETY: the relation came from index_open with LOCK_MODE.
        unsafe { pg_sys::index_close(self.0, Self::LOCK_MODE) };
    }
}
