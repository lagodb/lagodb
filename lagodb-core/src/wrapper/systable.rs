use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, pg_sys};
use std::panic::AssertUnwindSafe;

impl PgWrapper {
    /// Open an index relation for an ordered system-table scan.
    ///
    /// # Safety
    ///
    /// `index_id` must identify a PostgreSQL index relation.
    pub(crate) unsafe fn index_open_raw(
        index_id: pg_sys::Oid,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<pg_sys::Relation, PgError> {
        unsafe {
            PgTryBuilder::new(move || Ok(pg_sys::index_open(index_id, lock_mode)))
                .catch_others(|err| Err(PgError::from_caught(err)))
                .execute()
        }
    }

    /// Close an index relation opened by [`Self::index_open_raw`].
    ///
    /// # Safety
    ///
    /// `relation` must be a live index relation opened with `lock_mode`.
    pub(crate) unsafe fn index_close_raw(
        relation: pg_sys::Relation,
        lock_mode: pg_sys::LOCKMODE,
    ) -> Result<(), PgError> {
        let relation = AssertUnwindSafe(relation);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::index_close(*relation, lock_mode);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// # Safety
    ///
    /// `heap_relation` must be a valid open relation. `key` must be valid for
    /// `nkeys` entries, or null when `nkeys` is zero.
    pub(crate) unsafe fn systable_beginscan_raw(
        heap_relation: pg_sys::Relation,
        index_id: pg_sys::Oid,
        index_ok: bool,
        snapshot: pg_sys::Snapshot,
        nkeys: i32,
        key: pg_sys::ScanKey,
    ) -> Result<pg_sys::SysScanDesc, PgError> {
        let heap_relation = AssertUnwindSafe(heap_relation);
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::systable_beginscan(
                    *heap_relation,
                    index_id,
                    index_ok,
                    snapshot,
                    nkeys,
                    key,
                ))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Begin an index-ordered catalog scan.
    ///
    /// # Safety
    ///
    /// Both relations and all scan keys must remain live for the scan.
    pub(crate) unsafe fn systable_beginscan_ordered_raw(
        heap_relation: pg_sys::Relation,
        index_relation: pg_sys::Relation,
        snapshot: pg_sys::Snapshot,
        nkeys: i32,
        key: pg_sys::ScanKey,
    ) -> Result<pg_sys::SysScanDesc, PgError> {
        let heap_relation = AssertUnwindSafe(heap_relation);
        let index_relation = AssertUnwindSafe(index_relation);
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::systable_beginscan_ordered(
                    *heap_relation,
                    *index_relation,
                    snapshot,
                    nkeys,
                    key,
                ))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// # Safety
    ///
    /// `sysscan` must be a live system table scan descriptor.
    pub(crate) unsafe fn systable_getnext_raw(
        sysscan: pg_sys::SysScanDesc,
    ) -> Result<Option<pg_sys::HeapTuple>, PgError> {
        let sysscan = AssertUnwindSafe(sysscan);
        unsafe {
            PgTryBuilder::new(move || {
                let tuple = pg_sys::systable_getnext(*sysscan);
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Fetch the next tuple from an ordered catalog scan.
    ///
    /// # Safety
    ///
    /// `sysscan` must be a live ordered scan descriptor.
    pub(crate) unsafe fn systable_getnext_ordered_raw(
        sysscan: pg_sys::SysScanDesc,
    ) -> Result<Option<pg_sys::HeapTuple>, PgError> {
        let sysscan = AssertUnwindSafe(sysscan);
        unsafe {
            PgTryBuilder::new(move || {
                let tuple = pg_sys::systable_getnext_ordered(
                    *sysscan,
                    pg_sys::ScanDirection::ForwardScanDirection,
                );
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// # Safety
    ///
    /// `sysscan` must be a live system table scan descriptor that has not
    /// already been ended.
    pub(crate) unsafe fn systable_endscan_raw(
        sysscan: pg_sys::SysScanDesc,
    ) -> Result<(), PgError> {
        let sysscan = AssertUnwindSafe(sysscan);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::systable_endscan(*sysscan);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// End an ordered catalog scan.
    ///
    /// # Safety
    ///
    /// `sysscan` must be live and not already ended.
    pub(crate) unsafe fn systable_endscan_ordered_raw(
        sysscan: pg_sys::SysScanDesc,
    ) -> Result<(), PgError> {
        let sysscan = AssertUnwindSafe(sysscan);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::systable_endscan_ordered(*sysscan);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}
