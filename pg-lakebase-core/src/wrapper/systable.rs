use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, pg_sys};
use std::panic::AssertUnwindSafe;

impl PgWrapper {
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
}
