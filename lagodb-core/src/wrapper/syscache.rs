use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, pg_sys};
use std::panic::AssertUnwindSafe;

impl PgWrapper {
    pub(crate) fn search_sys_cache_copy_raw(
        cache_id: i32,
        key1: pg_sys::Datum,
        key2: pg_sys::Datum,
        key3: pg_sys::Datum,
        key4: pg_sys::Datum,
    ) -> Result<Option<pg_sys::HeapTuple>, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let tuple =
                    pg_sys::SearchSysCacheCopy(cache_id, key1, key2, key3, key4);
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    pub(crate) fn search_sys_cache1_raw(
        cache_id: i32,
        key1: pg_sys::Datum,
    ) -> Result<Option<pg_sys::HeapTuple>, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let tuple = pg_sys::SearchSysCache1(cache_id, key1);
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    pub(crate) fn search_sys_cache2_raw(
        cache_id: i32,
        key1: pg_sys::Datum,
        key2: pg_sys::Datum,
    ) -> Result<Option<pg_sys::HeapTuple>, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let tuple = pg_sys::SearchSysCache2(cache_id, key1, key2);
                Ok(if tuple.is_null() { None } else { Some(tuple) })
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// # Safety
    ///
    /// `tuple` must be a valid tuple for `cache_id`, and `is_null` must be a
    /// valid writable bool pointer.
    pub(crate) unsafe fn sys_cache_get_attr_raw(
        cache_id: i32,
        tuple: pg_sys::HeapTuple,
        attribute_number: i16,
        is_null: *mut bool,
    ) -> Result<pg_sys::Datum, PgError> {
        let tuple = AssertUnwindSafe(tuple);
        let is_null = AssertUnwindSafe(is_null);
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::SysCacheGetAttr(
                    cache_id,
                    *tuple,
                    attribute_number,
                    *is_null,
                ))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// # Safety
    ///
    /// `tuple` must be a syscache tuple returned by `SearchSysCache*` and not a
    /// heap-allocated copy.
    pub(crate) unsafe fn release_sys_cache_raw(
        tuple: pg_sys::HeapTuple,
    ) -> Result<(), PgError> {
        let tuple = AssertUnwindSafe(tuple);
        unsafe {
            PgTryBuilder::new(move || {
                pg_sys::ReleaseSysCache(*tuple);
                Ok(())
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}
