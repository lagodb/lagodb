use super::PgWrapper;
use pgrx::pg_sys;

impl PgWrapper {
    pub(crate) fn search_sys_cache_copy_raw(
        cache_id: i32,
        key1: pg_sys::Datum,
        key2: pg_sys::Datum,
        key3: pg_sys::Datum,
        key4: pg_sys::Datum,
    ) -> Option<pg_sys::HeapTuple> {
        unsafe {
            let tuple = pg_sys::SearchSysCacheCopy(cache_id, key1, key2, key3, key4);
            (!tuple.is_null()).then_some(tuple)
        }
    }

    pub(crate) fn search_sys_cache1_raw(
        cache_id: i32,
        key1: pg_sys::Datum,
    ) -> Option<pg_sys::HeapTuple> {
        unsafe {
            let tuple = pg_sys::SearchSysCache1(cache_id, key1);
            (!tuple.is_null()).then_some(tuple)
        }
    }

    pub(crate) fn search_sys_cache2_raw(
        cache_id: i32,
        key1: pg_sys::Datum,
        key2: pg_sys::Datum,
    ) -> Option<pg_sys::HeapTuple> {
        unsafe {
            let tuple = pg_sys::SearchSysCache2(cache_id, key1, key2);
            (!tuple.is_null()).then_some(tuple)
        }
    }

    /// # Safety
    ///
    /// `tuple` must be a valid tuple for `cache_id`, and `attribute_number`
    /// must identify a valid attribute for that cache's catalog relation.
    pub(crate) unsafe fn sys_cache_get_attr_raw(
        cache_id: i32,
        tuple: pg_sys::HeapTuple,
        attribute_number: i16,
        is_null: &mut bool,
    ) -> pg_sys::Datum {
        unsafe { pg_sys::SysCacheGetAttr(cache_id, tuple, attribute_number, is_null) }
    }

    /// # Safety
    ///
    /// `tuple` must be a syscache tuple returned by `SearchSysCache*` and not a
    /// heap-allocated copy.
    pub(crate) unsafe fn release_sys_cache_raw(tuple: pg_sys::HeapTuple) {
        unsafe { pg_sys::ReleaseSysCache(tuple) }
    }
}
