//! Owning guards for PostgreSQL syscache lookups.

use pgrx::pg_sys;

/// Tuple borrowed from PostgreSQL's syscache.
///
/// This is the result of `SearchSysCache*` and must be released with
/// `ReleaseSysCache`, not `heap_freetuple`.
#[derive(Debug)]
pub struct SysCacheTuple {
    cache_id: i32,
    tuple: pg_sys::HeapTuple,
}

impl SysCacheTuple {
    #[inline]
    pub fn as_raw(&self) -> pg_sys::HeapTuple {
        self.tuple
    }

    /// Returns an attribute datum while this syscache tuple is still pinned.
    ///
    /// The returned `Datum` may point into PostgreSQL-owned syscache memory.
    /// Callers must use it or copy its contents before this guard is dropped.
    pub fn get_attr(&self, attribute_number: i16) -> Option<pg_sys::Datum> {
        // SAFETY: this guard pins a tuple of `cache_id`.
        unsafe { sys_cache_get_attr(self.cache_id, self.tuple, attribute_number) }
    }
}

impl Drop for SysCacheTuple {
    fn drop(&mut self) {
        // SAFETY: the tuple came from `SearchSysCache1` and is released once.
        unsafe { pg_sys::ReleaseSysCache(self.tuple) }
    }
}

/// Modifiable copy of a syscache tuple.
///
/// This is the result of `SearchSysCacheCopy` and must be freed with
/// `heap_freetuple`, not `ReleaseSysCache`.
#[derive(Debug)]
pub struct SysCacheTupleCopy {
    cache_id: i32,
    tuple: pg_sys::HeapTuple,
}

impl SysCacheTupleCopy {
    #[inline]
    pub fn as_raw(&self) -> pg_sys::HeapTuple {
        self.tuple
    }

    /// Returns an attribute datum while this tuple copy is still alive.
    ///
    /// The returned `Datum` may point into this tuple copy. Callers must use it
    /// or copy its contents before this guard is dropped.
    pub fn get_attr(&self, attribute_number: i16) -> Option<pg_sys::Datum> {
        // SAFETY: this guard owns a copied tuple of `cache_id`.
        unsafe { sys_cache_get_attr(self.cache_id, self.tuple, attribute_number) }
    }
}

impl Drop for SysCacheTupleCopy {
    fn drop(&mut self) {
        unsafe {
            pg_sys::heap_freetuple(self.tuple);
        }
    }
}

/// # Safety
///
/// `tuple` must be a valid tuple for `cache_id`, and `attribute_number` must
/// identify a valid attribute for that cache's catalog relation.
unsafe fn sys_cache_get_attr(
    cache_id: i32,
    tuple: pg_sys::HeapTuple,
    attribute_number: i16,
) -> Option<pg_sys::Datum> {
    let mut is_null = false;
    let datum = unsafe {
        pg_sys::SysCacheGetAttr(cache_id, tuple, attribute_number, &mut is_null)
    };
    (!is_null).then_some(datum)
}

pub fn search_syscache1(cache_id: i32, key1: pg_sys::Datum) -> Option<SysCacheTuple> {
    let tuple = unsafe { pg_sys::SearchSysCache1(cache_id, key1) };
    (!tuple.is_null()).then_some(SysCacheTuple { cache_id, tuple })
}

pub fn search_syscache_copy(
    cache_id: i32,
    key1: pg_sys::Datum,
    key2: pg_sys::Datum,
    key3: pg_sys::Datum,
    key4: pg_sys::Datum,
) -> Option<SysCacheTupleCopy> {
    let tuple =
        unsafe { pg_sys::SearchSysCacheCopy(cache_id, key1, key2, key3, key4) };
    (!tuple.is_null()).then_some(SysCacheTupleCopy { cache_id, tuple })
}
