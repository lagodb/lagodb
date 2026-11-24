//! Owning guards for PostgreSQL syscache lookups.

use std::ptr::NonNull;

use pgrx::pg_sys;

use super::tuple::HeapTupleRef;

/// Tuple borrowed from PostgreSQL's syscache.
///
/// This is the result of `SearchSysCache*` and must be released with
/// `ReleaseSysCache`, not `heap_freetuple`.
#[derive(Debug)]
pub struct SysCacheTuple {
    cache_id: i32,
    tuple: NonNull<pg_sys::HeapTupleData>,
}

impl SysCacheTuple {
    #[inline]
    pub fn as_raw(&self) -> pg_sys::HeapTuple {
        self.tuple.as_ptr()
    }

    /// Borrow this pinned tuple with a lifetime tied to the syscache guard.
    #[inline]
    pub fn as_tuple_ref(&self) -> HeapTupleRef<'_> {
        unsafe { HeapTupleRef::from_raw(self.tuple.as_ptr()) }
    }

    /// Borrow the fixed-width catalog form stored in this pinned tuple.
    ///
    /// # Safety
    ///
    /// `T` must be the fixed-width form type for this cache's catalog relation.
    #[inline]
    pub unsafe fn form<T>(&self) -> &T {
        // SAFETY: upheld by the caller; the returned borrow is tied to `self`.
        unsafe { self.as_tuple_ref().form::<T>() }
    }

    /// Returns an attribute datum while this syscache tuple is still pinned.
    ///
    /// The returned `Datum` may point into PostgreSQL-owned syscache memory.
    /// Callers must use it or copy its contents before this guard is dropped.
    pub fn get_attr(&self, attribute_number: i16) -> Option<pg_sys::Datum> {
        let mut is_null = false;
        // SAFETY: this guard pins a tuple of `cache_id`.
        let datum = unsafe {
            pg_sys::SysCacheGetAttr(
                self.cache_id,
                self.tuple.as_ptr(),
                attribute_number,
                &mut is_null,
            )
        };
        (!is_null).then_some(datum)
    }
}

impl Drop for SysCacheTuple {
    fn drop(&mut self) {
        // SAFETY: the tuple came from `SearchSysCache*` and is released once.
        unsafe { pg_sys::ReleaseSysCache(self.tuple.as_ptr()) }
    }
}

pub fn search_syscache1(cache_id: i32, key1: pg_sys::Datum) -> Option<SysCacheTuple> {
    let tuple = unsafe { pg_sys::SearchSysCache1(cache_id, key1) };
    NonNull::new(tuple).map(|tuple| SysCacheTuple { cache_id, tuple })
}

pub fn search_syscache2(
    cache_id: i32,
    key1: pg_sys::Datum,
    key2: pg_sys::Datum,
) -> Option<SysCacheTuple> {
    let tuple = unsafe { pg_sys::SearchSysCache2(cache_id, key1, key2) };
    NonNull::new(tuple).map(|tuple| SysCacheTuple { cache_id, tuple })
}

pub fn search_syscache3(
    cache_id: i32,
    key1: pg_sys::Datum,
    key2: pg_sys::Datum,
    key3: pg_sys::Datum,
) -> Option<SysCacheTuple> {
    let tuple = unsafe { pg_sys::SearchSysCache3(cache_id, key1, key2, key3) };
    NonNull::new(tuple).map(|tuple| SysCacheTuple { cache_id, tuple })
}
