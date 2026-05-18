use crate::diag::PgError;
use crate::wrapper::PgWrapper;
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
    pub(crate) fn new(cache_id: i32, tuple: pg_sys::HeapTuple) -> Self {
        Self { cache_id, tuple }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::HeapTuple {
        self.tuple
    }

    /// Returns an attribute datum while this syscache tuple is still pinned.
    ///
    /// The returned `Datum` may point into PostgreSQL-owned syscache memory.
    /// Callers must use it or copy its contents before this guard is dropped.
    pub fn get_attr(
        &self,
        attribute_number: i16,
    ) -> Result<Option<pg_sys::Datum>, PgError> {
        let mut is_null = false;
        let datum = unsafe {
            PgWrapper::sys_cache_get_attr_raw(
                self.cache_id,
                self.tuple,
                attribute_number,
                &mut is_null,
            )?
        };
        Ok((!is_null).then_some(datum))
    }
}

impl Drop for SysCacheTuple {
    fn drop(&mut self) {
        // Drop cannot report PostgreSQL errors. Releasing a syscache reference
        // is best-effort here; callers needing explicit error handling should
        // add an explicit close/release API before relying on Drop.
        let _ = unsafe { PgWrapper::release_sys_cache_raw(self.tuple) };
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
    pub(crate) fn new(cache_id: i32, tuple: pg_sys::HeapTuple) -> Self {
        Self { cache_id, tuple }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::HeapTuple {
        self.tuple
    }

    /// Returns an attribute datum while this tuple copy is still alive.
    ///
    /// The returned `Datum` may point into this tuple copy. Callers must use it
    /// or copy its contents before this guard is dropped.
    pub fn get_attr(
        &self,
        attribute_number: i16,
    ) -> Result<Option<pg_sys::Datum>, PgError> {
        let mut is_null = false;
        let datum = unsafe {
            PgWrapper::sys_cache_get_attr_raw(
                self.cache_id,
                self.tuple,
                attribute_number,
                &mut is_null,
            )?
        };
        Ok((!is_null).then_some(datum))
    }
}

impl Drop for SysCacheTupleCopy {
    fn drop(&mut self) {
        unsafe {
            pg_sys::heap_freetuple(self.tuple);
        }
    }
}

pub fn search_syscache1(
    cache_id: i32,
    key1: pg_sys::Datum,
) -> Result<Option<SysCacheTuple>, PgError> {
    let tuple = PgWrapper::search_sys_cache1_raw(cache_id, key1)?;
    Ok(tuple.map(|tuple| SysCacheTuple::new(cache_id, tuple)))
}

pub fn search_syscache_copy(
    cache_id: i32,
    key1: pg_sys::Datum,
    key2: pg_sys::Datum,
    key3: pg_sys::Datum,
    key4: pg_sys::Datum,
) -> Result<Option<SysCacheTupleCopy>, PgError> {
    let tuple =
        PgWrapper::search_sys_cache_copy_raw(cache_id, key1, key2, key3, key4)?;
    Ok(tuple.map(|tuple| SysCacheTupleCopy::new(cache_id, tuple)))
}
