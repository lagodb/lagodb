use super::borrowed::{PgBorrowed, PgNullable};
use pgrx::pg_sys;

/// Safe wrapper for PostgreSQL IndexInfo.
#[derive(Debug)]
pub struct IndexInfoHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::IndexInfo>,
}

impl<'a> IndexInfoHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::IndexInfo) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::IndexInfo {
        self.inner.as_ptr()
    }
}

/// Safe wrapper for PostgreSQL ValidateIndexState.
#[derive(Debug)]
pub struct ValidateIndexStateHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::ValidateIndexState>,
}

impl<'a> ValidateIndexStateHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut pg_sys::ValidateIndexState) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::ValidateIndexState {
        self.inner.as_ptr()
    }
}

/// Safe wrapper for PostgreSQL IndexBuildCallback.
#[derive(Debug)]
pub struct IndexBuildCallbackHandle<'a> {
    inner: pg_sys::IndexBuildCallback,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> IndexBuildCallbackHandle<'a> {
    /// # Safety
    ///
    /// `callback`, if present, must remain callable for `'a`.
    #[inline]
    pub unsafe fn from_raw(callback: pg_sys::IndexBuildCallback) -> Self {
        Self {
            inner: callback,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::IndexBuildCallback {
        self.inner
    }
}

/// Safe wrapper for callback state pointer.
#[derive(Debug)]
pub struct CallbackStateHandle<'a> {
    inner: PgNullable<'a, ::core::ffi::c_void>,
}

impl<'a> CallbackStateHandle<'a> {
    /// # Safety
    ///
    /// If `ptr` is non-null, it must remain valid for `'a`.
    #[inline]
    pub unsafe fn from_raw(ptr: *mut ::core::ffi::c_void) -> Self {
        Self {
            inner: unsafe { PgNullable::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> *mut ::core::ffi::c_void {
        self.inner.as_ptr()
    }
}
