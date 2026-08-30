use super::borrowed::{PgBorrowed, PgNullable};
use super::relation::RelationHandle;
use super::tuple::ValidItemPointer;
use pgrx::pg_sys;
use std::ffi::c_void;
use std::ptr::NonNull;

/// Callback-scoped borrowed handle for PostgreSQL `IndexInfo`.
#[derive(Debug)]
pub struct IndexInfoHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::IndexInfo>,
}

impl<'a> IndexInfoHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: *mut pg_sys::IndexInfo) -> Self {
        // SAFETY: upheld by the callback dispatcher that constructs the handle.
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        Self {
            // SAFETY: the same dispatcher-owned lifetime is represented by `'a`.
            inner: unsafe { PgBorrowed::from_non_null(ptr) },
        }
    }

    /// Return the underlying pointer for explicit PostgreSQL FFI interop.
    ///
    /// The raw pointer does not carry `'a`; it is valid only while the active
    /// index callback keeps the underlying object alive and must not be used
    /// afterward.
    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::IndexInfo {
        self.inner.as_ptr()
    }

    /// Total number of attributes stored in each index tuple, including
    /// non-key `INCLUDE` attributes.
    #[inline]
    pub fn num_index_attributes(&self) -> usize {
        unsafe { self.inner.as_ref().ii_NumIndexAttrs as usize }
    }

    /// Number of key attributes in each index tuple.
    #[inline]
    pub fn num_index_key_attributes(&self) -> usize {
        unsafe { self.inner.as_ref().ii_NumIndexKeyAttrs as usize }
    }
}

/// Callback-scoped borrowed handle for PostgreSQL `ValidateIndexState`.
#[derive(Debug)]
pub struct ValidateIndexStateHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::ValidateIndexState>,
}

impl<'a> ValidateIndexStateHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be non-null and valid for `'a`.
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: *mut pg_sys::ValidateIndexState) -> Self {
        // SAFETY: upheld by the callback dispatcher that constructs the handle.
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        Self {
            // SAFETY: the same dispatcher-owned lifetime is represented by `'a`.
            inner: unsafe { PgBorrowed::from_non_null(ptr) },
        }
    }

    /// Return the underlying pointer for explicit PostgreSQL FFI interop.
    ///
    /// The raw pointer does not carry `'a`; it is valid only while the active
    /// index callback keeps the underlying object alive and must not be used
    /// afterward.
    #[inline]
    pub fn as_raw(&self) -> *mut pg_sys::ValidateIndexState {
        self.inner.as_ptr()
    }
}

/// Callback-scoped PostgreSQL index-build callback, index relation, and opaque
/// state.
type IndexBuildCallbackFn = unsafe extern "C-unwind" fn(
    index: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    tuple_is_alive: bool,
    state: *mut c_void,
);

#[derive(Debug)]
pub struct IndexBuildCallbackHandle<'a> {
    index: PgBorrowed<'a, pg_sys::RelationData>,
    callback: IndexBuildCallbackFn,
    state: PgNullable<'a, c_void>,
}

impl<'a> IndexBuildCallbackHandle<'a> {
    /// # Safety
    ///
    /// PostgreSQL guarantees that `callback` is present when it dispatches an
    /// index build callback. `index` must be the index relation supplied with
    /// that callback. If `callback_state` is non-null, it must remain valid for
    /// the callback duration represented by `'a`.
    #[inline]
    pub(crate) unsafe fn from_raw(
        index: &'a RelationHandle<'_>,
        callback: pg_sys::IndexBuildCallback,
        callback_state: *mut c_void,
    ) -> Self {
        Self {
            // SAFETY: RelationHandle's invariant guarantees a non-null index
            // relation that remains valid for this callback-scoped borrow.
            index: unsafe {
                PgBorrowed::from_non_null(NonNull::new_unchecked(index.as_raw()))
            },
            // SAFETY: a present callback is part of the PostgreSQL table-AM
            // index-build contract documented above.
            callback: unsafe { callback.unwrap_unchecked() },
            // SAFETY: the caller guarantees the nullable state remains valid
            // for the callback duration represented by `'a`.
            state: unsafe { PgNullable::from_raw(callback_state) },
        }
    }

    /// Submit one table tuple to PostgreSQL's index-build callback.
    ///
    /// The callback always receives the index relation bound when this handle
    /// was created; callers cannot substitute the table relation or another
    /// live relation.
    ///
    /// PostgreSQL's table-AM contract uses `INDEX_MAX_KEYS` stack arrays for
    /// this hot path. Callers fill the first
    /// [`IndexInfoHandle::num_index_attributes`] entries; the callback consumes
    /// the arrays synchronously and does not retain their addresses.
    #[inline]
    pub fn invoke(
        &mut self,
        tid: ValidItemPointer,
        values: &mut [pg_sys::Datum; pg_sys::INDEX_MAX_KEYS as usize],
        is_null: &mut [bool; pg_sys::INDEX_MAX_KEYS as usize],
        tuple_is_alive: bool,
    ) {
        let mut tid = tid.to_pg_sys();
        // SAFETY: both fixed-size arrays and the local TID remain valid and
        // writable for the synchronous call. The handle owns the matching
        // callback/state pair supplied by PostgreSQL.
        unsafe {
            (self.callback)(
                self.index.as_ptr(),
                &mut tid,
                values.as_mut_ptr(),
                is_null.as_mut_ptr(),
                tuple_is_alive,
                self.state.as_ptr(),
            );
        }
    }
}
