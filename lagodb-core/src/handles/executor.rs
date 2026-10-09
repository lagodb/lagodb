//! Borrowed executor lifetime and visibility at scan binding boundaries.

use pgrx::{PgMemoryContexts, pg_sys};

use super::SnapshotHandle;
use super::borrowed::PgBorrowed;

/// A live executor. Provider state retained here is released on normal query
/// teardown and on PostgreSQL ERROR, independently of scan-end callbacks.
pub struct ExecutorHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::EState>,
}

impl<'a> ExecutorHandle<'a> {
    /// # Safety
    ///
    /// `estate` must be an initialized executor that remains live for `'a`.
    pub unsafe fn from_raw(estate: *mut pg_sys::EState) -> Self {
        Self {
            // SAFETY: the caller supplies the live executor.
            inner: unsafe { PgBorrowed::from_raw(estate) },
        }
    }

    /// Identity for executor-local provider bindings; never a plan cache key.
    pub fn as_raw(&self) -> *mut pg_sys::EState {
        self.inner.as_ptr()
    }

    /// Borrow the snapshot already selected and registered by PostgreSQL.
    pub fn snapshot(&self) -> SnapshotHandle<'_> {
        // SAFETY: an initialized executor owns es_snapshot for its lifetime.
        unsafe { SnapshotHandle::from_raw(self.inner.as_ref().es_snapshot) }
    }

    /// Retain an owning Rust value until this executor's context is deleted.
    /// The value must not borrow callback-local PostgreSQL data.
    pub fn retain<T: 'static>(&self, value: T) {
        // SAFETY: es_query_cxt is owned by this live executor.
        let context = unsafe { self.inner.as_ref().es_query_cxt };
        PgMemoryContexts::For(context).leak_and_drop_on_delete(value);
    }
}
