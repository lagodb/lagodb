//! Borrowed access to PostgreSQL tuple descriptors.

use std::slice;

use pgrx::pg_sys;

use super::borrowed::PgBorrowed;

/// Borrowed view of a PostgreSQL [`TupleDescData`](pg_sys::TupleDescData).
///
/// PostgreSQL owns the descriptor and its attribute storage. This handle keeps
/// the descriptor lifetime attached to attribute borrows and hides the PG17
/// and PG18 physical layouts behind one slice-oriented API.
#[derive(Debug, Clone, Copy)]
pub struct TupleDescHandle<'a> {
    inner: PgBorrowed<'a, pg_sys::TupleDescData>,
}

impl<'a> TupleDescHandle<'a> {
    /// # Safety
    ///
    /// `ptr` must be a valid, non-null tuple descriptor that remains live for
    /// `'a`.
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: pg_sys::TupleDesc) -> Self {
        Self {
            inner: unsafe { PgBorrowed::from_raw(ptr) },
        }
    }

    #[inline]
    pub fn as_raw(&self) -> pg_sys::TupleDesc {
        self.inner.as_ptr()
    }

    #[inline]
    pub fn len(&self) -> usize {
        unsafe { self.inner.as_ref().natts as usize }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Attribute metadata indexed by `attno - 1`.
    ///
    /// PostgreSQL stores the full attribute array contiguously in both target
    /// majors, but its position within `TupleDescData` changed in
    /// PG18. Resolving the first entry once keeps iteration and per-column
    /// access as ordinary Rust slice operations.
    #[inline]
    pub fn attributes(&self) -> &'a [pg_sys::FormData_pg_attribute] {
        let len = self.len();
        if len == 0 {
            return &[];
        }
        // SAFETY: construction guarantees a live descriptor. PG17 exposes the
        // flexible attribute array directly. PG18 stores the full attribute
        // array immediately after `natts` compact attributes; this is the
        // inlined `TupleDescAttrAddress` calculation used by PostgreSQL and
        // pgrx's `PgTupleDesc`, and avoids its out-of-line C shim here.
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        let attributes = unsafe { self.inner.as_ref().attrs.as_ptr() };
        #[cfg(feature = "pg18")]
        let attributes =
            unsafe { self.inner.as_ref().compact_attrs.as_ptr().add(len).cast() };
        unsafe { slice::from_raw_parts(attributes, len) }
    }
}
