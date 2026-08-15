//! Borrowed access to detoasted PostgreSQL varlena values.

use std::slice;

use pgrx::{pg_sys, varlena};

/// A callback-scoped detoasted varlena value.
///
/// PostgreSQL may return the original inline datum or allocate a detoasted
/// copy. This guard borrows the former and releases only the latter.
#[must_use]
pub struct DetoastedVarlena {
    original: *mut pg_sys::varlena,
    detoasted: *mut pg_sys::varlena,
}

impl DetoastedVarlena {
    /// Detoast a PostgreSQL varlena datum.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-NULL varlena datum on the current
    /// PostgreSQL backend thread and remain live for this guard's lifetime.
    pub unsafe fn from_datum(datum: pg_sys::Datum) -> Self {
        let original = datum.cast_mut_ptr::<pg_sys::varlena>();
        // SAFETY: required by this method's contract.
        let detoasted = unsafe { pg_sys::pg_detoast_datum(original) };
        Self {
            original,
            detoasted,
        }
    }

    /// The detoasted payload without its varlena header.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: construction established a live detoasted varlena and this
        // shared borrow cannot outlive the owning guard.
        unsafe {
            slice::from_raw_parts(
                varlena::vardata_any(self.detoasted).cast::<u8>(),
                varlena::varsize_any_exhdr(self.detoasted),
            )
        }
    }

    /// The complete detoasted value including its varlena header.
    #[inline]
    pub fn full_varlena_bytes(&self) -> &[u8] {
        // SAFETY: construction established a live detoasted varlena and this
        // shared borrow cannot outlive the owning guard.
        unsafe {
            slice::from_raw_parts(
                self.detoasted.cast::<u8>(),
                varlena::varsize_any(self.detoasted),
            )
        }
    }

    /// The detoasted value as a PostgreSQL Datum for synchronous PG calls.
    #[inline]
    pub fn as_datum(&self) -> pg_sys::Datum {
        pg_sys::Datum::from(self.detoasted)
    }
}

impl Drop for DetoastedVarlena {
    fn drop(&mut self) {
        if self.detoasted != self.original {
            // SAFETY: only a pointer distinct from the caller-owned original
            // can be the palloc'd detoast result owned by this guard.
            unsafe { pg_sys::pfree(self.detoasted.cast()) };
        }
    }
}
