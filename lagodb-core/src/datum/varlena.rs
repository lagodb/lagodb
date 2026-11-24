//! Borrowed access to detoasted PostgreSQL varlena values.

use std::slice;

use pgrx::{pg_sys, varlena_to_byte_slice};

struct VarlenaGuard {
    original: *mut pg_sys::varlena,
    detoasted: *mut pg_sys::varlena,
}

impl VarlenaGuard {
    #[inline]
    fn as_datum(&self) -> pg_sys::Datum {
        pg_sys::Datum::from(self.detoasted)
    }
}

impl Drop for VarlenaGuard {
    fn drop(&mut self) {
        if self.detoasted != self.original {
            // SAFETY: only a pointer distinct from the caller-owned original
            // can be the palloc'd detoast result owned by this guard.
            unsafe { pg_sys::pfree(self.detoasted.cast()) };
        }
    }
}

/// A callback-scoped varlena detoasted to PostgreSQL's aligned 4-byte layout.
///
/// PostgreSQL may return the original inline datum or allocate a detoasted
/// copy. This guard borrows the former and releases only the latter.
#[must_use]
pub struct DetoastedVarlena(VarlenaGuard);

impl DetoastedVarlena {
    /// Detoast a PostgreSQL varlena datum to its aligned 4-byte layout.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-NULL varlena datum on the current
    /// PostgreSQL backend thread and remain live for this guard's lifetime.
    #[inline]
    pub unsafe fn from_datum(datum: pg_sys::Datum) -> Self {
        let original = datum.cast_mut_ptr::<pg_sys::varlena>();
        // SAFETY: required by this method's contract.
        let detoasted = unsafe { pg_sys::pg_detoast_datum(original) };
        Self(VarlenaGuard {
            original,
            detoasted,
        })
    }

    /// The detoasted payload without its 4-byte varlena header.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: `pg_detoast_datum` guarantees a live, uncompressed 4-byte
        // varlena header (`VARATT_IS_4B_U`), and this shared borrow cannot
        // outlive the owning guard.
        unsafe {
            slice::from_raw_parts(
                pg_sys::VARDATA!(self.0.detoasted).get().cast::<u8>(),
                self.total_len() - pg_sys::VARHDRSZ as usize,
            )
        }
    }

    /// Total byte length of the detoasted value including its 4-byte header.
    #[inline]
    pub fn total_len(&self) -> usize {
        // SAFETY: `pg_detoast_datum` guarantees a live, uncompressed 4-byte
        // varlena header (`VARATT_IS_4B_U`).
        unsafe { pg_sys::VARSIZE!(self.0.detoasted).get() as usize }
    }

    /// The detoasted value as a PostgreSQL Datum for synchronous PG calls.
    #[inline]
    pub fn as_datum(&self) -> pg_sys::Datum {
        self.0.as_datum()
    }
}

/// A callback-scoped packed varlena for alignment-oblivious consumers.
///
/// Inline short-header values remain borrowed without a palloc/copy. Payload
/// bytes must not be cast to a type that requires alignment.
#[must_use]
pub struct PackedDetoastedVarlena(VarlenaGuard);

impl PackedDetoastedVarlena {
    /// Detoast external or compressed storage while preserving packed inline
    /// values.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-NULL varlena datum on the current
    /// PostgreSQL backend thread and remain live for this guard's lifetime.
    #[inline]
    pub unsafe fn from_datum(datum: pg_sys::Datum) -> Self {
        let original = datum.cast_mut_ptr::<pg_sys::varlena>();
        // SAFETY: required by this method's contract.
        let detoasted = unsafe { pg_sys::pg_detoast_datum_packed(original) };
        Self(VarlenaGuard {
            original,
            detoasted,
        })
    }

    /// The alignment-oblivious payload without its varlena header.
    #[inline]
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: `pg_detoast_datum_packed` guarantees a live, uncompressed
        // ordinary or short-header varlena, and this shared borrow cannot
        // outlive the owning guard.
        unsafe { varlena_to_byte_slice(self.0.detoasted) }
    }

    /// The packed value as a Datum for PostgreSQL functions that accept packed
    /// varlena inputs.
    #[inline]
    pub fn as_datum(&self) -> pg_sys::Datum {
        self.0.as_datum()
    }
}
