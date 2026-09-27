//! Relation-bound PostgreSQL assignment of external COPY datums.

use std::ffi::c_void;
use std::panic::AssertUnwindSafe;
use std::ptr::NonNull;

use pgrx::{PgTryBuilder, pg_sys};

use crate::diag::PgError;

use super::pg::CopyBridge;

/// A cached target typmod expression, independent of source file formats.
///
/// PostgreSQL owns the coercion semantics for scalars and array elements.
/// Unconstrained targets retain no expression. This plan owns its PostgreSQL
/// memory context and must be dropped during the utility command that bound it.
pub struct CopyDatumCoercion {
    state: Option<NonNull<c_void>>,
}

impl CopyDatumCoercion {
    /// `source_typmod` is the typmod already enforced by the source decoder,
    /// or -1 when decoding produced an unconstrained value. As in PG's parser,
    /// matching source and target typmods need no additional conversion.
    pub fn bind(
        type_oid: pg_sys::Oid,
        source_typmod: i32,
        target_typmod: i32,
    ) -> Result<Self, PgError> {
        let state = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Ok(CopyBridge::begin_datum_coercion(
                    type_oid,
                    source_typmod,
                    target_typmod,
                ))
            }))
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }?;
        Ok(Self {
            state: NonNull::new(state),
        })
    }

    /// Apply target assignment without serializing or reparsing the datum.
    ///
    /// # Safety
    ///
    /// `value` must have the bound column's type. PostgreSQL must be active
    /// with COPY's per-tuple context selected. The caller must catch PG errors
    /// at its row conversion boundary, where ON_ERROR can classify rejection.
    pub unsafe fn apply(
        &mut self,
        value: Option<pg_sys::Datum>,
    ) -> Option<pg_sys::Datum> {
        match (self.state, value) {
            (Some(state), Some(value)) => {
                Some(unsafe { CopyBridge::coerce_datum(state.as_ptr(), value) })
            }
            (_, value) => value,
        }
    }
}

impl Drop for CopyDatumCoercion {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            // SAFETY: this object exclusively owns the context returned by
            // binding; deleting that context does not execute PG expressions.
            unsafe { CopyBridge::end_datum_coercion(state.as_ptr()) };
        }
    }
}
