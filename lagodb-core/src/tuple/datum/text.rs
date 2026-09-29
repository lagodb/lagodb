//! PostgreSQL text storage invariants for semantic string input.

use pgrx::IntoDatum;
use pgrx::pg_sys::{Datum, Oid};

use super::{Cell, DatumConversionError};

impl Cell {
    /// # Safety
    ///
    /// Borrowed cells must remain valid, and the PostgreSQL memory context
    /// owning the resulting text datum must be active.
    pub(super) unsafe fn into_text_datum(
        self,
        target: Oid,
    ) -> Result<Datum, DatumConversionError> {
        let text = match &self {
            Self::String(value) => value.as_str(),
            Self::StringView(view) => unsafe { view.as_str() },
            _ => return Err(DatumConversionError::incompatible(target)),
        };
        // Rust/Arrow/Avro strings permit NUL; PostgreSQL text does not.
        // Check before pgrx's byte-copying IntoDatum, including array elements.
        if text.as_bytes().contains(&0) {
            return Err(DatumConversionError::invalid_input(target));
        }
        text.into_datum()
            .ok_or(DatumConversionError::invalid_input(target))
    }
}
