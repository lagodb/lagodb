//! UTF-8 text conversion (`text` / `varchar` / `bpchar` / `json` / `name`):
//! read (`Arrow → Cell`) and write (bound datum / `Cell` → Arrow builder).

use std::ffi::CStr;
use std::str;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, StringBuilder};
use lagodb_core::tuple::{Cell, DetoastedVarlena};
use pgrx::pg_sys;

use super::{ColumnAppend, cell_type_mismatch};
use crate::error::ArrowConversionResult;

pub(crate) struct Utf8Encoder {
    builder: StringBuilder,
}

impl Utf8Encoder {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            builder: StringBuilder::with_capacity(capacity, 1024),
        }
    }

    /// Append a non-NULL PostgreSQL text-family varlena datum as UTF-8.
    ///
    /// # Safety
    /// `datum` must be a valid, non-NULL PostgreSQL `text`, `varchar`,
    /// `bpchar`, or `json` varlena Datum. The relation-bound PG_UTF8
    /// server-encoding invariant must also hold.
    pub(super) unsafe fn append_text(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        // The bound buffer validates PG_UTF8 once during construction;
        // PostgreSQL's text-family input boundary guarantees these detoasted
        // bytes are valid UTF-8 for the lifetime of the plan.
        let guard = unsafe { DetoastedVarlena::from_datum(datum) };
        // SAFETY: the relation-bound writer established the PG_UTF8
        // server-encoding contract before any row was appended.
        let value = unsafe { str::from_utf8_unchecked(guard.bytes()) };
        self.builder.append_value(value);
        Ok(value.len())
    }

    /// Append a non-NULL PostgreSQL `name` datum as UTF-8.
    ///
    /// # Safety
    /// `datum` must point to a valid, non-NULL PostgreSQL `NameData`, and the
    /// relation-bound PG_UTF8 server-encoding invariant must hold.
    pub(super) unsafe fn append_name(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        // `name` is a fixed NameData C string, not a varlena.
        let bytes = unsafe {
            let name_ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
            CStr::from_ptr((*name_ptr).data.as_ptr()).to_bytes()
        };
        // SAFETY: PostgreSQL validates name input against the same PG_UTF8
        // server-encoding contract checked by the buffer.
        let value = unsafe { str::from_utf8_unchecked(bytes) };
        self.builder.append_value(value);
        Ok(value.len())
    }
}

impl ColumnAppend for Utf8Encoder {
    fn append_cell(&mut self, cell: &Cell) -> ArrowConversionResult<()> {
        let s = match cell {
            Cell::String(v) => v.as_str(),
            // SAFETY: a `StringView` cell borrows live Arrow/slot bytes; the
            // row-world build copies them into the builder synchronously here.
            Cell::StringView(v) => unsafe { v.as_str() },
            Cell::Json(v) => v.as_str(),
            _ => return Err(cell_type_mismatch("text/json")),
        };
        self.builder.append_value(s);
        Ok(())
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }

    fn len(&self) -> usize {
        self.builder.len()
    }
}

// ---------------------------------------------------------------------------
// Read (Arrow → Cell): handled by the bound `ColumnReader` in `crate::read`.
// ---------------------------------------------------------------------------
