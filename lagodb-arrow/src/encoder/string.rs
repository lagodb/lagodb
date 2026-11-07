//! UTF-8 Arrow builder for PostgreSQL `text` / `varchar` / `bpchar` / `json`
//! varlena datums and fixed `name` datums.

use std::ffi::CStr;
use std::str;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, StringBuilder};
use lagodb_core::datum::DetoastedVarlena;
use pgrx::pg_sys;

use super::ColumnBuilder;
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
    /// `bpchar`, or `json` varlena Datum. The bound PG_UTF8 server-encoding
    /// invariant must also hold.
    pub(crate) unsafe fn append_text(&mut self, datum: pg_sys::Datum) -> usize {
        let guard = unsafe { DetoastedVarlena::from_datum(datum) };
        // SAFETY: the bound writer established the PG_UTF8 server-encoding
        // contract before any row was appended, and PostgreSQL's text-family
        // input boundary guarantees these bytes are valid in that encoding.
        let value = unsafe { str::from_utf8_unchecked(guard.bytes()) };
        self.builder.append_value(value);
        value.len()
    }

    /// Append a non-NULL PostgreSQL `name` datum as UTF-8.
    ///
    /// # Safety
    /// `datum` must point to a valid, non-NULL PostgreSQL `NameData`, and the
    /// bound PG_UTF8 server-encoding invariant must hold.
    pub(crate) unsafe fn append_name(&mut self, datum: pg_sys::Datum) -> usize {
        // `name` is a fixed NameData C string, not a varlena.
        let bytes = unsafe {
            let name_ptr = datum.cast_mut_ptr::<pg_sys::NameData>();
            CStr::from_ptr((*name_ptr).data.as_ptr()).to_bytes()
        };
        // SAFETY: PostgreSQL validates name input against the same PG_UTF8
        // server-encoding contract checked at binding.
        let value = unsafe { str::from_utf8_unchecked(bytes) };
        self.builder.append_value(value);
        value.len()
    }
}

impl ColumnBuilder for Utf8Encoder {
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
