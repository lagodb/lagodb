//! Plan-time interpretation of PostgreSQL predicate constants.

use core::ffi::c_char;
use std::ffi::CStr;

use pgrx::datum::Timestamp;
use pgrx::{FromDatum, pg_sys};

use super::{PgConst, PgExprRef};

impl<'a> PgConst<'a> {
    /// Return whether this is a non-NULL built-in floating-point NaN.
    ///
    /// # Safety
    ///
    /// The Const and its Datum must remain live in the current PostgreSQL
    /// memory context for this call.
    pub unsafe fn is_float_nan(self) -> bool {
        let (type_oid, _, datum, is_null) = self.parts();
        if is_null {
            return false;
        }
        match type_oid {
            pg_sys::FLOAT4OID => {
                unsafe { f32::from_datum(datum, false) }.is_some_and(f32::is_nan)
            }
            pg_sys::FLOAT8OID => {
                unsafe { f64::from_datum(datum, false) }.is_some_and(f64::is_nan)
            }
            _ => false,
        }
    }

    /// Return the value of a non-NULL timestamp-without-time-zone constant.
    ///
    /// # Safety
    ///
    /// The Const and its Datum must remain live for this call.
    pub(crate) unsafe fn timestamp(self) -> Option<Timestamp> {
        let (type_oid, _, datum, is_null) = self.parts();
        if type_oid != pg_sys::TIMESTAMPOID {
            return None;
        }
        // SAFETY: TIMESTAMP is a pass-by-value int64 Datum.
        unsafe { Timestamp::from_datum(datum, is_null) }
    }

    /// Materialize a non-NULL DATE constant derived from this constant.
    ///
    /// # Safety
    ///
    /// The current PostgreSQL memory context must be the one that owns this
    /// Const; the returned Const is allocated there.
    pub(crate) unsafe fn derived_date(self, date: pg_sys::DateADT) -> PgExprRef<'a> {
        // SAFETY: every field follows PostgreSQL's built-in DATE type metadata,
        // and makeConst allocates the node in the current memory context.
        let constant = unsafe {
            pg_sys::makeConst(
                pg_sys::DATEOID,
                -1,
                pg_sys::InvalidOid,
                4,
                pg_sys::Datum::from(date),
                false,
                true,
            )
        };
        // SAFETY: makeConst returned a live expression in that context.
        unsafe { PgExprRef::from_raw(constant.cast()) }
    }

    /// Parse PostgreSQL's canonical LIKE pattern and materialize the literal
    /// prefix for a pure `prefix%` pattern. Parsing is byte-oriented because
    /// PostgreSQL's escape and wildcard tokens are ASCII; provider admission
    /// separately proves the server encoding is UTF-8.
    ///
    /// # Safety
    ///
    /// The Const and its Datum must remain live in the current PostgreSQL
    /// memory context. The returned Const is allocated in that context.
    pub unsafe fn pure_like_prefix(self) -> Option<PgExprRef<'a>> {
        let (type_oid, collation, datum, is_null) = self.parts();
        if type_oid != pg_sys::TEXTOID || is_null {
            return None;
        }
        let pattern_ptr =
            unsafe { pg_sys::text_to_cstring(datum.cast_mut_ptr::<pg_sys::text>()) };
        let pattern = unsafe { CStr::from_ptr(pattern_ptr) }.to_bytes();
        let mut prefix = Vec::with_capacity(pattern.len());
        let mut index = 0;
        let mut terminal_wildcard = false;
        while index < pattern.len() {
            match pattern[index] {
                b'\\' => {
                    index += 1;
                    prefix.push(*pattern.get(index)?);
                }
                b'_' => return None,
                b'%' if index + 1 == pattern.len() => {
                    terminal_wildcard = true;
                }
                b'%' => return None,
                byte => prefix.push(byte),
            }
            index += 1;
        }
        if !terminal_wildcard {
            return None;
        }

        let text = unsafe {
            pg_sys::cstring_to_text_with_len(
                prefix.as_ptr().cast::<c_char>(),
                i32::try_from(prefix.len()).ok()?,
            )
        };
        let constant = unsafe {
            pg_sys::makeConst(
                pg_sys::TEXTOID,
                self.typmod(),
                collation,
                -1,
                pg_sys::Datum::from(text),
                false,
                false,
            )
        };
        Some(unsafe { PgExprRef::from_raw(constant.cast()) })
    }
}
