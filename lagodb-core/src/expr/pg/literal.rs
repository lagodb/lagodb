//! Plan-time interpretation of PostgreSQL predicate constants.

use core::ffi::c_char;
use std::ffi::CStr;

use pgrx::datum::Timestamp;
use pgrx::{FromDatum, pg_sys};

use super::{PgConst, PgExprRef};
use crate::datum::{
    ArrayDatumLayout, NumericRounding, NumericScaledDomain, NumericScaledValue,
};

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

    /// Materialize the int4 Const equal to this NUMERIC constant. Only a
    /// finite integral value within int4's range has one: NULL, NaN, an
    /// infinity, a fraction or a larger value does not.
    ///
    /// # Safety
    ///
    /// The Const and its Datum must remain live in the current PostgreSQL
    /// memory context. The returned Const is allocated in that context.
    pub unsafe fn exact_int4(self) -> Option<PgExprRef<'a>> {
        let (type_oid, _, datum, is_null) = self.parts();
        if type_oid != pg_sys::NUMERICOID || is_null {
            return None;
        }
        // Ten integer digits hold every int4 value.
        let domain = NumericScaledDomain::new(10, 0)?;
        // SAFETY: a non-NULL NUMERIC Const owns a live datum.
        let NumericScaledValue::Finite {
            coefficient,
            exact: true,
        } = unsafe { domain.bind_datum(datum, NumericRounding::Floor) }.ok()?
        else {
            return None;
        };
        // A ten-digit coefficient fits the low eight bytes of its two's
        // complement.
        let value =
            i32::try_from(i64::from_be_bytes(*coefficient.last_chunk()?)).ok()?;
        // SAFETY: INT4 is a pass-by-value, four-byte Datum; makeConst
        // allocates the node in the current memory context.
        let constant = unsafe {
            pg_sys::makeConst(
                pg_sys::INT4OID,
                -1,
                pg_sys::InvalidOid,
                4,
                pg_sys::Datum::from(value),
                false,
                true,
            )
        };
        // SAFETY: makeConst returned a live expression in that context.
        Some(unsafe { PgExprRef::from_raw(constant.cast()) })
    }

    /// Materialize the elements of a non-NULL array constant as scalar Consts
    /// in physical order across every dimension, as PostgreSQL's partition
    /// pruning expands a `ScalarArrayOpExpr` array (`partprune.c`). Each
    /// Const takes the array's collation and typmod -1; NULL elements remain
    /// NULL Consts.
    ///
    /// # Safety
    ///
    /// The Const and its Datum must remain live in the current PostgreSQL
    /// memory context. The returned Consts and their values are allocated in
    /// that context.
    pub unsafe fn array_elements(self) -> Option<Vec<PgExprRef<'a>>> {
        let (type_oid, collation, datum, is_null) = self.parts();
        let element_oid = unsafe { pg_sys::get_element_type(type_oid) };
        if is_null || element_oid == pg_sys::InvalidOid {
            return None;
        }
        let layout = ArrayDatumLayout::bind(element_oid);
        let len = i32::from(layout.len);
        // SAFETY: a non-NULL array Const owns a live array of this element
        // type. By-reference elements borrow the iterator's detoasted array,
        // so datumCopy moves each into the current context before the
        // iterator ends; makeConst does not copy a flat value.
        let elements = unsafe { layout.values(datum) }
            .map(|value| {
                let constant = match value {
                    None => unsafe {
                        pg_sys::makeNullConst(element_oid, -1, collation)
                    },
                    Some(value) => unsafe {
                        pg_sys::makeConst(
                            element_oid,
                            -1,
                            collation,
                            len,
                            pg_sys::datumCopy(value, layout.by_value, len),
                            false,
                            layout.by_value,
                        )
                    },
                };
                // SAFETY: makeConst/makeNullConst returned a live expression
                // in the current context.
                unsafe { PgExprRef::from_raw(constant.cast()) }
            })
            .collect();
        Some(elements)
    }

    /// Whether PostgreSQL accepts this as a LIKE pattern: non-NULL text in
    /// which every escape character has a following character. `MatchText`
    /// rejects a pattern that ends with its escape character, which Arrow
    /// reads as a literal backslash. An `ESCAPE` clause has already been
    /// folded into this canonical backslash form (`like_escape`).
    pub fn is_complete_like_pattern(self) -> bool {
        let Some(pattern) = self.text_bytes() else {
            return false;
        };
        let mut bytes = pattern.iter();
        while let Some(byte) = bytes.next() {
            if *byte == b'\\' && bytes.next().is_none() {
                return false;
            }
        }
        true
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
