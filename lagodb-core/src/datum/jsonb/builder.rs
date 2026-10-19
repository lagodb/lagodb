//! Token-based JSONB construction using PostgreSQL's own container builder.
//!
//! Borrowed strings live through `build`; generated strings and numerics belong
//! to the caller's current row context. PostgreSQL errors unwind to the enclosing
//! provider/FFI boundary.

mod datetime;

use std::ffi::c_char;
use std::fmt;
use std::io::Write;
use std::marker::PhantomData;
use std::ptr;
use std::slice;

use pgrx::pg_sys::{self, JsonbIteratorToken as Token, jbvType};

use crate::datum::json::JsonValueError;
use crate::wrapper::PgWrapper;

/// The position of a scalar in its enclosing JSONB container.
#[derive(Clone, Copy)]
pub enum JsonbPosition {
    Key,
    Value,
    Element,
}

impl JsonbPosition {
    fn token(self) -> Token::Type {
        match self {
            Self::Key => Token::WJB_KEY,
            Self::Value => Token::WJB_VALUE,
            Self::Element => Token::WJB_ELEM,
        }
    }
}

/// A single JSONB value built in the current PostgreSQL memory context.
///
/// This builder does not own a memory context. Its intermediate allocations
/// are reclaimed with the caller's row context, just like `to_jsonb` in PG.
pub struct JsonbBuilder<'a> {
    state: *mut pg_sys::JsonbParseState,
    result: *mut pg_sys::JsonbValue,
    borrowed: PhantomData<&'a str>,
}

impl<'a> JsonbBuilder<'a> {
    /// Construct one container. The callback's errors propagate without being
    /// reported here; PG errors unwind to the enclosing provider/FFI boundary.
    ///
    /// # Safety
    /// A backend and its destination row context must be active. `append` must
    /// emit exactly one balanced array/object. Borrowed strings and numeric
    /// storage must remain live through serialization, until `build` returns.
    pub unsafe fn build<E>(
        append: impl FnOnce(&mut Self) -> Result<(), E>,
    ) -> Result<pg_sys::Datum, E> {
        let mut builder = Self {
            state: ptr::null_mut(),
            result: ptr::null_mut(),
            borrowed: PhantomData,
        };
        append(&mut builder)?;
        // SAFETY: the callback completed one container with live payloads.
        Ok(unsafe { builder.finish() })
    }

    /// Check the additional restrictions JSONB places on a UTF-8 string.
    /// Schema keys can be checked once when their conversion plan is built.
    pub fn validate_string(value: &str) -> Result<(), JsonValueError> {
        if value.len() > pg_sys::JENTRY_OFFLENMASK as usize {
            return Err(JsonValueError::OutputTooLarge);
        }
        if value.as_bytes().contains(&0) {
            return Err(JsonValueError::InvalidJsonbString);
        }
        Ok(())
    }

    /// Begin an object, checking the backend stack before recursive traversal.
    ///
    /// # Safety
    /// Must occupy a value position and be paired with `end_object`.
    pub unsafe fn begin_object(&mut self) {
        unsafe {
            pg_sys::check_stack_depth();
            self.container(Token::WJB_BEGIN_OBJECT);
        }
    }

    /// End an object after all of its key/value pairs have been emitted.
    ///
    /// # Safety
    /// Must match the innermost unfinished `begin_object`.
    pub unsafe fn end_object(&mut self) {
        unsafe { self.container(Token::WJB_END_OBJECT) };
    }

    /// Begin an array, checking the backend stack before recursive traversal.
    ///
    /// # Safety
    /// Must occupy a value position and be paired with `end_array`.
    pub unsafe fn begin_array(&mut self) {
        unsafe {
            pg_sys::check_stack_depth();
            self.container(Token::WJB_BEGIN_ARRAY);
        }
    }

    /// End an array after all of its elements have been emitted.
    ///
    /// # Safety
    /// Must match the innermost unfinished `begin_array`.
    pub unsafe fn end_array(&mut self) {
        unsafe { self.container(Token::WJB_END_ARRAY) };
    }

    unsafe fn container(&mut self, token: Token::Type) {
        // SAFETY: the caller supplies a valid container transition.
        self.result = unsafe {
            pg_sys::pushJsonbValue(&mut self.state, token, ptr::null_mut())
        };
    }

    /// Append a JSON null.
    ///
    /// # Safety
    /// `position` must be Value or Element at a scalar position in the stream.
    /// PostgreSQL must be active on the current thread.
    pub unsafe fn null(&mut self, position: JsonbPosition) {
        let value = pg_sys::JsonbValue {
            type_: jbvType::jbvNull,
            val: pg_sys::JsonbValue__bindgen_ty_1 { boolean: false },
        };
        // SAFETY: the caller established the scalar position.
        unsafe { self.scalar(position, value) };
    }

    /// Append a boolean.
    ///
    /// # Safety
    /// Has the same token/backend requirements as `null`.
    pub unsafe fn boolean(&mut self, position: JsonbPosition, value: bool) {
        let value = pg_sys::JsonbValue {
            type_: jbvType::jbvBool,
            val: pg_sys::JsonbValue__bindgen_ty_1 { boolean: value },
        };
        // SAFETY: the caller established the scalar position.
        unsafe { self.scalar(position, value) };
    }

    /// Append a NUMERIC datum without text conversion or copying its payload.
    ///
    /// # Safety
    /// In addition to `null`'s requirements, `value` must be a non-null,
    /// detoasted, finite NUMERIC that remains live through `finish`.
    pub unsafe fn numeric(&mut self, position: JsonbPosition, value: pg_sys::Datum) {
        let value = pg_sys::JsonbValue {
            type_: jbvType::jbvNumeric,
            val: pg_sys::JsonbValue__bindgen_ty_1 {
                numeric: value.cast_mut_ptr(),
            },
        };
        // SAFETY: the caller established the datum and token contracts.
        unsafe { self.scalar(position, value) };
    }

    /// Append an integer without formatting it as text.
    ///
    /// # Safety
    /// Has the same token/backend requirements as `null`.
    pub unsafe fn integer(&mut self, position: JsonbPosition, value: i64) {
        // SAFETY: PG returns a fresh finite Numeric in the current context.
        let numeric = unsafe { pg_sys::int64_to_numeric(value) };
        unsafe { self.numeric(position, pg_sys::Datum::from(numeric)) };
    }

    /// Append a float without rounding its precision to session display settings.
    /// Finite values except negative zero use PG's shortest-decimal representation
    /// as JSONB numeric, independent of extra_float_digits. Negative zero uses
    /// the string "-0.0" so nested values retain its sign when written back.
    /// Non-finite values use the same strings as PostgreSQL's `to_jsonb`.
    ///
    /// # Safety
    /// Has the same token/backend requirements as `null`. `single_precision`
    /// indicates an input originating from a float4 rather than float8.
    pub unsafe fn float(
        &mut self,
        position: JsonbPosition,
        value: f64,
        single_precision: bool,
    ) {
        if value == 0.0 && value.is_sign_negative() {
            // PG Numeric normalizes every zero to a positive sign, even when
            // numeric_in receives "-0" from the shortest-decimal formatter.
            // This is a reversible storage representation, not to_jsonb's
            // numeric semantics: use the special-float string contract shared
            // with the schema-bound writer, including for untouched UPDATE columns.
            // SAFETY: this static string satisfies all JSONB restrictions.
            unsafe { self.string_unchecked(position, "-0.0") };
            return;
        }
        if !value.is_finite() {
            let text = if value.is_nan() {
                "NaN"
            } else if value.is_sign_negative() {
                "-Infinity"
            } else {
                "Infinity"
            };
            // SAFETY: these static strings satisfy all JSONB restrictions.
            unsafe { self.string_unchecked(position, text) };
            return;
        }
        // SAFETY: the input is finite and the caller supplies its precision
        // and row context. PG errors reach the enclosing provider/FFI boundary.
        let numeric =
            unsafe { PgWrapper::jsonb_numeric_from_float(value, single_precision) };
        // SAFETY: the wrapper allocated a finite Numeric in the row context.
        unsafe { self.numeric(position, numeric) };
    }

    /// Append a borrowed UTF-8 string or object key.
    ///
    /// # Safety
    /// `position` must be Key, Value or Element at the corresponding
    /// position in the stream. PostgreSQL must be active on this thread.
    pub unsafe fn string(
        &mut self,
        position: JsonbPosition,
        value: &'a str,
    ) -> Result<(), JsonValueError> {
        Self::validate_string(value)?;
        // SAFETY: string restrictions were checked above; token is caller-bound.
        unsafe { self.string_unchecked(position, value) };
        Ok(())
    }

    /// Append a string already checked while binding the schema.
    ///
    /// # Safety
    /// Has `string`'s requirements, and `validate_string(value)` must succeed.
    pub unsafe fn string_unchecked(
        &mut self,
        position: JsonbPosition,
        value: &'a str,
    ) {
        // SAFETY: the lifetime records the borrowed storage until finish.
        unsafe {
            self.string_pointer(
                position,
                value.as_ptr().cast_mut().cast(),
                value.len(),
            )
        };
    }

    /// Format a generated scalar into PostgreSQL-owned storage.
    ///
    /// # Safety
    /// Has `string`'s token/backend requirements. The Display implementations
    /// used by `value` must emit valid UTF-8 without NUL bytes. This scalar
    /// formatter supports at most 128 bytes (UUID representations fit);
    /// larger output returns an error.
    pub unsafe fn formatted_string(
        &mut self,
        position: JsonbPosition,
        value: fmt::Arguments<'_>,
    ) -> Result<(), JsonValueError> {
        let mut buffer = [0_u8; 128];
        let mut remaining = buffer.as_mut_slice();
        remaining
            .write_fmt(value)
            .map_err(|_| JsonValueError::OutputTooLarge)?;
        let len = 128 - remaining.len();
        // SAFETY: the formatted value fits the initialized stack buffer and
        // the copy is kept by the row context until serialization completes.
        unsafe { self.copy_string(position, &buffer[..len]) };
        Ok(())
    }

    /// Copy generated text into the current row context for serialization.
    ///
    /// # Safety
    /// Has `string`'s token/backend requirements. `value` must be valid UTF-8
    /// without NUL bytes and no larger than JENTRY_OFFLENMASK.
    unsafe fn copy_string(&mut self, position: JsonbPosition, value: &[u8]) {
        // SAFETY: allocate disjoint PG storage and retain it until the row
        // context is reset, after the completed JSONB has been serialized.
        unsafe {
            let text = pg_sys::palloc(value.len().max(1)).cast::<u8>();
            ptr::copy_nonoverlapping(value.as_ptr(), text, value.len());
            self.string_pointer(position, text.cast(), value.len());
        }
    }

    /// Encode binary as lowercase hexadecimal, without a text prefix.
    ///
    /// # Safety
    /// Has the same token/backend requirements as `string`.
    pub unsafe fn hex_string(
        &mut self,
        position: JsonbPosition,
        value: &[u8],
    ) -> Result<(), JsonValueError> {
        if value.len() > pg_sys::JENTRY_OFFLENMASK as usize / 2 {
            return Err(JsonValueError::OutputTooLarge);
        }
        let len = value.len() * 2;
        // SAFETY: the checked size fits PG's allocation and JSONB limits.
        let ptr = unsafe { pg_sys::palloc(len.max(1)).cast::<u8>() };
        let bytes = unsafe { slice::from_raw_parts_mut(ptr, len) };
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (pair, byte) in bytes.chunks_exact_mut(2).zip(value) {
            pair[0] = HEX[(byte >> 4) as usize];
            pair[1] = HEX[(byte & 15) as usize];
        }
        // SAFETY: hexadecimal is valid UTF-8 without NUL, owned by the context.
        unsafe { self.string_pointer(position, ptr.cast(), len) };
        Ok(())
    }

    /// Serialize the completed container to a self-contained JSONB datum.
    ///
    /// # Safety
    /// The stream must contain exactly one completed array or object. All
    /// borrowed numeric storage must remain valid. PostgreSQL must be active.
    unsafe fn finish(self) -> pg_sys::Datum {
        // SAFETY: the caller completed the balanced token stream.
        pg_sys::Datum::from(unsafe { pg_sys::JsonbValueToJsonb(self.result) })
    }

    unsafe fn string_pointer(
        &mut self,
        position: JsonbPosition,
        ptr: *mut c_char,
        len: usize,
    ) {
        let value = pg_sys::JsonbValue {
            type_: jbvType::jbvString,
            val: pg_sys::JsonbValue__bindgen_ty_1 {
                string: pg_sys::JsonbValue__bindgen_ty_1__bindgen_ty_1 {
                    len: len as i32,
                    val: ptr,
                },
            },
        };
        // SAFETY: callers provide valid, live storage and a scalar token.
        unsafe { self.scalar(position, value) };
    }

    unsafe fn scalar(
        &mut self,
        position: JsonbPosition,
        mut value: pg_sys::JsonbValue,
    ) {
        // SAFETY: callers establish the token, tag and payload invariants.
        self.result = unsafe {
            pg_sys::pushJsonbValue(&mut self.state, position.token(), &mut value)
        };
    }
}
