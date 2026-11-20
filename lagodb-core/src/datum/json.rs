//! Owned semantic values for PostgreSQL `json` and `jsonb`.
//!
//! These types deliberately do not expose PostgreSQL's internal varlena
//! representation.  A provider that chooses a physical encoding for JSONB
//! must keep that encoding at its own storage boundary.

mod object_encoder;

use std::ffi::{CString, NulError, c_char};
use std::fmt;

use pgrx::pg_sys::{self, Datum};
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{fcinfo, varlena};
use thiserror::Error;

use crate::diag::{PgError, SqlStateError};

use super::error::DatumConversionError;
use super::output_cstring::PgOutputCString;
use super::varlena::DetoastedVarlena;

pub use object_encoder::{BoundJsonObjectEncoder, JsonDatumEncoder};

/// PostgreSQL's JSON representation class for a bound column type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonDatumKind {
    Boolean,
    Numeric,
    String,
    Json,
    Array,
    Composite,
    Cast,
}

/// An owned, PostgreSQL-validated `json` value.
///
/// PostgreSQL's `json` type preserves the input text, including whitespace,
/// object-key order, and duplicate keys.  The framework therefore keeps the
/// validated text rather than parsing it into a lossy generic JSON tree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsonText {
    text: Box<str>,
}

/// An owned semantic `jsonb` value.
///
/// The text is produced by PostgreSQL's `jsonb_out` and is used only as an
/// owned semantic representation.  It is not PostgreSQL's internal varlena
/// bytes and is not a storage-format contract.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsonbValue {
    text: Box<str>,
}

/// Errors raised while converting PostgreSQL and semantic JSON values.
#[derive(Debug, Error)]
pub enum JsonValueError {
    #[error(
        "expected a finite ISO date/time in the JSONB encoding range (UTC uses +00:00)"
    )]
    InvalidDatetime,
    #[error("JSONB strings cannot contain a NUL byte")]
    InvalidJsonbString,

    #[error("JSON text contains an interior NUL byte: {0}")]
    Nul(#[from] NulError),

    #[error("PostgreSQL JSON object encoding does not support {0:?} values")]
    UnsupportedOutputKind(JsonDatumKind),

    #[error("PostgreSQL JSON output exceeds its buffer or representation limit")]
    OutputTooLarge,
}

impl SqlStateError for JsonValueError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::InvalidDatetime => PgSqlErrorCode::ERRCODE_INVALID_DATETIME_FORMAT,
            Self::Nul(_) => PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION,
            Self::InvalidJsonbString => {
                PgSqlErrorCode::ERRCODE_UNTRANSLATABLE_CHARACTER
            }
            Self::UnsupportedOutputKind(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::OutputTooLarge => PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        }
    }
}

impl JsonText {
    /// Call PostgreSQL's validating `json_in` input function for a
    /// NUL-terminated string.
    ///
    /// This parses the input. PG errors unwind to the enclosing operation or
    /// provider/FFI boundary; array/composite leaves do not catch individually.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live NUL-terminated string for the duration of
    /// the PostgreSQL call.
    pub(crate) unsafe fn input_from_cstr(ptr: *const c_char) -> Option<Datum> {
        // SAFETY: the caller keeps the NUL-terminated input live. pgrx's PG
        // binding translates PG ERROR into unwinding through Rust frames.
        unsafe {
            fcinfo::direct_function_call_as_datum(
                pg_sys::json_in,
                &[Some(Datum::from(ptr))],
            )
        }
    }

    /// Read a PostgreSQL `json` Datum into an owned semantic value.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid PostgreSQL `json` Datum and PostgreSQL must be
    /// running on the current backend thread. The semantic JSON conversion
    /// contract requires a UTF-8 server encoding; callers must validate that
    /// capability once while binding the relation/column conversion plan.
    pub(crate) unsafe fn from_datum(
        datum: Datum,
        is_null: bool,
    ) -> Result<Option<Self>, DatumConversionError> {
        if is_null {
            return Ok(None);
        }

        let original = datum.cast_mut_ptr::<pg_sys::varlena>();
        let detoasted = unsafe { pg_sys::pg_detoast_datum_packed(original) };
        let text = unsafe { varlena::text_to_rust_str(detoasted) }
            .map(str::to_owned)
            .map_err(|_| DatumConversionError::InvalidUtf8 {
                target: pg_sys::JSONOID,
            });
        if detoasted != original {
            unsafe { pg_sys::pfree(detoasted.cast()) };
        }
        let text = text?;
        Ok(Some(Self {
            text: text.into_boxed_str(),
        }))
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Build a PostgreSQL `json` Datum through the already-bound semantic
    /// conversion path. The public value type deliberately does not implement
    /// `IntoDatum`, whose `Option<Datum>` return type cannot preserve errors.
    pub(crate) unsafe fn to_datum_checked(
        &self,
    ) -> Result<Datum, DatumConversionError> {
        let len = i32::try_from(self.text.len()).map_err(|_| {
            DatumConversionError::OutOfRange {
                target: pg_sys::JSONOID,
            }
        })?;
        // SAFETY: `self.text` contains `len` initialized bytes and PostgreSQL
        // owns the returned text allocation in the current memory context.
        Ok(Datum::from(unsafe {
            pg_sys::cstring_to_text_with_len(self.text.as_ptr().cast(), len)
        }))
    }
}

impl fmt::Display for JsonText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl JsonbValue {
    /// Call PostgreSQL's validating `jsonb_in` input function for a
    /// NUL-terminated string.
    /// PG errors unwind to the enclosing operation or provider/FFI boundary.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live NUL-terminated string for the duration of
    /// the PostgreSQL call.
    pub(crate) unsafe fn input_from_cstr(ptr: *const c_char) -> Option<Datum> {
        // SAFETY: the caller keeps the NUL-terminated input live. pgrx's PG
        // binding translates PG ERROR into unwinding through Rust frames.
        unsafe {
            fcinfo::direct_function_call_as_datum(
                pg_sys::jsonb_in,
                &[Some(Datum::from(ptr))],
            )
        }
    }

    /// Read a PostgreSQL `jsonb` Datum into an owned semantic value without
    /// passing through `serde_json::Value`.
    ///
    /// `jsonb_out` preserves PostgreSQL's numeric spelling and does not impose
    /// serde_json's recursion limit.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid PostgreSQL `jsonb` Datum and PostgreSQL must be
    /// running on the current backend thread. The semantic JSONB conversion
    /// contract requires a UTF-8 server encoding; callers must validate that
    /// capability once while binding the relation/column conversion plan.
    pub(crate) unsafe fn from_datum(
        datum: Datum,
        is_null: bool,
    ) -> Result<Option<Self>, DatumConversionError> {
        if is_null {
            return Ok(None);
        }

        let detoasted = unsafe { DetoastedVarlena::from_datum(datum) };
        let output = unsafe {
            PgOutputCString::from_function_call(
                pg_sys::jsonb_out,
                &[Some(detoasted.as_datum())],
            )
        }
        .ok_or(DatumConversionError::InvalidInput {
            target: pg_sys::JSONBOID,
        })?;
        let text = Self::output_text(output).map_err(|_| {
            DatumConversionError::InvalidUtf8 {
                target: pg_sys::JSONBOID,
            }
        })?;
        Ok(Some(Self { text }))
    }

    fn output_text(output: PgOutputCString) -> Result<Box<str>, std::str::Utf8Error> {
        output.as_cstr().to_str().map(Box::<str>::from)
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Build a PostgreSQL JSONB Datum through validating input. PG errors
    /// unwind to the enclosing operation or provider/FFI boundary.
    pub(crate) unsafe fn to_datum_checked(&self) -> Result<Option<Datum>, PgError> {
        let input = CString::new(self.as_str()).map_err(PgError::from)?;
        // SAFETY: input is NUL-terminated and lives through the PG call.
        Ok(unsafe { Self::input_from_cstr(input.as_ptr()) })
    }
}

impl fmt::Display for JsonbValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
