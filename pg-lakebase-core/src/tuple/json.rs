//! Owned semantic values for PostgreSQL `json` and `jsonb`.
//!
//! These types deliberately do not expose PostgreSQL's internal varlena
//! representation.  A provider that chooses a physical encoding for JSONB
//! must keep that encoding at its own storage boundary.

use std::ffi::{CString, NulError, c_void};
use std::fmt;
use std::mem::size_of;
use std::os::raw::c_uint;
use std::ptr::NonNull;

use pgrx::pg_sys::{self, Datum};
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{PgTryBuilder, fcinfo, varlena};
use thiserror::Error;

use crate::diag::{PgError, SqlStateError};
use crate::wrapper::{PgOutputCString, PgWrapper};

use super::datum::{ColumnDatumTarget, DatumConversionError};

type PgJsonTypeCategory = c_uint;

const JSONTYPE_NULL: PgJsonTypeCategory = 0;
const JSONTYPE_BOOL: PgJsonTypeCategory = 1;
const JSONTYPE_NUMERIC: PgJsonTypeCategory = 2;
const JSONTYPE_DATE: PgJsonTypeCategory = 3;
const JSONTYPE_TIMESTAMP: PgJsonTypeCategory = 4;
const JSONTYPE_TIMESTAMPTZ: PgJsonTypeCategory = 5;
const JSONTYPE_JSON: PgJsonTypeCategory = 6;
const JSONTYPE_JSONB: PgJsonTypeCategory = 7;
const JSONTYPE_ARRAY: PgJsonTypeCategory = 8;
const JSONTYPE_COMPOSITE: PgJsonTypeCategory = 9;
const JSONTYPE_CAST: PgJsonTypeCategory = 10;
const JSONTYPE_OTHER: PgJsonTypeCategory = 11;

unsafe extern "C-unwind" {
    #[link_name = "json_categorize_type"]
    fn pg_json_categorize_type(
        type_oid: pg_sys::Oid,
        is_jsonb: bool,
        category: *mut PgJsonTypeCategory,
        output_function: *mut pg_sys::Oid,
    );

    #[link_name = "datum_to_json"]
    fn pg_datum_to_json(
        datum: Datum,
        category: PgJsonTypeCategory,
        output_function: pg_sys::Oid,
    ) -> Datum;
}

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
    Unsupported,
}

/// Begin-time PostgreSQL Datum-to-JSON conversion plan.
#[derive(Clone, Copy, Debug)]
pub struct JsonDatumEncoder {
    category: PgJsonTypeCategory,
    output_function: pg_sys::Oid,
    kind: JsonDatumKind,
}

/// One PostgreSQL-owned JSON text datum.
pub struct EncodedJson {
    text: NonNull<pg_sys::varlena>,
}

/// Relation-shaped slot encoder backed by PostgreSQL `row_to_json`.
pub struct JsonRowEncoder;

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

/// Owns a detoast copy for the duration of one semantic JSONB conversion.
struct DetoastedJsonb {
    original: *mut pg_sys::varlena,
    detoasted: *mut pg_sys::varlena,
}

impl DetoastedJsonb {
    unsafe fn new(datum: Datum) -> Self {
        let original = datum.cast_mut_ptr::<pg_sys::varlena>();
        let detoasted = unsafe { pg_sys::pg_detoast_datum(original) };
        Self {
            original,
            detoasted,
        }
    }

    #[inline]
    fn datum(&self) -> Datum {
        Datum::from(self.detoasted)
    }
}

impl Drop for DetoastedJsonb {
    fn drop(&mut self) {
        if self.detoasted != self.original {
            unsafe { pg_sys::pfree(self.detoasted.cast()) };
        }
    }
}

/// Errors raised while constructing a semantic JSON value from text.
#[derive(Debug, Error)]
pub enum JsonValueError {
    #[error("JSON text contains an interior NUL byte: {0}")]
    Nul(#[from] NulError),

    #[error("PostgreSQL rejected JSON input: {0}")]
    Postgres(#[from] PgError),

    #[error("PostgreSQL JSON input returned NULL")]
    NullInput,

    #[error(
        "semantic JSON conversion requires UTF-8 server encoding, found encoding {encoding}"
    )]
    UnsupportedServerEncoding { encoding: i32 },

    #[error("JSON value is not valid UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}

impl JsonDatumEncoder {
    /// Bind PostgreSQL's JSON category and output function once for a type.
    pub fn bind(type_oid: pg_sys::Oid) -> Result<Self, JsonValueError> {
        let mut category = JSONTYPE_NULL;
        let mut output_function = pg_sys::InvalidOid;
        unsafe {
            PgTryBuilder::new(|| {
                pg_json_categorize_type(
                    type_oid,
                    false,
                    &mut category,
                    &mut output_function,
                );
                Ok(())
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }?;
        let kind = match category {
            JSONTYPE_BOOL => JsonDatumKind::Boolean,
            JSONTYPE_NUMERIC => JsonDatumKind::Numeric,
            JSONTYPE_DATE
            | JSONTYPE_TIMESTAMP
            | JSONTYPE_TIMESTAMPTZ
            | JSONTYPE_OTHER => JsonDatumKind::String,
            JSONTYPE_JSON | JSONTYPE_JSONB => JsonDatumKind::Json,
            JSONTYPE_ARRAY => JsonDatumKind::Array,
            JSONTYPE_COMPOSITE => JsonDatumKind::Composite,
            JSONTYPE_CAST => JsonDatumKind::Cast,
            _ => JsonDatumKind::Unsupported,
        };
        Ok(Self {
            category,
            output_function,
            kind,
        })
    }

    #[inline]
    pub const fn kind(self) -> JsonDatumKind {
        self.kind
    }

    /// Encode a non-NULL Datum with the cached PostgreSQL JSON plan.
    ///
    /// # Safety
    ///
    /// `datum` must be valid for the type used to construct this encoder. The
    /// active PostgreSQL memory context must outlive the returned value.
    pub unsafe fn encode(self, datum: Datum) -> Result<EncodedJson, JsonValueError> {
        let encoded = unsafe {
            PgTryBuilder::new(|| {
                Ok(pg_datum_to_json(
                    datum,
                    self.category,
                    self.output_function,
                ))
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }?;
        Ok(unsafe { EncodedJson::from_datum(encoded) })
    }
}

impl JsonRowEncoder {
    /// Encode one relation-shaped slot with PostgreSQL `row_to_json`.
    ///
    /// # Safety
    ///
    /// `slot` must contain a live tuple with a valid tuple descriptor for the
    /// duration of this synchronous call.
    pub unsafe fn encode(
        slot: *mut pg_sys::TupleTableSlot,
    ) -> Result<EncodedJson, JsonValueError> {
        let encoded = unsafe {
            PgTryBuilder::new(|| {
                let composite = pg_sys::ExecFetchSlotHeapTupleDatum(slot);
                let encoded = fcinfo::direct_function_call_as_datum(
                    pg_sys::row_to_json,
                    &[Some(composite)],
                );
                pg_sys::pfree(composite.cast_mut_ptr::<c_void>());
                encoded.ok_or(JsonValueError::NullInput)
            })
            .catch_others(|error| Err(JsonValueError::Postgres(PgError::from(error))))
            .execute()
        }?;
        Ok(unsafe { EncodedJson::from_datum(encoded) })
    }
}

impl EncodedJson {
    unsafe fn from_datum(datum: Datum) -> Self {
        let text = NonNull::new(datum.cast_mut_ptr::<pg_sys::varlena>())
            .expect("PostgreSQL JSON encoder returned a null text datum");
        Self { text }
    }

    pub fn as_str(&self) -> Result<&str, JsonValueError> {
        unsafe { varlena::text_to_rust_str(self.text.as_ptr()) }
            .map_err(JsonValueError::from)
    }

    #[inline]
    pub fn as_bytes(&self) -> Result<&[u8], JsonValueError> {
        self.as_str().map(str::as_bytes)
    }
}

impl Drop for EncodedJson {
    fn drop(&mut self) {
        unsafe { pg_sys::pfree(self.text.as_ptr().cast()) };
    }
}

impl SqlStateError for JsonValueError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Nul(_) => PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION,
            Self::Postgres(error) => error.sql_error_code(),
            Self::NullInput => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::UnsupportedServerEncoding { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::Utf8(_) => PgSqlErrorCode::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
        }
    }
}

impl JsonText {
    /// Validate and own JSON text through PostgreSQL's `json_in` function.
    pub fn parse(text: &str) -> Result<Self, JsonValueError> {
        match ColumnDatumTarget::validate_utf8_server_encoding() {
            Ok(()) => {}
            Err(DatumConversionError::UnsupportedServerEncoding { encoding }) => {
                return Err(JsonValueError::UnsupportedServerEncoding { encoding });
            }
            Err(_) => unreachable!(
                "UTF-8 capability validation returned an unrelated datum error"
            ),
        }
        let input = CString::new(text)?;
        let datum = unsafe { PgWrapper::json_input_from_cstr(input.as_ptr())? }
            .ok_or(JsonValueError::NullInput)?;

        // `json_in` returns a fresh text datum.  The semantic value keeps its
        // own Rust allocation, so the temporary PostgreSQL allocation is no
        // longer needed after validation.
        unsafe { pg_sys::pfree(datum.cast_mut_ptr::<c_void>()) };
        Ok(Self { text: text.into() })
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

    #[inline]
    pub fn mem_size(&self) -> usize {
        size_of::<Self>() + self.text.len()
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
        let ptr = self.text.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(Datum::from(pg_sys::cstring_to_text_with_len(
                    ptr.cast(),
                    len,
                )))
            })
            .catch_others(|error| Err(PgError::from(error)))
            .execute()
        }
        .map_err(DatumConversionError::Postgres)
    }
}

impl TryFrom<&str> for JsonText {
    type Error = JsonValueError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        Self::parse(text)
    }
}

impl fmt::Display for JsonText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl JsonbValue {
    /// Validate JSON text and normalize it through PostgreSQL's JSONB input
    /// and output functions.
    pub fn parse(text: &str) -> Result<Self, JsonValueError> {
        match ColumnDatumTarget::validate_utf8_server_encoding() {
            Ok(()) => {}
            Err(DatumConversionError::UnsupportedServerEncoding { encoding }) => {
                return Err(JsonValueError::UnsupportedServerEncoding { encoding });
            }
            Err(_) => unreachable!(
                "UTF-8 capability validation returned an unrelated datum error"
            ),
        }
        let input = CString::new(text)?;
        let datum = unsafe { PgWrapper::jsonb_input_from_cstr(input.as_ptr())? }
            .ok_or(JsonValueError::NullInput)?;
        let result = match unsafe {
            PgOutputCString::from_function_call(pg_sys::jsonb_out, &[Some(datum)])
        } {
            Some(output) => Self::output_text(output).map_err(JsonValueError::from),
            None => Err(JsonValueError::NullInput),
        };
        unsafe { pg_sys::pfree(datum.cast_mut_ptr::<c_void>()) };
        Ok(Self { text: result? })
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

        let detoasted = unsafe { DetoastedJsonb::new(datum) };
        let output = unsafe {
            PgOutputCString::from_function_call(
                pg_sys::jsonb_out,
                &[Some(detoasted.datum())],
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

    #[inline]
    pub fn mem_size(&self) -> usize {
        size_of::<Self>() + self.text.len()
    }

    /// Build a PostgreSQL JSONB Datum while retaining the original PG error.
    pub(crate) unsafe fn to_datum_checked(&self) -> Result<Option<Datum>, PgError> {
        let input = CString::new(self.as_str()).map_err(PgError::from)?;
        unsafe { PgWrapper::jsonb_input_from_cstr(input.as_ptr()) }
    }
}

impl TryFrom<&str> for JsonbValue {
    type Error = JsonValueError;

    fn try_from(text: &str) -> Result<Self, Self::Error> {
        Self::parse(text)
    }
}

impl fmt::Display for JsonbValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
