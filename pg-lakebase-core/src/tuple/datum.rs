//! Target-aware PostgreSQL `Datum` construction for [`Cell`].
//!
//! [`Cell`] stores semantic source values. This module owns the
//! destination-attribute conversion policy without exposing provider-specific
//! physical encodings.

use std::error::Error;
use std::ffi::CString;
use std::fmt;
use std::slice;

use pgrx::pg_sys::{self, Datum, Oid};
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{IntoDatum, fcinfo};

use crate::diag::{PgError, SqlStateError};
use crate::wrapper::PgWrapper;

use super::cell::Cell;

/// A relation attribute's target conversion plan.
///
/// The plan is resolved once from the destination OID. Typmods are not part of
/// this base datum conversion: provider/schema mapping owns the contract that
/// values passed through the normal Cell API already satisfy the destination
/// attribute's storage-domain constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDatumTarget {
    oid: Oid,
    kind: DatumKind,
}

impl ColumnDatumTarget {
    /// Resolve the supported destination representation once.
    #[must_use]
    pub fn from_oid(oid: Oid) -> Self {
        Self {
            oid,
            kind: DatumKind::from_oid(oid),
        }
    }

    #[inline]
    pub const fn oid(self) -> Oid {
        self.oid
    }

    /// Validate the framework's UTF-8 semantic capability without assuming a
    /// particular target OID. This is used by Arrow rules that have not yet
    /// been paired with a concrete PostgreSQL attribute.
    pub fn validate_utf8_server_encoding() -> Result<(), DatumConversionError> {
        let encoding = unsafe { pg_sys::GetDatabaseEncoding() };
        if encoding != pg_sys::pg_enc::PG_UTF8 as i32 {
            return Err(DatumConversionError::UnsupportedServerEncoding { encoding });
        }
        Ok(())
    }

    pub(crate) fn requires_utf8_server_encoding(self) -> bool {
        matches!(
            self.kind,
            DatumKind::Scalar(
                ScalarKind::Text
                    | ScalarKind::Varchar
                    | ScalarKind::Bpchar
                    | ScalarKind::Name
                    | ScalarKind::Json
                    | ScalarKind::Jsonb,
            ) | DatumKind::Array {
                element: ScalarKind::Text
                    | ScalarKind::Varchar
                    | ScalarKind::Bpchar
                    | ScalarKind::Name
                    | ScalarKind::Json,
                ..
            }
        )
    }

    /// Build plans for every relation attribute position in a live descriptor.
    ///
    /// # Safety
    ///
    /// `relation` must point to a live PostgreSQL relation with a valid tuple
    /// descriptor for the duration of this call. The returned plans own no
    /// PostgreSQL pointers and may be retained with the relation's executor
    /// state.
    pub(crate) unsafe fn from_relation(
        relation: pg_sys::Relation,
    ) -> Option<Box<[Self]>> {
        if relation.is_null() {
            return None;
        }
        let tuple_desc = unsafe { (*relation).rd_att };
        if tuple_desc.is_null() {
            return None;
        }
        let natts = unsafe { (*tuple_desc).natts };
        if natts < 0
            || (natts > 0 && unsafe { (*tuple_desc).attrs.as_ptr().is_null() })
        {
            return None;
        }
        if natts == 0 {
            return Some(Vec::new().into_boxed_slice());
        }
        let attrs = unsafe {
            slice::from_raw_parts((*tuple_desc).attrs.as_ptr(), natts as usize)
        };
        Some(
            attrs
                .iter()
                .map(|attr| Self::from_oid(attr.atttypid))
                .collect(),
        )
    }
}

/// A destination column target whose semantic server-encoding capability has
/// already been validated.
///
/// `ColumnDatumTarget` is only an OID-derived description. This bound form is
/// the public entry point for converting a semantic [`Cell`] into a Datum, so
/// callers cannot accidentally bypass the relation/column binding check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnDatumCodec {
    target: ColumnDatumTarget,
}

impl ColumnDatumCodec {
    /// Bind one destination target and validate semantic UTF-8 capability once.
    pub fn bind(target: ColumnDatumTarget) -> Result<Self, DatumConversionError> {
        if target.requires_utf8_server_encoding() {
            ColumnDatumTarget::validate_utf8_server_encoding()?;
        }
        Ok(Self { target })
    }

    #[inline]
    pub const fn oid(self) -> Oid {
        self.target.oid()
    }

    #[inline]
    pub(crate) const fn target(self) -> ColumnDatumTarget {
        self.target
    }

    #[inline]
    pub(crate) const fn from_validated(target: ColumnDatumTarget) -> Self {
        Self { target }
    }

    /// Convert one semantic Cell for this already-bound destination column.
    ///
    /// # Safety
    ///
    /// PostgreSQL must be running on the current backend thread, and the
    /// caller must have selected the memory context that owns any returned
    /// by-reference Datum.
    pub unsafe fn cell_to_datum(
        self,
        cell: Cell,
    ) -> Result<Datum, DatumConversionError> {
        unsafe { cell.into_datum_for_attribute(self.target) }
    }

    /// Convert one Datum from this already-bound source column into a semantic
    /// Cell.
    ///
    /// `None` is returned only for SQL NULL.
    ///
    /// # Safety
    ///
    /// PostgreSQL must be running on the current backend thread, and `datum`
    /// must be valid for this bound column's OID when `is_null` is false.
    pub unsafe fn datum_to_cell(
        self,
        datum: Datum,
        is_null: bool,
    ) -> Result<Option<Cell>, DatumConversionError> {
        unsafe {
            Cell::from_polymorphic_datum_checked(datum, is_null, self.target.oid())
        }
    }
}

/// A conversion failure that is distinct from SQL NULL.
#[derive(Debug)]
pub enum DatumConversionError {
    /// The Cell variant cannot represent the destination attribute.
    IncompatibleType { target: Oid },
    /// A numeric source cannot be narrowed to the target width.
    OutOfRange { target: Oid },
    /// Text input contains an invalid value for the destination input path.
    InvalidInput { target: Oid },
    /// The bound semantic conversion requires UTF-8 server encoding.
    UnsupportedServerEncoding { encoding: i32 },
    /// A PostgreSQL text datum was not valid UTF-8 for the semantic Cell API.
    InvalidUtf8 { target: Oid },
    /// PostgreSQL raised an error while parsing or constructing a datum.
    Postgres(PgError),
}

impl DatumConversionError {
    #[inline]
    const fn incompatible(target: Oid) -> Self {
        Self::IncompatibleType { target }
    }

    #[inline]
    const fn out_of_range(target: Oid) -> Self {
        Self::OutOfRange { target }
    }

    #[inline]
    const fn invalid_input(target: Oid) -> Self {
        Self::InvalidInput { target }
    }
}

impl fmt::Display for DatumConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompatibleType { target } => write!(
                f,
                "Cell cannot be represented by PostgreSQL target type {}",
                u32::from(*target)
            ),
            Self::OutOfRange { target } => write!(
                f,
                "Cell value is out of range for PostgreSQL target type {}",
                u32::from(*target)
            ),
            Self::InvalidInput { target } => write!(
                f,
                "Cell text is invalid for PostgreSQL target type {}",
                u32::from(*target)
            ),
            Self::UnsupportedServerEncoding { encoding } => write!(
                f,
                "semantic UTF-8 conversion requires UTF-8 server encoding, found encoding {}",
                encoding
            ),
            Self::InvalidUtf8 { target } => write!(
                f,
                "PostgreSQL datum for target type {} is not valid UTF-8",
                u32::from(*target)
            ),
            Self::Postgres(error) => error.fmt(f),
        }
    }
}

impl Error for DatumConversionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Postgres(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PgError> for DatumConversionError {
    #[inline]
    fn from(error: PgError) -> Self {
        Self::Postgres(error)
    }
}

impl SqlStateError for DatumConversionError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::IncompatibleType { .. } => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }
            Self::OutOfRange { .. } => {
                PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
            }
            Self::InvalidInput { .. } => {
                PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION
            }
            Self::UnsupportedServerEncoding { .. } => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            Self::InvalidUtf8 { .. } => {
                PgSqlErrorCode::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
            }
            Self::Postgres(error) => error.sql_error_code(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatumKind {
    Scalar(ScalarKind),
    Array {
        element_oid: Oid,
        element: ScalarKind,
    },
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarKind {
    Bool,
    Char,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    Text,
    Varchar,
    Bpchar,
    Name,
    Json,
    Jsonb,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    Interval,
    Bytea,
    Uuid,
}

impl DatumKind {
    fn from_oid(oid: Oid) -> Self {
        use ScalarKind as S;

        let scalar = match oid {
            pg_sys::BOOLOID => Some(S::Bool),
            pg_sys::CHAROID => Some(S::Char),
            pg_sys::INT2OID => Some(S::Int2),
            pg_sys::INT4OID => Some(S::Int4),
            pg_sys::INT8OID => Some(S::Int8),
            pg_sys::FLOAT4OID => Some(S::Float4),
            pg_sys::FLOAT8OID => Some(S::Float8),
            pg_sys::NUMERICOID => Some(S::Numeric),
            pg_sys::TEXTOID => Some(S::Text),
            pg_sys::VARCHAROID => Some(S::Varchar),
            pg_sys::BPCHAROID => Some(S::Bpchar),
            pg_sys::NAMEOID => Some(S::Name),
            pg_sys::JSONOID => Some(S::Json),
            pg_sys::JSONBOID => Some(S::Jsonb),
            pg_sys::DATEOID => Some(S::Date),
            pg_sys::TIMEOID => Some(S::Time),
            pg_sys::TIMESTAMPOID => Some(S::Timestamp),
            pg_sys::TIMESTAMPTZOID => Some(S::Timestamptz),
            pg_sys::INTERVALOID => Some(S::Interval),
            pg_sys::BYTEAOID => Some(S::Bytea),
            pg_sys::UUIDOID => Some(S::Uuid),
            _ => None,
        };
        if let Some(scalar) = scalar {
            return Self::Scalar(scalar);
        }

        let array = match oid {
            pg_sys::BOOLARRAYOID => Some((pg_sys::BOOLOID, S::Bool)),
            pg_sys::INT2ARRAYOID => Some((pg_sys::INT2OID, S::Int2)),
            pg_sys::INT4ARRAYOID => Some((pg_sys::INT4OID, S::Int4)),
            pg_sys::INT8ARRAYOID => Some((pg_sys::INT8OID, S::Int8)),
            pg_sys::FLOAT4ARRAYOID => Some((pg_sys::FLOAT4OID, S::Float4)),
            pg_sys::FLOAT8ARRAYOID => Some((pg_sys::FLOAT8OID, S::Float8)),
            pg_sys::TEXTARRAYOID => Some((pg_sys::TEXTOID, S::Text)),
            pg_sys::VARCHARARRAYOID => Some((pg_sys::VARCHAROID, S::Varchar)),
            pg_sys::BPCHARARRAYOID => Some((pg_sys::BPCHAROID, S::Bpchar)),
            pg_sys::NAMEARRAYOID => Some((pg_sys::NAMEOID, S::Name)),
            pg_sys::JSONARRAYOID => Some((pg_sys::JSONOID, S::Json)),
            _ => None,
        };
        match array {
            Some((element_oid, element)) => Self::Array {
                element_oid,
                element,
            },
            None => Self::Unsupported,
        }
    }
}

impl Cell {
    /// Convert a Cell to the already-bound destination attribute.
    ///
    /// This is the normal framework path. It validates semantic text input
    /// through PostgreSQL's JSON input functions and performs value-dependent
    /// integer narrowing. `Cell::String` JSON coercions are parsed through
    /// PostgreSQL input functions. A `Cell::Json` is accepted only by a `json` target and
    /// a `Cell::Jsonb` only by a `jsonb` target; crossing the two semantic types
    /// requires an explicit text parse by the caller.
    /// It intentionally does not apply typmods; the caller must provide values
    /// that satisfy the destination attribute's storage contract.
    ///
    /// # Safety
    ///
    /// PostgreSQL must be running on the current backend thread, and the caller
    /// must have selected the memory context that owns any returned by-reference
    /// datum.
    pub(crate) unsafe fn into_datum_for_attribute(
        self,
        target: ColumnDatumTarget,
    ) -> Result<Datum, DatumConversionError> {
        match target.kind {
            DatumKind::Scalar(kind) => unsafe {
                self.into_scalar_datum(kind, target.oid)
            },
            DatumKind::Array {
                element_oid,
                element,
            } => unsafe { self.into_array_datum(target.oid, element_oid, element) },
            DatumKind::Unsupported => {
                Err(DatumConversionError::incompatible(target.oid))
            }
        }
    }

    unsafe fn into_scalar_datum(
        self,
        kind: ScalarKind,
        target: Oid,
    ) -> Result<Datum, DatumConversionError> {
        match kind {
            ScalarKind::Bool => match self {
                Cell::Bool(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Char => match self {
                Cell::I8(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Int2 | ScalarKind::Int4 | ScalarKind::Int8 => unsafe {
                self.into_integer_datum(kind, target)
            },
            ScalarKind::Float4 => match self {
                Cell::F32(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Float8 => match self {
                Cell::F64(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Numeric => match self {
                Cell::Numeric(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Text | ScalarKind::Varchar | ScalarKind::Bpchar => match self
            {
                Cell::String(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::StringView(view) => unsafe { view.as_str() }
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Name => unsafe { self.into_name_datum(target) },
            ScalarKind::Json | ScalarKind::Jsonb => unsafe {
                self.into_json_datum(target)
            },
            ScalarKind::Date => match self {
                Cell::Date(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Time => match self {
                Cell::Time(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Timestamp => match self {
                Cell::Timestamp(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Timestamptz => match self {
                Cell::Timestamptz(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Interval => match self {
                Cell::Interval(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Bytea => match self {
                Cell::Bytea(value) => value
                    .as_ref()
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::ByteaView(view) => unsafe { view.as_slice() }
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Uuid => match self {
                Cell::Uuid(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
        }
    }

    unsafe fn into_integer_datum(
        self,
        kind: ScalarKind,
        target: Oid,
    ) -> Result<Datum, DatumConversionError> {
        match kind {
            ScalarKind::Int2 => match self {
                Cell::I16(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::I32(value) => i16::try_from(value)
                    .map_err(|_| DatumConversionError::out_of_range(target))?
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::I64(value) => i16::try_from(value)
                    .map_err(|_| DatumConversionError::out_of_range(target))?
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Int4 => match self {
                Cell::I16(value) => (value as i32)
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::I32(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::I64(value) => i32::try_from(value)
                    .map_err(|_| DatumConversionError::out_of_range(target))?
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            ScalarKind::Int8 => match self {
                Cell::I16(value) => (value as i64)
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::I32(value) => (value as i64)
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::I64(value) => value
                    .into_datum()
                    .ok_or(DatumConversionError::invalid_input(target)),
                _ => Err(DatumConversionError::incompatible(target)),
            },
            _ => Err(DatumConversionError::incompatible(target)),
        }
    }

    unsafe fn into_name_datum(
        self,
        target: Oid,
    ) -> Result<Datum, DatumConversionError> {
        let text = match self {
            Cell::String(value) => CString::new(value),
            Cell::StringView(view) => CString::new(unsafe { view.as_str() }),
            _ => return Err(DatumConversionError::incompatible(target)),
        }
        .map_err(|_| DatumConversionError::invalid_input(target))?;
        unsafe {
            fcinfo::direct_function_call_as_datum(
                pg_sys::namein,
                &[Some(Datum::from(text.as_ptr()))],
            )
        }
        .ok_or(DatumConversionError::invalid_input(target))
    }

    unsafe fn into_json_datum(
        self,
        target: Oid,
    ) -> Result<Datum, DatumConversionError> {
        match target {
            pg_sys::JSONOID => match self {
                Cell::Json(value) => unsafe { value.to_datum_checked() },
                Cell::String(value) => unsafe {
                    Cell::String(value).into_json_text_datum(target)
                },
                Cell::StringView(view) => unsafe {
                    Cell::StringView(view).into_json_text_datum(target)
                },
                _ => Err(DatumConversionError::incompatible(target)),
            },
            pg_sys::JSONBOID => match self {
                Cell::Jsonb(value) => unsafe { value.to_datum_checked() }
                    .map_err(DatumConversionError::from)?
                    .ok_or(DatumConversionError::invalid_input(target)),
                Cell::String(value) => unsafe {
                    Cell::String(value).into_json_text_datum(target)
                },
                Cell::StringView(view) => unsafe {
                    Cell::StringView(view).into_json_text_datum(target)
                },
                _ => Err(DatumConversionError::incompatible(target)),
            },
            _ => Err(DatumConversionError::incompatible(target)),
        }
    }

    unsafe fn into_json_text_datum(
        self,
        target: Oid,
    ) -> Result<Datum, DatumConversionError> {
        let text = match self {
            Cell::String(value) => CString::new(value),
            Cell::StringView(view) => CString::new(unsafe { view.as_str() }),
            _ => return Err(DatumConversionError::incompatible(target)),
        }
        .map_err(|_| DatumConversionError::invalid_input(target))?;
        let result = match target {
            pg_sys::JSONOID => unsafe {
                PgWrapper::json_input_from_cstr(text.as_ptr())
            },
            pg_sys::JSONBOID => unsafe {
                PgWrapper::jsonb_input_from_cstr(text.as_ptr())
            },
            _ => return Err(DatumConversionError::incompatible(target)),
        }?;
        result.ok_or(DatumConversionError::invalid_input(target))
    }

    unsafe fn into_array_datum(
        self,
        target: Oid,
        element_oid: Oid,
        element: ScalarKind,
    ) -> Result<Datum, DatumConversionError> {
        let context = unsafe { pg_sys::CurrentMemoryContext };
        let mut state =
            unsafe { pg_sys::initArrayResult(element_oid, context, false) };

        let mut append = |value: Option<Cell>| -> Result<(), DatumConversionError> {
            let (datum, is_null) = match value {
                Some(value) => (
                    unsafe { value.into_scalar_datum(element, element_oid) }?,
                    false,
                ),
                None => (Datum::from(0usize), true),
            };
            state = unsafe {
                pg_sys::accumArrayResult(state, datum, is_null, element_oid, context)
            };
            Ok(())
        };

        match self {
            Cell::BoolArray(values) => {
                if element != ScalarKind::Bool {
                    return Err(DatumConversionError::incompatible(target));
                }
                for value in values {
                    append(value.map(Cell::Bool))?;
                }
            }
            Cell::I16Array(values) => {
                if !matches!(
                    element,
                    ScalarKind::Int2 | ScalarKind::Int4 | ScalarKind::Int8
                ) {
                    return Err(DatumConversionError::incompatible(target));
                }
                for value in values {
                    append(value.map(Cell::I16))?;
                }
            }
            Cell::I32Array(values) => {
                if !matches!(
                    element,
                    ScalarKind::Int2 | ScalarKind::Int4 | ScalarKind::Int8
                ) {
                    return Err(DatumConversionError::incompatible(target));
                }
                for value in values {
                    append(value.map(Cell::I32))?;
                }
            }
            Cell::I64Array(values) => {
                if !matches!(
                    element,
                    ScalarKind::Int2 | ScalarKind::Int4 | ScalarKind::Int8
                ) {
                    return Err(DatumConversionError::incompatible(target));
                }
                for value in values {
                    append(value.map(Cell::I64))?;
                }
            }
            Cell::F32Array(values) => {
                if element != ScalarKind::Float4 {
                    return Err(DatumConversionError::incompatible(target));
                }
                for value in values {
                    append(value.map(Cell::F32))?;
                }
            }
            Cell::F64Array(values) => {
                if element != ScalarKind::Float8 {
                    return Err(DatumConversionError::incompatible(target));
                }
                for value in values {
                    append(value.map(Cell::F64))?;
                }
            }
            Cell::StringArray(values) => {
                if !matches!(
                    element,
                    ScalarKind::Text
                        | ScalarKind::Varchar
                        | ScalarKind::Bpchar
                        | ScalarKind::Name
                        | ScalarKind::Json
                        | ScalarKind::Jsonb
                ) {
                    return Err(DatumConversionError::incompatible(target));
                }
                for value in values {
                    append(value.map(Cell::String))?;
                }
            }
            _ => return Err(DatumConversionError::incompatible(target)),
        }

        Ok(unsafe { pg_sys::makeArrayResult(state, context) })
    }
}
