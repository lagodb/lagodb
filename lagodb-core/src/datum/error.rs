//! Errors shared by semantic values and PostgreSQL Datum codecs.

use std::error::Error;
use std::fmt;

use pgrx::pg_sys::Oid;
use pgrx::prelude::PgSqlErrorCode;

use crate::diag::{PgError, SqlStateError};

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

/// The database encoding cannot satisfy a semantic UTF-8 conversion contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Utf8ServerEncodingError {
    pub(super) encoding: i32,
}

impl Utf8ServerEncodingError {
    #[inline]
    pub const fn encoding(self) -> i32 {
        self.encoding
    }
}

impl fmt::Display for Utf8ServerEncodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "semantic UTF-8 conversion requires UTF-8 server encoding, found encoding {}",
            self.encoding
        )
    }
}

impl Error for Utf8ServerEncodingError {}

impl SqlStateError for Utf8ServerEncodingError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
    }
}

impl DatumConversionError {
    #[inline]
    pub(super) const fn incompatible(target: Oid) -> Self {
        Self::IncompatibleType { target }
    }

    #[inline]
    pub(super) const fn out_of_range(target: Oid) -> Self {
        Self::OutOfRange { target }
    }

    #[inline]
    pub(super) const fn invalid_input(target: Oid) -> Self {
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

impl From<Utf8ServerEncodingError> for DatumConversionError {
    #[inline]
    fn from(error: Utf8ServerEncodingError) -> Self {
        Self::UnsupportedServerEncoding {
            encoding: error.encoding(),
        }
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
