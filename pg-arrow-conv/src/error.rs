//! Conversion error model.

use std::borrow::Cow;

use pg_lakebase_core::diag::{PgError, SqlStateError};
use pg_lakebase_core::tuple::{DatumConversionError, DecimalCodecError};
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

/// Format-neutral conversion error, classified for SQLSTATE reporting through
/// [`SqlStateError`].
#[derive(Error, Debug)]
pub enum ArrowConversionError {
    /// The column's Arrow `DataType` is not one this layer can materialize.
    #[error("column data type is not supported: {0}")]
    UnsupportedColumnType(String),

    /// The column's Arrow `DataType` is recognized but incompatible with the
    /// target PostgreSQL type (or the requested precision/scale/width).
    #[error("column data type '{0}' is incompatible: {1}")]
    IncompatibleColumnType(String, String),

    /// The physical Arrow array did not match the resolved column rule.
    #[error("arrow type mismatch: expected {0}")]
    ArrowTypeMismatch(Cow<'static, str>),

    /// An error originating from the Arrow library itself.
    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    /// A value could not be converted to/from its PostgreSQL datum form.
    #[error("datum conversion error: {0}")]
    DatumConversion(#[source] DatumConversionError),

    /// Arrow or PostgreSQL input rejected a semantic value.
    #[error("invalid conversion input: {0}")]
    InvalidInput(String),

    /// A value exceeded a conversion range without a more specific domain
    /// error. Numeric codec ranges use [`Self::DecimalCodec`] so they can
    /// retain PostgreSQL's numeric SQLSTATE.
    #[error("conversion value is out of range: {0}")]
    ValueOutOfRange(String),

    /// A PostgreSQL ERROR raised while allocating or constructing a Datum.
    /// Preserve its SQLSTATE for the callback boundary.
    #[error("PostgreSQL error: {0}")]
    Postgres(#[from] PgError),

    /// A conversion-layer invariant violation. Used for "cannot happen"
    /// branches where a runtime guard remains because the type system does not
    /// yet encode the invariant — for example an encoder receiving a datum
    /// whose source type does not match the column rule the schema validator
    /// already resolved. Surfacing one of these in production is a bug in
    /// `pg_arrow_conv`, not a user error.
    ///
    /// Prefer expressing invariants directly in the type system over guarding
    /// with this variant when the unreachable case can be made
    /// unrepresentable.
    #[error("invariant violation in pg-arrow-conv: {0}")]
    InvariantViolated(&'static str),

    /// A NUMERIC codec error. Its SQLSTATE is selected from the structured
    /// error rather than from a formatted message.
    #[error("decimal codec error: {0}")]
    DecimalCodec(#[source] DecimalCodecError),

    /// A date/time value fell outside the range PostgreSQL can represent.
    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(
        #[from] pgrx::datum::datetime_support::DateTimeConversionError,
    ),

    /// A NUMERIC value could not be parsed or constructed.
    #[error("numeric error: {0}")]
    NumericError(#[from] pgrx::datum::numeric_support::error::Error),

    /// A UUID value could not be constructed from its bytes.
    #[error("uuid error: {0}")]
    UuidConversionError(#[from] uuid::Error),
}

/// Result alias for the conversion surface.
pub type ArrowConversionResult<T> = Result<T, ArrowConversionError>;

impl SqlStateError for ArrowConversionError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            ArrowConversionError::UnsupportedColumnType(_)
            | ArrowConversionError::IncompatibleColumnType(_, _)
            | ArrowConversionError::ArrowTypeMismatch(_) => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }
            ArrowConversionError::DatumConversion(source) => source.sql_error_code(),
            ArrowConversionError::InvalidInput(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_TEXT_REPRESENTATION
            }
            ArrowConversionError::ValueOutOfRange(_) => {
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
            }
            ArrowConversionError::NumericError(_)
            | ArrowConversionError::DatetimeConversionError(_)
            | ArrowConversionError::UuidConversionError(_) => {
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
            }
            ArrowConversionError::Postgres(error) => error.sql_error_code(),
            ArrowConversionError::ArrowError(_)
            | ArrowConversionError::InvariantViolated(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
            ArrowConversionError::DecimalCodec(error) => match error {
                DecimalCodecError::ValueOutOfRange { .. } => {
                    PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
                }
                DecimalCodecError::InvalidBinaryRepresentation { .. } => {
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
                }
                DecimalCodecError::PrecisionOutOfRange { .. }
                | DecimalCodecError::ScaleOutOfRange { .. } => {
                    PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
                }
            },
        }
    }
}

impl From<DatumConversionError> for ArrowConversionError {
    fn from(error: DatumConversionError) -> Self {
        Self::DatumConversion(error)
    }
}

/// Routes a [`DecimalCodecError`] to the conversion layer that matches its
/// cause: an unmappable column shape becomes a datatype mismatch, numeric
/// out-of-range data retains the numeric SQLSTATE, and malformed wire bytes
/// remain a codec error.
impl From<DecimalCodecError> for ArrowConversionError {
    fn from(err: DecimalCodecError) -> Self {
        match err {
            error @ DecimalCodecError::PrecisionOutOfRange { precision } => {
                ArrowConversionError::IncompatibleColumnType(
                    format!("decimal(precision={precision})"),
                    error.to_string(),
                )
            }
            error @ DecimalCodecError::ScaleOutOfRange { precision, scale } => {
                ArrowConversionError::IncompatibleColumnType(
                    format!("decimal({precision}, {scale})"),
                    error.to_string(),
                )
            }
            error @ (DecimalCodecError::ValueOutOfRange { .. }
            | DecimalCodecError::InvalidBinaryRepresentation { .. }) => {
                ArrowConversionError::DecimalCodec(error)
            }
        }
    }
}
