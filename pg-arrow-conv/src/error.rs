//! Conversion error model.

use std::borrow::Cow;

use pg_lakebase_core::diag::SqlStateError;
use pg_lakebase_core::tuple::DecimalCodecError;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

/// Format-neutral conversion error, classified for SQLSTATE reporting through
/// [`SqlStateError`].
#[derive(Error, Debug)]
pub enum ConvError {
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
    DatumConversionError(String),

    /// A text/name datum's bytes were not valid UTF-8. Reachable on a database
    /// whose server encoding is not UTF-8, so it is user/data-level, not an
    /// invariant: the typed [`std::str::Utf8Error`] is kept as the `source` so
    /// the offending byte offset survives into the report's DETAIL.
    #[error("text datum is not valid UTF-8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

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

    /// The NUMERIC codec produced bytes PostgreSQL rejected as malformed — a
    /// bug in the codec, not a user error.
    #[error("internal codec error in pg-arrow-conv: {0}")]
    DecimalCodecBug(String),

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
pub type ConvResult<T> = Result<T, ConvError>;

impl SqlStateError for ConvError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            ConvError::UnsupportedColumnType(_)
            | ConvError::IncompatibleColumnType(_, _)
            | ConvError::ArrowTypeMismatch(_) => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }
            ConvError::DatumConversionError(_)
            | ConvError::InvalidUtf8(_)
            | ConvError::NumericError(_)
            | ConvError::DatetimeConversionError(_)
            | ConvError::UuidConversionError(_) => {
                PgSqlErrorCode::ERRCODE_DATA_EXCEPTION
            }
            ConvError::ArrowError(_)
            | ConvError::InvariantViolated(_)
            | ConvError::DecimalCodecBug(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}

/// Routes a [`DecimalCodecError`] to the conversion layer that matches its
/// cause: an unmappable column shape becomes a datatype mismatch, out-of-range
/// user data a data exception, and malformed wire bytes a codec bug.
impl From<DecimalCodecError> for ConvError {
    fn from(err: DecimalCodecError) -> Self {
        match err {
            DecimalCodecError::PrecisionOutOfRange { precision } => {
                ConvError::IncompatibleColumnType(
                    format!("decimal(precision={precision})"),
                    err.to_string(),
                )
            }
            DecimalCodecError::ScaleOutOfRange { precision, scale } => {
                ConvError::IncompatibleColumnType(
                    format!("decimal({precision}, {scale})"),
                    err.to_string(),
                )
            }
            DecimalCodecError::ValueOutOfRange { .. } => {
                ConvError::DatumConversionError(err.to_string())
            }
            DecimalCodecError::InvalidBinaryRepresentation { .. } => {
                ConvError::DecimalCodecBug(err.to_string())
            }
        }
    }
}
