//! Error layering for the Iceberg table access method.
//!
//! Keep Iceberg business logic on [`IcebergResult<T>`] and [`IcebergError`].
//! The PostgreSQL table-AM callback boundary returns
//! `pg_lakebase_core::api::AmResult<T>`, which is `Result<T, ErrorReport>`.
//! The bridge is the `From<IcebergError> for ErrorReport` implementation in
//! this file, so callback methods can use normal `?` propagation.
//!
//! Avoid adding `try_*` callback wrappers or scattered
//! `.map_err(Into::into)` / `.into()` conversions in access-method code. If
//! third-party errors need adaptation, keep that inside meaningful Iceberg
//! object methods returning [`IcebergResult<T>`], then let the callback boundary
//! perform the final conversion to PostgreSQL.

use crate::catalog::iceberg_metadata::IcebergMetadataError;
use pg_lakebase_core::diag::{PgError, SqlStateError};
use pg_lakebase_core::options::TablespaceError;
use pg_lakebase_core::options::{TableOptionError, TablespaceCacheError};
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error("metadata error: {0}")]
    MetadataError(#[from] IcebergMetadataError),

    #[error("tablespace error: {0}")]
    TablespaceError(#[from] TablespaceError),

    #[error("tablespace cache error: {0}")]
    TablespaceCacheError(#[from] TablespaceCacheError),

    #[error("table option error: {0}")]
    TableOptionError(#[from] TableOptionError),

    #[error("storage error: {0}")]
    StorageError(#[from] pg_lakebase_storage::StorageError),

    #[error("postgres error: {0}")]
    PgError(#[from] PgError),

    #[error("tablespace options not found")]
    TablespaceNotFound,

    #[error("namespace name is null")]
    NamespaceNull,

    #[error("metadata location is null")]
    MetadataLocationNull,

    #[error("schema build error: {0}")]
    SchemaBuildError(String),

    #[error("column {0} is not found in source")]
    ColumnNotFound(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("column '{0}' data type '{1}' is incompatible")]
    IncompatibleColumnType(String, String),

    #[error("cannot import column '{0}' data type '{1}'")]
    ImportColumnError(String, String),

    #[error("decimal conversion error: {0}")]
    DecimalConversionError(#[from] rust_decimal::Error),

    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(
        #[from] pgrx::datum::datetime_support::DateTimeConversionError,
    ),

    #[error("datum conversion error: {0}")]
    DatumConversionError(String),

    #[error("uuid error: {0}")]
    UuidConversionError(#[from] uuid::Error),

    #[error("numeric error: {0}")]
    NumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("iceberg error: {0}")]
    IcebergLiteError(#[from] iceberg_lite::Error),

    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("arrow type mismatch: expected {0}")]
    ArrowTypeMismatch(String),

    #[error("SPI error: {0}")]
    SpiError(String),

    #[error("json error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("{0}")]
    IoError(#[from] std::io::Error),

    #[error("feature not yet implemented: {0}")]
    NotImplemented(&'static str),
}

impl SqlStateError for IcebergError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            IcebergError::TablespaceError(_)
            | IcebergError::TablespaceCacheError(_)
            | IcebergError::TableOptionError(_)
            | IcebergError::StorageError(_)
            | IcebergError::TablespaceNotFound => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }

            IcebergError::PgError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::NamespaceNull | IcebergError::MetadataLocationNull => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
            }

            IcebergError::SchemaBuildError(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION
            }

            IcebergError::ColumnNotFound(_) => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN
            }

            IcebergError::UnsupportedColumnType(_)
            | IcebergError::IncompatibleColumnType(_, _)
            | IcebergError::ImportColumnError(_, _) => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }

            IcebergError::DecimalConversionError(_)
            | IcebergError::ParseFloatError(_)
            | IcebergError::DatetimeConversionError(_)
            | IcebergError::DatumConversionError(_)
            | IcebergError::UuidConversionError(_)
            | IcebergError::NumericError(_) => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,

            IcebergError::IcebergLiteError(_)
            | IcebergError::ArrowError(_)
            | IcebergError::ArrowTypeMismatch(_)
            | IcebergError::JsonError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::SpiError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::IoError(_) => PgSqlErrorCode::ERRCODE_IO_ERROR,

            IcebergError::NotImplemented(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }

            IcebergError::MetadataError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}

impl From<IcebergError> for ErrorReport {
    fn from(value: IcebergError) -> Self {
        ErrorReport::new(value.sql_error_code(), format!("{value}"), "")
    }
}

pub type IcebergResult<T> = Result<T, IcebergError>;
