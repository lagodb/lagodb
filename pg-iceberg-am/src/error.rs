//! Error layering for the Iceberg table access method.
//!
//! Keep Iceberg business logic on [`IcebergResult<T>`] and [`IcebergError`].
//! The PostgreSQL table-AM callback boundary returns
//! `pg_lakebase_core::api::AmResult<T>`, which owns a PostgreSQL
//! `ErrorReport` through a small error handle.
//! The bridge is the `From<IcebergError> for ErrorReport` implementation in
//! this file, so callback methods can use normal `?` propagation.
//!
//! Avoid adding `try_*` callback wrappers or scattered
//! `.map_err(Into::into)` / `.into()` conversions in access-method code. If
//! third-party errors need adaptation, keep that inside meaningful Iceberg
//! object methods returning [`IcebergResult<T>`], then let the callback boundary
//! perform the final conversion to PostgreSQL.

use pg_lakebase_core::diag::{PgError, SqlStateError};
use pg_lakebase_core::options::TablespaceError;
use pg_lakebase_core::options::{TableOptionError, TablespaceCacheError};
use pg_lakebase_storage::{StorageError, StorageErrorKind};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use std::fmt::{Display, Formatter};
use thiserror::Error;

// ============================================================================
//  Metadata Catalog Operation
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCatalogOperation {
    Access,
    Insert,
    Read,
    Update,
    Delete,
}

impl Display for MetadataCatalogOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Access => f.write_str("access"),
            Self::Insert => f.write_str("insert"),
            Self::Read => f.write_str("read"),
            Self::Update => f.write_str("update"),
            Self::Delete => f.write_str("delete"),
        }
    }
}

// ============================================================================
//  Iceberg Error
// ============================================================================

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error("failed to {operation} lakebase.iceberg_metadata catalog: {source}")]
    MetadataCatalog {
        operation: MetadataCatalogOperation,
        #[source]
        source: PgError,
    },

    #[error("metadata catalog record not found for relid: {0}")]
    MetadataCatalogNotFound(pg_sys::Oid),

    #[error("metadata catalog record already exists for relid: {0}")]
    MetadataCatalogAlreadyExists(pg_sys::Oid),

    #[error("invalid metadata catalog record: {0}")]
    MetadataCatalogInvalidRecord(String),

    #[error("optimistic locking failed: metadata location changed concurrently")]
    MetadataCatalogConflict,

    #[error("metadata tracker error: {0}")]
    MetadataTracker(String),

    #[error(
        "failed to commit metadata for relid {relid} after {max_retries} retries due to concurrent updates"
    )]
    MetadataCommitConflict {
        relid: pg_sys::Oid,
        max_retries: i32,
    },

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
            IcebergError::MetadataCatalog { source, .. } => source.sql_error_code(),

            IcebergError::MetadataCatalogNotFound(_) => {
                PgSqlErrorCode::ERRCODE_NO_DATA_FOUND
            }

            IcebergError::MetadataCatalogAlreadyExists(_) => {
                PgSqlErrorCode::ERRCODE_UNIQUE_VIOLATION
            }

            IcebergError::MetadataCatalogConflict => {
                PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
            }

            IcebergError::MetadataCatalogInvalidRecord(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::TablespaceError(error) => error.sql_error_code(),

            IcebergError::TablespaceCacheError(error) => error.sql_error_code(),

            IcebergError::TableOptionError(error) => error.sql_error_code(),

            IcebergError::StorageError(error) => storage_sql_error_code(error),

            IcebergError::TablespaceNotFound => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }

            IcebergError::PgError(error) => error.sql_error_code(),

            IcebergError::MetadataTracker(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::MetadataCommitConflict { .. } => {
                PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
            }

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

            IcebergError::IcebergLiteError(error) => {
                iceberg_lite_sql_error_code(error)
            }

            IcebergError::ArrowError(_)
            | IcebergError::ArrowTypeMismatch(_)
            | IcebergError::JsonError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::SpiError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::IoError(_) => PgSqlErrorCode::ERRCODE_IO_ERROR,

            IcebergError::NotImplemented(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
        }
    }
}

impl From<IcebergError> for ErrorReport {
    fn from(value: IcebergError) -> Self {
        ErrorReport::new(value.sql_error_code(), format!("{value}"), "")
    }
}

pub type IcebergResult<T> = Result<T, IcebergError>;

impl IcebergError {
    pub fn metadata_catalog(
        operation: MetadataCatalogOperation,
        source: PgError,
    ) -> Self {
        Self::MetadataCatalog { operation, source }
    }
}

fn storage_sql_error_code(error: &StorageError) -> PgSqlErrorCode {
    match error.kind() {
        StorageErrorKind::InvalidPath | StorageErrorKind::Configuration => {
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
        }
        StorageErrorKind::NotFound => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
        StorageErrorKind::Unsupported => {
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        }
        StorageErrorKind::Busy => PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
        StorageErrorKind::ResourceExhausted => {
            PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
        }
        StorageErrorKind::Io
        | StorageErrorKind::Backend
        | StorageErrorKind::Cache
        | StorageErrorKind::CacheFillAborted => PgSqlErrorCode::ERRCODE_IO_ERROR,
        StorageErrorKind::Protocol | StorageErrorKind::ClosedHandle => {
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
        }
    }
}

fn iceberg_lite_sql_error_code(error: &iceberg_lite::Error) -> PgSqlErrorCode {
    match error.kind() {
        iceberg_lite::ErrorKind::FeatureUnsupported => {
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        }
        _ => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_catalog_sqlstate_survives_iceberg_error_boundary() {
        let conflict = IcebergError::MetadataCatalogConflict;
        assert_eq!(
            conflict.sql_error_code(),
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        );

        let not_found = IcebergError::MetadataCatalogNotFound(pg_sys::Oid::from(42));
        assert_eq!(
            not_found.sql_error_code(),
            PgSqlErrorCode::ERRCODE_NO_DATA_FOUND
        );
    }

    #[test]
    fn retry_exhaustion_reports_serialization_failure() {
        let error = IcebergError::MetadataCommitConflict {
            relid: pg_sys::Oid::from(42),
            max_retries: 3,
        };

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        );
    }

    #[test]
    fn iceberg_lite_feature_unsupported_reports_feature_not_supported() {
        let error = IcebergError::IcebergLiteError(iceberg_lite::Error::new(
            iceberg_lite::ErrorKind::FeatureUnsupported,
            "catalog method not implemented",
        ));

        assert_eq!(
            error.sql_error_code(),
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        );
    }
}
