//! Shared Iceberg domain errors.
//!
//! Keep shared scan, schema, predicate, write and storage logic on
//! [`IcebergResult<T>`] and [`IcebergError`]. This type carries no adapter
//! policy: managed-table and FDW errors wrap it transparently in their own
//! adapter error types, and delegate [`SqlStateError`] to it for every wrapped
//! shared failure.
//!
//! Core framework boundaries own the final conversion to PostgreSQL. A
//! captured PostgreSQL report leaves through
//! [`SqlStateError::try_into_pg_report`] with its structured fields intact.
//! Avoid adding `try_*` callback wrappers or scattered
//! `.map_err(Into::into)` / `.into()` conversions in access-method code. If
//! third-party errors need adaptation, keep that inside meaningful Iceberg
//! object methods returning [`IcebergResult<T>`].

use std::error::Error as StdError;
use std::io;

use iceberg_lite::catalog::rest::{RestError, RestErrorKind};
use iceberg_lite::{Error as IcebergLiteError, ErrorKind as IcebergLiteErrorKind};
use lagodb_core::diag::{PgError, PgReportError, SqlStateError};
use lagodb_storage::{StorageError, StorageErrorKind};
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error(
        "Iceberg mutation exceeds the synthetic ctid limit of {max_files} data files per transaction and relation"
    )]
    FileIdLimitExceeded { max_files: usize },

    #[error("Iceberg row identity exceeds the synthetic ctid capacity")]
    RowIdentityLimitExceeded,

    #[error("unknown Iceberg file ID {file_id}")]
    UnknownFileId { file_id: u32 },

    #[error("cannot find Iceberg scan task metadata for deletion target {path}")]
    MutationTaskNotFound { path: String },

    #[error("storage error: {0}")]
    StorageError(#[from] StorageError),

    /// A captured PostgreSQL report. Boundary conversions take it back out
    /// through [`SqlStateError::try_into_pg_report`].
    #[error(transparent)]
    Postgres(PgReportError),

    #[error(transparent)]
    ArrowConversion(#[from] lagodb_arrow::ArrowConversionError),

    #[error("schema build error: {0}")]
    SchemaBuildError(String),

    #[error("column {0} is not found in source")]
    ColumnNotFound(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(
        #[from] pgrx::datum::datetime_support::DateTimeConversionError,
    ),

    #[error("uuid error: {0}")]
    UuidConversionError(#[from] uuid::Error),

    #[error("numeric error: {0}")]
    NumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("iceberg error: {0}")]
    IcebergLiteError(#[from] IcebergLiteError),

    #[error("Iceberg schema evolution conflict: {source}")]
    SchemaEvolutionConflict {
        #[source]
        source: IcebergLiteError,
    },

    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("arrow type mismatch: expected {0}")]
    ArrowTypeMismatch(String),

    #[error("binary codec error: {0}")]
    BinaryCodecError(#[from] bincode::Error),

    /// Iceberg integration-internal invariant violation. Used for branches
    /// where a runtime guard remains because the type system does not yet
    /// encode the invariant. Surfacing one of these in production is a bug
    /// in `iceberg`, not a user error.
    ///
    /// Prefer expressing invariants directly in the type system (for
    /// example, an enum-based state machine) over guarding with this
    /// variant when the unreachable case can be made unrepresentable.
    #[error("invariant violation in iceberg: {0}")]
    InvariantViolated(&'static str),

    #[error("feature not yet implemented: {0}")]
    NotImplemented(&'static str),
}

impl SqlStateError for IcebergError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            IcebergError::StorageError(error) => storage_sql_error_code(error),

            IcebergError::Postgres(error) => error.sql_error_code(),

            IcebergError::ArrowConversion(conv) => conv.sql_error_code(),

            IcebergError::FileIdLimitExceeded { .. }
            | IcebergError::RowIdentityLimitExceeded => {
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED
            }

            IcebergError::UnknownFileId { .. }
            | IcebergError::MutationTaskNotFound { .. } => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::SchemaBuildError(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION
            }

            IcebergError::ColumnNotFound(_) => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN
            }

            IcebergError::UnsupportedColumnType(_) => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }

            IcebergError::DatetimeConversionError(_)
            | IcebergError::UuidConversionError(_)
            | IcebergError::NumericError(_) => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,

            IcebergError::IcebergLiteError(error) => {
                iceberg_lite_sql_error_code(error)
            }

            IcebergError::SchemaEvolutionConflict { .. } => {
                PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
            }

            IcebergError::ArrowError(_)
            | IcebergError::ArrowTypeMismatch(_)
            | IcebergError::BinaryCodecError(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::InvariantViolated(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::NotImplemented(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
        }
    }

    fn try_into_pg_report(self) -> Result<PgReportError, Self> {
        match self {
            Self::Postgres(error) => Ok(error),
            Self::ArrowConversion(error) => {
                error.try_into_pg_report().map_err(Self::ArrowConversion)
            }
            error => Err(error),
        }
    }
}

impl From<PgError> for IcebergError {
    fn from(error: PgError) -> Self {
        Self::Postgres(error.into())
    }
}

impl From<PgReportError> for IcebergError {
    fn from(error: PgReportError) -> Self {
        Self::Postgres(error)
    }
}

pub type IcebergResult<T> = Result<T, IcebergError>;

impl IcebergError {
    pub fn schema_evolution_conflict(source: IcebergLiteError) -> Self {
        Self::SchemaEvolutionConflict { source }
    }
}

pub(crate) fn storage_error_to_iceberg_lite(error: StorageError) -> IcebergLiteError {
    let kind = match error.kind() {
        StorageErrorKind::Unsupported => IcebergLiteErrorKind::FeatureUnsupported,
        _ => IcebergLiteErrorKind::IoError,
    };
    IcebergLiteError::new(kind, error.to_string()).with_source(error)
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
        StorageErrorKind::Protocol
        | StorageErrorKind::ClosedHandle
        | StorageErrorKind::ExpiredCursor
        | StorageErrorKind::Conflict => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        StorageErrorKind::Ambiguous => PgSqlErrorCode::ERRCODE_IO_ERROR,
    }
}

fn iceberg_lite_sql_error_code(error: &IcebergLiteError) -> PgSqlErrorCode {
    let mut current = error.source();
    while let Some(source) = current {
        if let Some(rest_error) = source.downcast_ref::<RestError>() {
            return match rest_error.kind() {
                RestErrorKind::Unauthenticated => {
                    PgSqlErrorCode::ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION
                }
                RestErrorKind::Forbidden => {
                    PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE
                }
                RestErrorKind::CommitConflict | RestErrorKind::Conflict => {
                    PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
                }
                RestErrorKind::RateLimited => {
                    PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
                }
                RestErrorKind::Server
                | RestErrorKind::Client
                | RestErrorKind::CommitStateUnknown
                | RestErrorKind::Unexpected => PgSqlErrorCode::ERRCODE_FDW_ERROR,
            };
        }
        if let Some(storage_error) = source.downcast_ref::<StorageError>() {
            return storage_sql_error_code(storage_error);
        }
        if let Some(io_error) = source.downcast_ref::<io::Error>()
            && let Some(storage_error) = io_error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<StorageError>())
        {
            return storage_sql_error_code(storage_error);
        }
        current = source.source();
    }

    match error.kind() {
        IcebergLiteErrorKind::FeatureUnsupported => {
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        }
        // Object-store / file IO failure surfacing from the scan or writer.
        IcebergLiteErrorKind::IoError => PgSqlErrorCode::ERRCODE_IO_ERROR,
        // Unparseable or corrupted Iceberg metadata / data files.
        IcebergLiteErrorKind::DataInvalid => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
        // Optimistic catalog commit lost the race to a concurrent update;
        // matches how managed-transaction commit conflicts are classified so the
        // executor can retry.
        IcebergLiteErrorKind::CatalogCommitConflicts => {
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        }
        // Optimistic row-level validation found concurrent data changes. The
        // same Iceberg commit must not be retried transparently; PostgreSQL
        // aborts the transaction and lets the client rebuild it.
        IcebergLiteErrorKind::DataConflict => {
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        }
        IcebergLiteErrorKind::TableNotFound => {
            PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE
        }
        IcebergLiteErrorKind::TableAlreadyExists => {
            PgSqlErrorCode::ERRCODE_DUPLICATE_TABLE
        }
        IcebergLiteErrorKind::NamespaceNotFound => {
            PgSqlErrorCode::ERRCODE_INVALID_SCHEMA_NAME
        }
        IcebergLiteErrorKind::NamespaceAlreadyExists => {
            PgSqlErrorCode::ERRCODE_DUPLICATE_SCHEMA
        }
        IcebergLiteErrorKind::PreconditionFailed => {
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
        }
        // `Unexpected` and any future `#[non_exhaustive]` kind: an opaque
        // internal error with no more specific SQLSTATE.
        _ => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
    }
}
