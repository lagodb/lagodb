//! Shared Iceberg domain errors and PostgreSQL boundary conversion.
//!
//! Keep shared engine and AM business logic on [`IcebergResult<T>`] and
//! [`IcebergError`]. The PostgreSQL table-AM callback boundary returns
//! `lagodb_core::api::AmResult<T>`, which owns a PostgreSQL
//! `ErrorReport` through a small error handle.
//! The bridge is the `From<IcebergError> for ErrorReport` implementation in
//! this file, so callback methods can use normal `?` propagation.
//!
//! Avoid adding `try_*` callback wrappers or scattered
//! `.map_err(Into::into)` / `.into()` conversions in access-method code. If
//! third-party errors need adaptation, keep that inside meaningful Iceberg
//! object methods returning [`IcebergResult<T>`], then let the callback boundary
//! perform the final conversion to PostgreSQL. FDW-specific errors wrap this
//! type transparently and delegate [`SqlStateError`] to the source.

use std::error::Error as StdError;

use iceberg_lite::catalog::rest::{RestError, RestErrorKind};
use lagodb_core::diag::{PgError, SqlStateError, domain_error_report};
use lagodb_core::extension_worker::WorkerNotificationError;
use lagodb_core::object_cleanup::ObjectCleanupError;
use lagodb_core::options::TablespaceError;
use lagodb_core::options::{TableOptionError, TablespaceCacheError};
use lagodb_storage::{StorageError, StorageErrorKind};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

pub use crate::managed_table::catalog::error::{
    MetadataCatalogError, MetadataCatalogOperation,
};
pub use crate::managed_table::maintenance::error::IcebergVacuumError;

#[derive(Error, Debug)]
pub enum IcebergError {
    #[error(transparent)]
    MetadataCatalog(#[from] MetadataCatalogError),

    #[error("Iceberg VACUUM failed: {source}")]
    Vacuum {
        #[source]
        source: IcebergVacuumError,
    },

    #[error("metadata tracker error: {0}")]
    MetadataTracker(String),

    #[error(
        "Iceberg mutation exceeds the synthetic ctid limit of {max_files} data files per transaction and relation"
    )]
    FileIdLimitExceeded { max_files: usize },

    #[error("Iceberg row identity exceeds the synthetic ctid capacity")]
    RowIdentityLimitExceeded,

    #[error(
        "Iceberg ANALYZE physical population exceeds PostgreSQL synthetic TID capacity"
    )]
    AnalyzeTidCapacityExceeded,

    #[error(
        "failed to commit metadata for relid {relid} after {max_retries} retries due to concurrent updates"
    )]
    MetadataCommitConflict {
        relid: pg_sys::Oid,
        max_retries: i32,
    },

    #[error("cannot commit truncate: Iceberg metadata changed after TRUNCATE")]
    TruncateCommitConflict { relid: pg_sys::Oid },

    #[error("tablespace error: {0}")]
    TablespaceError(#[from] TablespaceError),

    #[error("tablespace cache error: {0}")]
    TablespaceCacheError(#[from] TablespaceCacheError),

    #[error("table option error: {0}")]
    TableOptionError(#[from] TableOptionError),

    #[error("storage error: {0}")]
    StorageError(#[from] lagodb_storage::StorageError),

    #[error("maintenance error: {0}")]
    ObjectCleanupError(#[from] ObjectCleanupError),

    #[error("failed to schedule Iceberg automatic maintenance: {source}")]
    AutomaticMaintenanceNotification {
        #[source]
        source: WorkerNotificationError,
    },

    #[error("managed Iceberg table {relid} has no persisted storage location")]
    ManagedTableLocationMissing { relid: pg_sys::Oid },

    #[error("local Iceberg partitioned table {relid} has no valid relfilenumber")]
    InvalidLocalStorageIdentity { relid: pg_sys::Oid },

    #[error("invalid managed Iceberg table location {location:?}: {reason}")]
    InvalidManagedTableLocation { location: String, reason: String },

    #[error("managed Iceberg table location {location:?} is not empty")]
    ManagedTableLocationNotEmpty { location: String },

    #[error("managed Iceberg table location {location:?} is still pending deletion")]
    ManagedTableLocationCleanupPending { location: String },

    #[error("postgres error: {0}")]
    PgError(#[from] PgError),

    #[error("Arrow/Datum conversion error: {0}")]
    ArrowConversion(#[from] lagodb_arrow::ArrowConversionError),

    #[error("tablespace options not found")]
    TablespaceNotFound,

    #[error("namespace name is null")]
    NamespaceNull,

    #[error("metadata location is null")]
    MetadataLocationNull,

    #[error("schema build error: {0}")]
    SchemaBuildError(String),

    #[error("invalid managed Iceberg partition definition: {0}")]
    InvalidPartitionDefinition(String),

    #[error("unsupported managed Iceberg partition definition: {0}")]
    UnsupportedPartitionDefinition(String),

    #[error("column {0} is not found in source")]
    ColumnNotFound(String),

    #[error(
        "required column \"{0}\" has no live PostgreSQL column to write \
         (was it dropped without a default?)"
    )]
    RequiredColumnMissingSource(String),

    #[error("column '{0}' data type is not supported")]
    UnsupportedColumnType(String),

    #[error("cannot import column '{0}' data type '{1}'")]
    ImportColumnError(String, String),

    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("datetime conversion error: {0}")]
    DatetimeConversionError(
        #[from] pgrx::datum::datetime_support::DateTimeConversionError,
    ),

    #[error("uuid error: {0}")]
    UuidConversionError(#[from] uuid::Error),

    #[error("numeric error: {0}")]
    NumericError(#[from] pgrx::datum::numeric_support::error::Error),

    #[error("iceberg error: {0}")]
    IcebergLiteError(#[from] iceberg_lite::Error),

    #[error("Iceberg schema evolution conflict: {source}")]
    SchemaEvolutionConflict {
        #[source]
        source: iceberg_lite::Error,
    },

    #[error("arrow error: {0}")]
    ArrowError(#[from] arrow_schema::ArrowError),

    #[error("arrow type mismatch: expected {0}")]
    ArrowTypeMismatch(String),

    #[error("SPI error: {0}")]
    SpiError(String),

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
            IcebergError::MetadataCatalog(error) => error.sql_error_code(),

            IcebergError::Vacuum { source } => source.sql_error_code(),

            IcebergError::TablespaceError(error) => error.sql_error_code(),

            IcebergError::TablespaceCacheError(error) => error.sql_error_code(),

            IcebergError::TableOptionError(error) => error.sql_error_code(),

            IcebergError::StorageError(error) => storage_sql_error_code(error),

            IcebergError::ObjectCleanupError(error) => error.sql_error_code(),

            IcebergError::AutomaticMaintenanceNotification { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }

            IcebergError::ManagedTableLocationMissing { .. }
            | IcebergError::InvalidLocalStorageIdentity { .. } => {
                PgSqlErrorCode::ERRCODE_DATA_CORRUPTED
            }

            IcebergError::InvalidManagedTableLocation { .. } => {
                PgSqlErrorCode::ERRCODE_DATA_CORRUPTED
            }

            IcebergError::ManagedTableLocationNotEmpty { .. } => {
                PgSqlErrorCode::ERRCODE_DUPLICATE_FILE
            }

            IcebergError::ManagedTableLocationCleanupPending { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_IN_USE
            }

            IcebergError::TablespaceNotFound => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }

            IcebergError::PgError(error) => error.sql_error_code(),

            IcebergError::ArrowConversion(conv) => conv.sql_error_code(),

            IcebergError::MetadataTracker(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::FileIdLimitExceeded { .. }
            | IcebergError::RowIdentityLimitExceeded
            | IcebergError::AnalyzeTidCapacityExceeded => {
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED
            }

            IcebergError::MetadataCommitConflict { .. }
            | IcebergError::TruncateCommitConflict { .. } => {
                PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
            }

            IcebergError::NamespaceNull | IcebergError::MetadataLocationNull => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
            }

            IcebergError::SchemaBuildError(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION
            }

            IcebergError::InvalidPartitionDefinition(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION
            }

            IcebergError::UnsupportedPartitionDefinition(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }

            IcebergError::ColumnNotFound(_) => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN
            }

            IcebergError::RequiredColumnMissingSource(_) => {
                PgSqlErrorCode::ERRCODE_NOT_NULL_VIOLATION
            }

            IcebergError::UnsupportedColumnType(_)
            | IcebergError::ImportColumnError(_, _) => {
                PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH
            }

            IcebergError::ParseFloatError(_)
            | IcebergError::DatetimeConversionError(_)
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

            IcebergError::SpiError(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,

            IcebergError::InvariantViolated(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }

            IcebergError::NotImplemented(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
        }
    }
}

impl From<IcebergError> for ErrorReport {
    fn from(value: IcebergError) -> Self {
        domain_error_report(value)
    }
}

pub type IcebergResult<T> = Result<T, IcebergError>;

impl From<IcebergError> for lagodb_core::table_maintenance::TableMaintenanceError {
    fn from(source: IcebergError) -> Self {
        Self::provider(source)
    }
}

impl IcebergError {
    pub fn schema_evolution_conflict(source: iceberg_lite::Error) -> Self {
        Self::SchemaEvolutionConflict { source }
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
        StorageErrorKind::Protocol
        | StorageErrorKind::ClosedHandle
        | StorageErrorKind::ExpiredCursor
        | StorageErrorKind::Conflict => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        StorageErrorKind::Ambiguous => PgSqlErrorCode::ERRCODE_IO_ERROR,
    }
}

fn iceberg_lite_sql_error_code(error: &iceberg_lite::Error) -> PgSqlErrorCode {
    if let Some(rest_error) = error
        .source()
        .and_then(|source| source.downcast_ref::<RestError>())
    {
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

    use iceberg_lite::ErrorKind;
    match error.kind() {
        ErrorKind::FeatureUnsupported => {
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        }
        // Object-store / file IO failure surfacing from the scan or writer.
        ErrorKind::IoError => PgSqlErrorCode::ERRCODE_IO_ERROR,
        // Unparseable or corrupted Iceberg metadata / data files.
        ErrorKind::DataInvalid => PgSqlErrorCode::ERRCODE_DATA_CORRUPTED,
        // Optimistic catalog commit lost the race to a concurrent update;
        // matches how the metadata-tracker conflicts are classified so the
        // executor can retry.
        ErrorKind::CatalogCommitConflicts => {
            PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
        }
        // Optimistic row-level validation found concurrent data changes. The
        // same Iceberg commit must not be retried transparently; PostgreSQL
        // aborts the transaction and lets the client rebuild it.
        ErrorKind::DataConflict => PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE,
        ErrorKind::TableNotFound => PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
        ErrorKind::TableAlreadyExists => PgSqlErrorCode::ERRCODE_DUPLICATE_TABLE,
        ErrorKind::NamespaceNotFound => PgSqlErrorCode::ERRCODE_INVALID_SCHEMA_NAME,
        ErrorKind::NamespaceAlreadyExists => PgSqlErrorCode::ERRCODE_DUPLICATE_SCHEMA,
        ErrorKind::PreconditionFailed => {
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
        }
        // `Unexpected` and any future `#[non_exhaustive]` kind: an opaque
        // internal error with no more specific SQLSTATE.
        _ => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
    }
}
