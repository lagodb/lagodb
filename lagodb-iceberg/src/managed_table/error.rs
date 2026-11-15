//! Managed-table adapter errors.
//!
//! Like the FDW adapter's `IcebergFdwError`, [`ManagedTableError`] wraps the
//! shared [`IcebergError`] transparently and adds the catalog, maintenance,
//! storage-placement and DDL failures that only the table access method
//! produces. Shared failures keep their message, source chain and SQLSTATE.

use std::borrow::Cow;

use lagodb_core::diag::{PgError, PgReportError, SqlStateError};
use lagodb_core::extension_worker::WorkerNotificationError;
use lagodb_core::object_cleanup::ObjectCleanupError;
use lagodb_core::options::TableOptionError;
use lagodb_core::storage::tablespace::TablespaceStorageError;
use lagodb_core::table_maintenance::TableMaintenanceError;
use lagodb_storage::StorageError;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use super::catalog::error::MetadataCatalogError;
use super::maintenance::error::IcebergVacuumError;
use crate::error::IcebergError;
use crate::predicate::IcebergFilterError;

#[derive(Error, Debug)]
pub(crate) enum ManagedTableError {
    #[error(transparent)]
    Iceberg(#[from] IcebergError),

    #[error(transparent)]
    Filter(#[from] IcebergFilterError),

    #[error(transparent)]
    MetadataCatalog(#[from] MetadataCatalogError),

    #[error("Iceberg VACUUM failed: {source}")]
    Vacuum {
        #[source]
        source: IcebergVacuumError,
    },

    #[error("managed transaction error: {0}")]
    ManagedTransaction(String),

    #[error(
        "failed to commit metadata for relid {relid} after {max_retries} retries due to concurrent updates"
    )]
    MetadataCommitConflict {
        relid: pg_sys::Oid,
        max_retries: i32,
    },

    #[error("cannot commit truncate: Iceberg metadata changed after TRUNCATE")]
    TruncateCommitConflict { relid: pg_sys::Oid },

    #[error("tablespace storage error: {0}")]
    TablespaceStorageError(#[from] TablespaceStorageError),

    #[error("table option error: {0}")]
    TableOptionError(#[from] TableOptionError),

    #[error("maintenance error: {0}")]
    ObjectCleanupError(#[from] ObjectCleanupError),

    #[error("failed to schedule Iceberg automatic maintenance: {source}")]
    AutomaticMaintenanceNotification {
        #[source]
        source: WorkerNotificationError,
    },

    #[error("managed Iceberg table {relid} has no persisted storage location")]
    ManagedTableLocationMissing { relid: pg_sys::Oid },

    #[error("invalid managed Iceberg table location {location:?}: {reason}")]
    InvalidManagedTableLocation { location: String, reason: String },

    #[error("managed Iceberg table location {location:?} is not empty")]
    ManagedTableLocationNotEmpty { location: String },

    #[error("managed Iceberg table location {location:?} is still pending deletion")]
    ManagedTableLocationCleanupPending { location: String },

    #[error("namespace name is null")]
    NamespaceNull,

    #[error("metadata location is null")]
    MetadataLocationNull,

    #[error("invalid managed Iceberg partition definition: {0}")]
    InvalidPartitionDefinition(Cow<'static, str>),

    #[error("unsupported managed Iceberg partition definition: {0}")]
    UnsupportedPartitionDefinition(Cow<'static, str>),
}

pub(crate) type ManagedResult<T> = Result<T, ManagedTableError>;

impl SqlStateError for ManagedTableError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Iceberg(error) => error.sql_error_code(),
            Self::Filter(error) => error.sql_error_code(),
            Self::MetadataCatalog(error) => error.sql_error_code(),
            Self::Vacuum { source } => source.sql_error_code(),
            Self::TablespaceStorageError(error) => error.sql_error_code(),
            Self::TableOptionError(error) => error.sql_error_code(),
            Self::ObjectCleanupError(error) => error.sql_error_code(),
            Self::ManagedTransaction(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::MetadataCommitConflict { .. }
            | Self::TruncateCommitConflict { .. } => {
                PgSqlErrorCode::ERRCODE_T_R_SERIALIZATION_FAILURE
            }
            Self::AutomaticMaintenanceNotification { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
            }
            Self::ManagedTableLocationMissing { .. }
            | Self::InvalidManagedTableLocation { .. } => {
                PgSqlErrorCode::ERRCODE_DATA_CORRUPTED
            }
            Self::ManagedTableLocationNotEmpty { .. } => {
                PgSqlErrorCode::ERRCODE_DUPLICATE_FILE
            }
            Self::ManagedTableLocationCleanupPending { .. } => {
                PgSqlErrorCode::ERRCODE_OBJECT_IN_USE
            }
            Self::NamespaceNull | Self::MetadataLocationNull => {
                PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT
            }
            Self::InvalidPartitionDefinition(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION
            }
            Self::UnsupportedPartitionDefinition(_) => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
        }
    }

    fn try_into_pg_report(self) -> Result<PgReportError, Self> {
        match self {
            Self::Iceberg(error) => error.try_into_pg_report().map_err(Self::Iceberg),
            Self::Filter(error) => error.try_into_pg_report().map_err(Self::Filter),
            Self::MetadataCatalog(error) => {
                error.try_into_pg_report().map_err(Self::MetadataCatalog)
            }
            Self::TablespaceStorageError(error) => error
                .try_into_pg_report()
                .map(|report| report.contextualize("tablespace storage error", None))
                .map_err(Self::TablespaceStorageError),
            Self::TableOptionError(error) => error
                .try_into_pg_report()
                .map(|report| report.contextualize("table option error", None))
                .map_err(Self::TableOptionError),
            Self::ObjectCleanupError(error) => error
                .try_into_pg_report()
                .map(|report| report.contextualize("maintenance error", None))
                .map_err(Self::ObjectCleanupError),
            error => Err(error),
        }
    }
}

impl From<PgError> for ManagedTableError {
    fn from(error: PgError) -> Self {
        Self::Iceberg(error.into())
    }
}

impl From<PgReportError> for ManagedTableError {
    fn from(error: PgReportError) -> Self {
        Self::Iceberg(error.into())
    }
}

impl From<iceberg_lite::Error> for ManagedTableError {
    fn from(error: iceberg_lite::Error) -> Self {
        Self::Iceberg(error.into())
    }
}

impl From<StorageError> for ManagedTableError {
    fn from(error: StorageError) -> Self {
        Self::Iceberg(error.into())
    }
}

impl From<ManagedTableError> for TableMaintenanceError {
    fn from(source: ManagedTableError) -> Self {
        Self::provider(source)
    }
}

impl ManagedTableError {
    pub(crate) fn invalid_partition(message: impl Into<Cow<'static, str>>) -> Self {
        Self::InvalidPartitionDefinition(message.into())
    }

    pub(crate) fn unsupported_partition(
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self::UnsupportedPartitionDefinition(message.into())
    }
}
