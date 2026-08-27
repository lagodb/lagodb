//! Errors at the format-neutral object-cleanup boundary.

use std::fmt;

use crate::diag::{PgError, SqlStateError};
use pgrx::prelude::PgSqlErrorCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectCleanupCatalogOperation {
    Resolve,
    Open,
    Scan,
    Insert,
    Update,
    Delete,
}

impl fmt::Display for ObjectCleanupCatalogOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Resolve => "resolve",
            Self::Open => "open",
            Self::Scan => "scan",
            Self::Insert => "insert into",
            Self::Update => "update",
            Self::Delete => "delete from",
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectCleanupError {
    #[error("invalid maintenance target: {0}")]
    InvalidTarget(#[from] lagodb_storage::StorageError),

    #[error("failed to {operation} maintenance queue catalog: {source}")]
    Catalog {
        operation: ObjectCleanupCatalogOperation,
        #[source]
        source: PgError,
    },

    #[error("maintenance queue is not installed")]
    QueueUnavailable,

    #[error("invalid maintenance queue record: {0}")]
    InvalidRecord(String),

    #[error("failed to notify maintenance worker: {0}")]
    WorkerNotification(#[source] crate::extension_worker::WorkerNotificationError),

    #[error("maintenance producer name must not be empty or exceed 128 bytes")]
    InvalidProducer,

    #[error("maintenance source name exceeds 256 bytes")]
    InvalidSourceName,

    #[error("maintenance batch exceeds the configured limit of {0} items")]
    BatchTooLarge(usize),
}

impl ObjectCleanupError {
    pub(crate) fn catalog(
        operation: ObjectCleanupCatalogOperation,
        source: PgError,
    ) -> Self {
        Self::Catalog { operation, source }
    }
}

impl SqlStateError for ObjectCleanupError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::InvalidTarget(_)
            | Self::InvalidProducer
            | Self::InvalidSourceName => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::BatchTooLarge(_) => PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
            Self::Catalog { source, .. } => source.sql_error_code(),
            Self::QueueUnavailable => PgSqlErrorCode::ERRCODE_UNDEFINED_TABLE,
            Self::InvalidRecord(_) | Self::WorkerNotification(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
        }
    }
}
