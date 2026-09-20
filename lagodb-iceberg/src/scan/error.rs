//! Catalog-independent Iceberg query and parallel-scan failures.

use lagodb_core::diag::SqlStateError;
use lagodb_core::runtime_api::RuntimePredicateCodecError;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::IcebergError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ScanError {
    #[error("failed to bind or plan Iceberg table scan: {0}")]
    Iceberg(#[from] IcebergError),
    #[error("invalid runtime pruning predicate: {0}")]
    RuntimePredicate(#[from] RuntimePredicateCodecError),
    #[error("invalid Iceberg parallel scan configuration: {0}")]
    ParallelScanConfiguration(String),
    #[error("invalid Iceberg worker source payload: {0}")]
    WorkerPayload(String),
}

impl SqlStateError for ScanError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Iceberg(error) => error.sql_error_code(),
            Self::RuntimePredicate(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::ParallelScanConfiguration(_) => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            Self::WorkerPayload(_) => PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
        }
    }
}
