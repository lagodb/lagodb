//! Catalog-independent Iceberg query and parallel-scan failures.

use bincode::Error as BinaryCodecError;
use iceberg_lite::Error as IcebergLiteError;
use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_core::parallel_scan::ParallelScanError;
use lagodb_core::runtime_api::RuntimePredicateCodecError;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::IcebergError;

use super::parallel::TaskGroupingError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ScanError {
    #[error("failed to bind or plan Iceberg table scan: {0}")]
    Iceberg(#[from] IcebergError),
    #[error("invalid runtime pruning predicate: {0}")]
    RuntimePredicate(#[from] RuntimePredicateCodecError),
    #[error("invalid Iceberg parallel scan configuration: {0}")]
    ParallelScanConfiguration(#[from] TaskGroupingError),
    #[error("invalid Iceberg worker source payload: {0}")]
    WorkerPayload(String),
    #[error("invalid PostgreSQL parallel scan state: {0}")]
    ParallelCoordinator(#[from] ParallelScanError),
}

impl From<IcebergLiteError> for ScanError {
    fn from(error: IcebergLiteError) -> Self {
        Self::Iceberg(error.into())
    }
}

impl From<BinaryCodecError> for ScanError {
    fn from(error: BinaryCodecError) -> Self {
        Self::Iceberg(error.into())
    }
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
            Self::ParallelCoordinator(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }

    fn try_into_pg_report(self) -> Result<PgReportError, Self> {
        match self {
            Self::Iceberg(error) => error
                .try_into_pg_report()
                .map(|report| {
                    report.contextualize(
                        "failed to bind or plan Iceberg table scan",
                        None,
                    )
                })
                .map_err(Self::Iceberg),
            error => Err(error),
        }
    }
}
