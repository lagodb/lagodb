//! Managed-Iceberg query-source errors before the runtime FFI boundary.

use lagodb_core::diag::SqlStateError;
use lagodb_core::plan_data::PlanDataError;
use lagodb_core::query_contract::SourceEstimateError;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::IcebergError;

use super::IcebergSourcePlanError;

#[derive(Debug, thiserror::Error)]
pub(super) enum IcebergQuerySourceError {
    #[error("invalid Iceberg query source plan: {0}")]
    Plan(#[from] IcebergSourcePlanError),
    #[error("invalid Iceberg query source estimate: {0}")]
    Estimate(#[from] SourceEstimateError),
    #[error("failed to prepare Iceberg query source: {0}")]
    Iceberg(#[from] IcebergError),
    #[error("query source batch row limit {value} exceeds this platform")]
    BatchRowLimit { value: u64 },
}

impl From<PlanDataError> for IcebergQuerySourceError {
    fn from(error: PlanDataError) -> Self {
        Self::Plan(error.into())
    }
}

impl SqlStateError for IcebergQuerySourceError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Iceberg(error) => error.sql_error_code(),
            Self::Plan(_) | Self::Estimate(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
            Self::BatchRowLimit { .. } => {
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED
            }
        }
    }
}
