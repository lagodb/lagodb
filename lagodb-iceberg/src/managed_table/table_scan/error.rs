//! Managed-Iceberg table-scan errors before the runtime callback boundary.

use lagodb_core::diag::{PgError, SqlStateError};
use lagodb_core::expr::ExpressionCodecError;
use lagodb_core::plan_data::PlanDataError;
use lagodb_core::query_contract::ScanCostError;
use lagodb_core::runtime_api::RuntimePredicateCodecError;
use pgrx::prelude::PgSqlErrorCode;

use crate::engine::predicate::IcebergFilterError;
use crate::error::IcebergError;

use super::IcebergScanPlanError;

#[derive(Debug, thiserror::Error)]
pub(super) enum IcebergTableScanError {
    #[error("invalid Iceberg table scan plan: {0}")]
    Plan(#[from] IcebergScanPlanError),
    #[error("invalid Iceberg table scan estimate: {0}")]
    Cost(#[from] ScanCostError),
    #[error("failed to bind or plan Iceberg table scan: {0}")]
    Iceberg(#[from] IcebergError),
    #[error("failed to plan or bind Iceberg pruning predicate: {0}")]
    Filter(#[from] IcebergFilterError),
    #[error("invalid shared query expression: {0}")]
    Expression(#[from] ExpressionCodecError),
    #[error("invalid runtime pruning predicate: {0}")]
    RuntimePredicate(#[from] RuntimePredicateCodecError),
    #[error("table scan batch row limit {value} exceeds this platform")]
    BatchRowLimit { value: u64 },
}

impl From<PlanDataError> for IcebergTableScanError {
    fn from(error: PlanDataError) -> Self {
        Self::Plan(error.into())
    }
}

impl From<PgError> for IcebergTableScanError {
    fn from(error: PgError) -> Self {
        Self::Iceberg(error.into())
    }
}

impl SqlStateError for IcebergTableScanError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Iceberg(error) => error.sql_error_code(),
            Self::Filter(error) => error.sql_error_code(),
            Self::Plan(_) | Self::Cost(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::Expression(_) | Self::RuntimePredicate(_) => {
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
            }
            Self::BatchRowLimit { .. } => {
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED
            }
        }
    }
}
