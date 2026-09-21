//! Query-offload provider failures before the runtime callback boundary.

use lagodb_core::diag::{PgError, SqlStateError};
use lagodb_core::expr::ExpressionCodecError;
use lagodb_core::plan_data::PlanDataError;
use lagodb_core::query_contract::ScanCostError;
use lagodb_core::runtime_api::RuntimePredicateCodecError;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::IcebergError;
use crate::foreign_table::IcebergFdwError;
use crate::predicate::IcebergFilterError;
use crate::scan::ScanError;

use super::plan::PlanError;

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("invalid Iceberg table scan plan: {0}")]
    Plan(#[from] PlanError),
    #[error("invalid Iceberg table scan estimate: {0}")]
    Cost(#[from] ScanCostError),
    #[error("failed to bind or plan Iceberg table scan: {0}")]
    Iceberg(#[from] IcebergError),
    #[error("failed to plan or bind foreign Iceberg table scan: {0}")]
    Foreign(#[source] Box<IcebergFdwError>),
    #[error("failed to plan or bind Iceberg pruning predicate: {0}")]
    Filter(#[from] IcebergFilterError),
    #[error("Iceberg scan data plane failed: {0}")]
    Scan(#[from] ScanError),
    #[error("invalid shared query expression: {0}")]
    Expression(#[from] ExpressionCodecError),
    #[error("invalid runtime pruning predicate: {0}")]
    RuntimePredicate(#[from] RuntimePredicateCodecError),
    #[error("table scan batch row limit {value} exceeds this platform")]
    BatchRowLimit { value: u64 },
}

impl From<IcebergFdwError> for Error {
    fn from(error: IcebergFdwError) -> Self {
        Self::Foreign(Box::new(error))
    }
}

impl From<PlanDataError> for Error {
    fn from(error: PlanDataError) -> Self {
        Self::Plan(error.into())
    }
}

impl From<PgError> for Error {
    fn from(error: PgError) -> Self {
        Self::Iceberg(error.into())
    }
}

impl SqlStateError for Error {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Iceberg(error) => error.sql_error_code(),
            Self::Foreign(error) => error.sql_error_code(),
            Self::Filter(error) => error.sql_error_code(),
            Self::Scan(error) => error.sql_error_code(),
            Self::BatchRowLimit { .. } => {
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED
            }
            Self::Plan(_)
            | Self::Cost(_)
            | Self::Expression(_)
            | Self::RuntimePredicate(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}
