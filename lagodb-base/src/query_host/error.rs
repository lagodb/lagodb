//! Query-host errors converted only at PostgreSQL callback boundaries.

use std::fmt::Display;

use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_query::datafusion::QueryExecutionError;
use pgrx::prelude::PgSqlErrorCode;

#[derive(Debug, thiserror::Error)]
pub(super) enum QueryHostError {
    #[error("invalid query-offload plan: {detail}")]
    InvalidPlan { detail: String },
    #[error("query-offload executor contract is invalid: {0}")]
    ExecutorContract(&'static str),
    #[error("query-offload memory budget exceeds the host address space")]
    MemoryBudgetOverflow,
    #[error("table scan failed: {0}")]
    TableScan(#[source] PgReportError),
    #[error("query engine failed: {0}")]
    Execution(#[source] QueryExecutionError),
}

impl QueryHostError {
    pub(super) fn invalid_plan(error: impl Display) -> Self {
        Self::InvalidPlan {
            detail: error.to_string(),
        }
    }

    pub(super) fn into_report(self) -> PgReportError {
        match self {
            Self::TableScan(error) => error,
            Self::Execution(error) => error.into_report(),
            error => PgReportError::from_domain_error(error),
        }
    }
}

impl From<PgReportError> for QueryHostError {
    fn from(error: PgReportError) -> Self {
        Self::TableScan(error)
    }
}

impl From<QueryExecutionError> for QueryHostError {
    fn from(error: QueryExecutionError) -> Self {
        Self::Execution(error)
    }
}

impl SqlStateError for QueryHostError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::TableScan(error) => error.sql_error_code(),
            Self::Execution(error) => error.sql_error_code(),
            Self::InvalidPlan { .. }
            | Self::ExecutorContract(_)
            | Self::MemoryBudgetOverflow => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}
