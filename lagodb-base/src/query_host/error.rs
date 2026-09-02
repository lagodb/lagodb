//! Query-host errors converted only at PostgreSQL callback boundaries.

use std::fmt::Display;

use lagodb_core::diag::{PgReportError, SqlStateError};
use lagodb_query::datafusion::QueryExecutionError;
use pgrx::prelude::PgSqlErrorCode;

#[derive(Debug, thiserror::Error)]
pub(super) enum QueryHostError {
    #[error("invalid AggregateScan plan: {detail}")]
    InvalidPlan { detail: String },
    #[error("AggregateScan executor contract is invalid: {0}")]
    ExecutorContract(&'static str),
    #[error("AggregateScan memory budget exceeds the host address space")]
    MemoryBudgetOverflow,
    #[error("query source failed: {0}")]
    Source(#[source] PgReportError),
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
            Self::Source(error) => error,
            Self::Execution(error) => error.into_report(),
            error => PgReportError::from_domain_error(error),
        }
    }
}

impl From<PgReportError> for QueryHostError {
    fn from(error: PgReportError) -> Self {
        Self::Source(error)
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
            Self::Source(error) => error.sql_error_code(),
            Self::Execution(error) => error.sql_error_code(),
            Self::InvalidPlan { .. }
            | Self::ExecutorContract(_)
            | Self::MemoryBudgetOverflow => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}
