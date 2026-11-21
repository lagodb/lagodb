//! Query-offload provider failures before the runtime callback boundary.

use bincode::Error as BinaryCodecError;
use lagodb_core::diag::{PgError, PgReportError, SqlStateError};
use lagodb_core::expr::ExpressionCodecError;
use lagodb_core::plan_data::PlanDataError;
use lagodb_core::query_contract::ScanCostError;
use lagodb_core::runtime_api::RuntimePredicateCodecError;
use pgrx::prelude::PgSqlErrorCode;

use crate::error::ConnectorError;

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error("Parquet table scan projection kind {found} is unsupported")]
    UnknownProjection { found: i32 },
    #[error("invalid Parquet table scan estimate: {0}")]
    Cost(#[from] ScanCostError),
    #[error("invalid shared query expression: {0}")]
    Expression(#[from] ExpressionCodecError),
    #[error("invalid runtime pruning predicate: {0}")]
    RuntimePredicate(#[from] RuntimePredicateCodecError),
    #[error("invalid Parquet worker payload: {0}")]
    WorkerPayload(Box<str>),
    #[error("Parquet table scan invariant violated: {0}")]
    Invariant(&'static str),
    #[error("table scan batch row limit {value} exceeds this platform")]
    BatchRowLimit { value: u64 },
}

impl Error {
    pub(super) fn worker_payload(reason: impl Into<Box<str>>) -> Self {
        Self::WorkerPayload(reason.into())
    }
}

impl From<PlanDataError> for Error {
    fn from(error: PlanDataError) -> Self {
        Self::Connector(error.into())
    }
}

impl From<PgError> for Error {
    fn from(error: PgError) -> Self {
        Self::Connector(ConnectorError::Postgres(PgReportError::from_pg_error(
            error,
        )))
    }
}

impl From<BinaryCodecError> for Error {
    fn from(error: BinaryCodecError) -> Self {
        Self::worker_payload(error.to_string())
    }
}

impl SqlStateError for Error {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Connector(error) => error.sql_error_code(),
            Self::BatchRowLimit { .. } => {
                PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED
            }
            Self::UnknownProjection { .. }
            | Self::Cost(_)
            | Self::Expression(_)
            | Self::RuntimePredicate(_)
            | Self::WorkerPayload(_)
            | Self::Invariant(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }

    fn try_into_pg_report(self) -> Result<PgReportError, Self> {
        match self {
            Self::Connector(error) => error
                .try_into_pg_report()
                .map(|report| report.contextualize("Parquet table scan failed", None))
                .map_err(Self::Connector),
            error => Err(error),
        }
    }
}
