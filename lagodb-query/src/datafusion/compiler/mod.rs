//! Compilation of validated query semantics into DataFusion plans.

mod expression;
mod plan;

use datafusion::common::DataFusionError;
use pgrx::pg_sys;

pub(super) use plan::DataFusionPlanCompiler;

#[derive(Debug, thiserror::Error)]
pub(super) enum DataFusionPlanError {
    #[error("DataFusion plan compilation failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("query fragment references missing table scan {index}")]
    MissingScan { index: usize },
    #[error(
        "query expression references unprojected attribute {attno} in scan {scan}"
    )]
    MissingColumn {
        scan: usize,
        attno: pg_sys::AttrNumber,
    },
    #[error(
        "query expression uses unsupported PostgreSQL comparison operator {oid:?}"
    )]
    UnsupportedOperator { oid: pg_sys::Oid },
    #[error("query runtime value {index} is missing")]
    MissingRuntimeValue { index: usize },
    #[error("query runtime value has unsupported PostgreSQL type {oid:?}")]
    UnsupportedRuntimeType { oid: pg_sys::Oid },
    #[error("query runtime value could not be decoded as PostgreSQL type {oid:?}")]
    InvalidRuntimeValue { oid: pg_sys::Oid },
    #[error("LIMIT/OFFSET value is outside the execution range")]
    InvalidLimit,
    #[error("STRING_AGG delimiter is not valid UTF-8")]
    InvalidStringAggDelimiter,
    #[error("PostgreSQL expression fallback has an unsupported type contract")]
    UnsupportedPostgresExpression,
}
