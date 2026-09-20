//! Shared Iceberg scan planning and execution data plane.

pub(crate) mod batch;
mod error;
pub(crate) mod parallel;
pub(crate) mod projection;
pub(crate) mod query;
mod query_cursor;
mod spec;

pub(crate) use error::ScanError;
pub(crate) use query_cursor::QueryCursor;
pub(crate) use spec::{
    AnalyzeScanInput, BoundQueryScanInput, MutationScanInput, QueryTaskPlanner,
    ScanSource, ScanSpec,
};
