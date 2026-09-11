//! Shared Iceberg scan planning and execution data plane.

pub(crate) mod batch;
pub(crate) mod projection;
mod query_cursor;
mod spec;

pub(crate) use query_cursor::IcebergQueryCursor;
pub(crate) use spec::{
    AnalyzeScanInput, BoundQueryScanInput, MutationScanInput, QueryTaskPlanner,
    ScanSource, ScanSpec,
};
