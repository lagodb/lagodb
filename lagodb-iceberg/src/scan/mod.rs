//! Shared Iceberg scan planning and execution data plane.

pub(crate) mod batch;
pub(crate) mod columnar;
mod cost;
mod cursor;
mod error;
pub(crate) mod parallel;
mod read;
mod task_metrics;

pub(crate) use columnar::{ScanSourceBinding, ScanTaskPlanner};
pub(crate) use cost::{ReaderScanCost, ScanVolume};
pub(crate) use cursor::PgRowCursor;
pub(crate) use error::ScanError;
pub(crate) use read::{
    AnalyzeScanInput, CountRowsRead, IcebergReadSnapshot, PreparedIcebergRead,
    PreparedRowScan, ReaderPredicate, RowLocationScanInput, ScanPredicates,
    StablePruningPredicate,
};
pub(crate) use task_metrics::IcebergTaskMetrics;
