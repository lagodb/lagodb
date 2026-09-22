//! Shared Iceberg scan planning and execution data plane.

pub(crate) mod batch;
mod error;
pub(crate) mod parallel;
pub(crate) mod projection;
pub(crate) mod query;
mod query_cursor;
mod read;

pub(crate) use error::ScanError;
pub(crate) use query::{QuerySourceBinding, QueryTaskPlanner};
pub(crate) use query_cursor::PgRowCursor;
pub(crate) use read::{
    AnalyzeScanInput, CountRowsRead, IcebergReadSnapshot, PreparedIcebergRead,
    PreparedRowScan, ReaderPredicate, RowLocationScanInput, ScanPredicates,
    StablePruningPredicate,
};
