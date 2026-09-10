//! Typed provider query-source SPI and Arrow C Stream runtime adapter.
//!
//! Provider implementations depend on this module; the host registry and
//! DataFusion consumer remain outside this crate and communicate through
//! `lagodb-core`'s Arrow-independent descriptor ABI.

mod adapter;
mod contract;
mod stream_export;

pub use adapter::TableScanAdapter;
pub use contract::{
    PlannedScan, PlannedScanTasks, RuntimePredicateUpdate, ScanPlanningContext,
    ScanProjection, ScanStreamOptions, ScanSupport, ScanTaskPlanningOptions,
    TableScanProvider, TableScanStream,
};
