//! Typed provider table-scan SPI and Arrow C Stream runtime adapter.
//!
//! Provider implementations depend on this module; the host registry and
//! DataFusion consumer remain outside this crate and communicate through
//! `lagodb-core`'s Arrow-independent descriptor ABI.

mod adapter;
mod c_stream;
mod parallel;
mod provider;

pub use adapter::TableScanAdapter;
pub use parallel::{
    TableScanWorkerAdapter, TableScanWorkerProvider, WorkerSourcePayload,
    WorkerStreamOptions,
};
pub use provider::{
    PlannedScan, PlannedScanTasks, RuntimePredicateUpdate, ScanPlanningContext,
    ScanProjection, ScanStreamOptions, ScanSupport, ScanTaskPlanningOptions,
    TableScanProvider, TableScanStream,
};
