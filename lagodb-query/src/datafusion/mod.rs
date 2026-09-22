//! DataFusion execution components owned by the central query engine.

mod compiler;
mod error;
mod execution;
mod integer_abs;
mod memory;
mod metrics;
mod numeric_aggregate;
mod parallel;
mod physical_plan;
mod postgres_eval;
mod scan_binding;
mod scan_callbacks;
mod table_scan;

pub use error::QueryExecutionError;
pub use execution::{ExecutionMetricsMode, QueryExecution, QueryExecutionRequest};
pub use memory::QueryExecutionLimits;
pub use metrics::{
    ExecutionMetricsSnapshot, QueryExecutionMode, ScanExecutionMetricsSnapshot,
};
pub use parallel::{
    InterruptHoldState, ParallelExecutionHost, ParallelQueryOptions,
    ParallelWorkerHost, ParallelWorkers, run_parallel_worker,
};
pub use scan_callbacks::{TableScanCallbacks, WorkerTableScanCallbacks};
