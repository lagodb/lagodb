//! DataFusion execution components owned by the central query engine.

mod execution;
mod expression_compiler;
mod memory;
mod metrics;
mod native_semantics;
mod numeric_aggregate;
mod parallel;
mod physical_plan;
mod plan_compiler;
mod postgres_eval;
mod scan_binding;
mod scan_callbacks;
mod table_scan;

pub use execution::{
    ExecutionMetricsMode, QueryExecution, QueryExecutionError, QueryExecutionRequest,
};
pub use memory::QueryExecutionLimits;
pub use metrics::{
    ExecutionMetricsSnapshot, QueryExecutionMode, ScanExecutionMetricsSnapshot,
};
pub use parallel::{
    InterruptHoldState, ParallelExecutionHost, ParallelQueryOptions,
    ParallelWorkerHost, ParallelWorkers, run_parallel_worker,
};
pub use scan_callbacks::{SerialTableScanCallbacks, WorkerTableScanCallbacks};
