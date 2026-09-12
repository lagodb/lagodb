//! DataFusion execution components owned by the central query engine.

mod execution;
mod expression_compiler;
mod memory;
mod metrics;
mod native_semantics;
mod numeric_aggregate;
mod physical_plan;
mod plan_compiler;
mod postgres_eval;
mod scan_binding;
mod scan_callbacks;
mod table_scan;

pub use execution::{
    ExecutionMetricsMode, QueryExecutionError, SerialQueryExecution,
};
pub use memory::SerialExecutionLimits;
pub use metrics::{ExecutionMetricsSnapshot, ScanExecutionMetricsSnapshot};
pub use scan_callbacks::SerialTableScanCallbacks;
