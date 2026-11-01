//! DataFusion execution components owned by the central query engine.

use std::sync::Arc;

use datafusion::logical_expr::AggregateUDF;

mod admission;
mod compiler;
mod error;
mod execution;
mod integer_abs;
mod join_planning;
mod memory;
mod metrics;
mod numeric_aggregate;
mod parallel;
mod physical_plan;
mod postgres_eval;
mod postgres_string_agg;
mod real_sum;
mod scan_binding;
mod scan_callbacks;
mod spill;
mod table_scan;

pub(in crate::datafusion) fn udaf_by_name(name: &str) -> Option<Arc<AggregateUDF>> {
    match name {
        numeric_aggregate::SUM_NAME => Some(numeric_aggregate::sum()),
        numeric_aggregate::AVG_NAME => Some(numeric_aggregate::avg()),
        postgres_string_agg::NAME => Some(postgres_string_agg::udaf()),
        _ => None,
    }
}

pub use admission::{DataFusionAdmissionDecline, DataFusionPlanAdmission};
pub use error::QueryExecutionError;
pub use execution::{
    ExecutionMetricsMode, PendingQueryExecution, QueryExecution,
    QueryExecutionRequest,
};
pub use memory::QueryExecutionLimits;
pub use metrics::{
    ExecutionMetricsSnapshot, QueryExecutionMode, ScanExecutionMetricsSnapshot,
};
pub use parallel::{
    InterruptHoldState, ParallelExecutionHost, ParallelQueryOptions,
    ParallelWorkerHost, ParallelWorkers, run_parallel_worker,
};
pub use scan_callbacks::{TableScanCallbacks, WorkerTableScanCallbacks};
