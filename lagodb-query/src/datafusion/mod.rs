//! DataFusion execution components owned by the central query engine.

mod audit;
mod execution;
mod memory;
mod metrics;
mod plan_compiler;
mod source;
mod source_ffi;

pub use audit::PhysicalPlanAuditError;
pub use execution::{QueryExecutionError, SerialCountExecution};
pub use memory::SerialExecutionLimits;
pub use metrics::ExecutionMetricsSnapshot;
pub use source_ffi::SerialSourceCallbacks;
