//! Shared distributed-plan and PG-worker execution substrate.

mod bootstrap;
mod catalog;
mod codec;
mod host;
mod lifecycle;
mod metrics;
mod provider;
mod run;
mod scan_exec;
mod scan_metrics;
mod session;
mod source_inventory;
mod stages;
mod transport;
mod worker;

pub(super) use catalog::ParallelSourceCatalog;
pub use host::{
    InterruptHoldState, ParallelExecutionHost, ParallelInterrupt, ParallelWorkerHost,
    ParallelWorkers,
};
pub(super) use provider::{ParallelTableProvider, ParallelTableScanBinding};
pub use run::ParallelQueryOptions;
pub(super) use run::{ParallelRun, PreparedParallelPlan};
pub(super) use scan_exec::ParallelTableScanExec;
pub(in crate::datafusion) use session::ParallelSession;
pub(in crate::datafusion) use transport::ParallelRuntime;
pub use worker::run_parallel_worker;
