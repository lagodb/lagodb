//! Per-scan physical metrics exposed through DataFusion EXPLAIN.

use std::time::Instant;

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use lagodb_core::runtime_api::TableScanTaskMetrics;

#[derive(Debug, Clone)]
pub(super) struct ScanExecMetrics {
    set: ExecutionPlanMetricsSet,
    planned_tasks: Count,
    planned_files: Count,
    planned_bytes: Count,
    dynamic_filter_input_rows: Count,
    dynamic_filter_output_rows: Count,
    // Opening the provider stream is timed apart from producing batches, as
    // DataFusion's FileStream does. Batch time is this scan's own elapsed_compute
    // and, unlike FileStream's time_elapsed_processing, includes the provider's
    // synchronous I/O, decompression, decode and pushed filters plus the runtime
    // filters applied here.
    time_elapsed_opening: Time,
    elapsed_compute: Time,
}

impl ScanExecMetrics {
    pub(super) fn new() -> Self {
        let set = ExecutionPlanMetricsSet::new();
        let planned_tasks = MetricBuilder::new(&set).counter("planned_tasks", 0);
        let planned_files = MetricBuilder::new(&set).counter("planned_files", 0);
        let planned_bytes = MetricBuilder::new(&set).counter("planned_bytes", 0);
        let dynamic_filter_input_rows =
            MetricBuilder::new(&set).counter("dynamic_filter_input_rows", 0);
        let dynamic_filter_output_rows =
            MetricBuilder::new(&set).counter("dynamic_filter_output_rows", 0);
        let time_elapsed_opening =
            MetricBuilder::new(&set).subset_time("time_elapsed_opening", 0);
        let elapsed_compute = MetricBuilder::new(&set).elapsed_compute(0);
        Self {
            set,
            planned_tasks,
            planned_files,
            planned_bytes,
            dynamic_filter_input_rows,
            dynamic_filter_output_rows,
            time_elapsed_opening,
            elapsed_compute,
        }
    }

    pub(super) fn record_opening(&self, started: Instant) {
        self.time_elapsed_opening.add_elapsed(started);
    }

    pub(super) fn record_compute(&self, started: Instant) {
        self.elapsed_compute.add_elapsed(started);
    }

    pub(super) fn record_task_plan(&self, metrics: TableScanTaskMetrics) {
        self.planned_tasks.add(metrics.planned_tasks as usize);
        self.planned_files.add(metrics.planned_files as usize);
        self.planned_bytes.add(metrics.planned_bytes as usize);
    }

    pub(super) fn record_dynamic_filter(
        &self,
        input_rows: usize,
        output_rows: usize,
    ) {
        self.dynamic_filter_input_rows.add(input_rows);
        self.dynamic_filter_output_rows.add(output_rows);
    }

    pub(super) fn snapshot(&self) -> MetricsSet {
        self.set.clone_inner()
    }
}
