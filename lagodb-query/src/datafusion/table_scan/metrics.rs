//! Per-scan physical metrics exposed through DataFusion EXPLAIN.

use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
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
        Self {
            set,
            planned_tasks,
            planned_files,
            planned_bytes,
            dynamic_filter_input_rows,
            dynamic_filter_output_rows,
        }
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
