//! Scan batch counters carried by normal distributed task reports.

use arrow_array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Label, MetricBuilder, MetricsSet,
};
use lagodb_core::query_contract::ScanId;

use crate::datafusion::metrics::ExecutionMetrics;

const SCAN_LABEL: &str = "lagodb_scan";

#[derive(Debug, Clone)]
pub(super) struct ParallelScanMetrics {
    set: ExecutionPlanMetricsSet,
    batches: Count,
    rows: Count,
    bytes: Count,
}

impl ParallelScanMetrics {
    pub(super) fn new(scan: ScanId) -> Self {
        let set = ExecutionPlanMetricsSet::new();
        let label = Label::new(SCAN_LABEL, scan.index().to_string());
        let batches = MetricBuilder::new(&set)
            .with_label(label.clone())
            .counter("input_batches", 0);
        let rows = MetricBuilder::new(&set)
            .with_label(label.clone())
            .counter("input_rows", 0);
        let bytes = MetricBuilder::new(&set)
            .with_label(label)
            .counter("arrow_batch_bytes", 0);
        Self {
            set,
            batches,
            rows,
            bytes,
        }
    }

    pub(super) fn record(&self, batch: &RecordBatch) {
        self.batches.add(1);
        self.rows.add(batch.num_rows());
        self.bytes.add(batch.get_array_memory_size());
    }

    pub(super) fn snapshot(&self) -> MetricsSet {
        self.set.clone_inner()
    }

    pub(super) fn merge_worker_report(
        report: &MetricsSet,
        execution: &ExecutionMetrics,
    ) -> Result<()> {
        let mut scan = None;
        let (mut batches, mut rows, mut bytes) = (0, 0, 0);
        for metric in report.iter() {
            let Some(label) = metric
                .labels()
                .iter()
                .find(|label| label.name() == SCAN_LABEL)
            else {
                continue;
            };
            if scan.is_none() {
                scan = Some(ScanId::from_index(label.value().parse().map_err(
                    |error| {
                        DataFusionError::Execution(format!(
                            "invalid scan identity in worker metrics: {error}"
                        ))
                    },
                )?));
            }
            let value = metric.value().as_usize() as u64;
            match metric.value().name() {
                "input_batches" => batches += value,
                "input_rows" => rows += value,
                "arrow_batch_bytes" => bytes += value,
                _ => {}
            }
        }
        if let Some(scan) = scan {
            execution.record_worker_input(scan, batches, rows, bytes);
        }
        Ok(())
    }
}
