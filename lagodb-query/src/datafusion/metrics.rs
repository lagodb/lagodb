//! Statement-cumulative metrics collected at scan batch boundaries.

use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use lagodb_core::query_contract::ScanId;
use lagodb_core::runtime_api::TableScanTaskMetrics;

#[derive(Debug, Default)]
struct ScanExecutionMetrics {
    input_batches: AtomicU64,
    input_rows: AtomicU64,
    arrow_batch_bytes: AtomicU64,
    planned_tasks: AtomicU64,
    planned_files: AtomicU64,
    planned_bytes: AtomicU64,
}

#[derive(Debug)]
pub(super) struct ExecutionMetrics {
    scans: Box<[ScanExecutionMetrics]>,
}

impl ExecutionMetrics {
    pub(super) fn new(scan_count: usize) -> Self {
        Self {
            scans: (0..scan_count)
                .map(|_| ScanExecutionMetrics::default())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    pub(super) fn record_input(&self, scan: ScanId, batch: &RecordBatch) {
        let metrics = &self.scans[scan.index()];
        metrics.input_batches.fetch_add(1, Ordering::Relaxed);
        metrics.input_rows.fetch_add(
            u64::try_from(batch.num_rows())
                .expect("Arrow batch row count fits in u64"),
            Ordering::Relaxed,
        );
        metrics.arrow_batch_bytes.fetch_add(
            u64::try_from(batch.get_array_memory_size())
                .expect("Arrow batch memory size fits in u64"),
            Ordering::Relaxed,
        );
    }

    pub(super) fn record_task_plan(
        &self,
        scan: ScanId,
        task_metrics: TableScanTaskMetrics,
    ) {
        let metrics = &self.scans[scan.index()];
        metrics
            .planned_tasks
            .fetch_add(task_metrics.planned_tasks, Ordering::Relaxed);
        metrics
            .planned_files
            .fetch_add(task_metrics.planned_files, Ordering::Relaxed);
        metrics
            .planned_bytes
            .fetch_add(task_metrics.planned_bytes, Ordering::Relaxed);
    }

    pub(super) fn snapshot(
        &self,
        engine_peak_memory_bytes: usize,
    ) -> ExecutionMetricsSnapshot {
        let scans = self
            .scans
            .iter()
            .map(|metrics| ScanExecutionMetricsSnapshot {
                input_batches: metrics.input_batches.load(Ordering::Relaxed),
                input_rows: metrics.input_rows.load(Ordering::Relaxed),
                arrow_batch_bytes: metrics.arrow_batch_bytes.load(Ordering::Relaxed),
                planned_tasks: metrics.planned_tasks.load(Ordering::Relaxed),
                planned_files: metrics.planned_files.load(Ordering::Relaxed),
                planned_bytes: metrics.planned_bytes.load(Ordering::Relaxed),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        ExecutionMetricsSnapshot {
            scans,
            engine_peak_memory_bytes: u64::try_from(engine_peak_memory_bytes)
                .expect("DataFusion memory usage fits in u64"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanExecutionMetricsSnapshot {
    pub input_batches: u64,
    pub input_rows: u64,
    pub arrow_batch_bytes: u64,
    pub planned_tasks: u64,
    pub planned_files: u64,
    pub planned_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionMetricsSnapshot {
    scans: Box<[ScanExecutionMetricsSnapshot]>,
    pub engine_peak_memory_bytes: u64,
}

impl ExecutionMetricsSnapshot {
    pub fn scan(&self, scan: ScanId) -> ScanExecutionMetricsSnapshot {
        self.scans[scan.index()]
    }
}
