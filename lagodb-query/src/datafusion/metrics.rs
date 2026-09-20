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
    serial_runs: AtomicU64,
    parallel_runs: AtomicU64,
    maximum_workers: AtomicU64,
    worker_metric_reports: AtomicU64,
    expected_worker_metric_reports: AtomicU64,
    maximum_worker_fragment_peak_memory_bytes: AtomicU64,
    maximum_worker_participant_peak_memory_bytes: AtomicU64,
}

impl ExecutionMetrics {
    pub(super) fn new(scan_count: usize) -> Self {
        Self {
            scans: (0..scan_count)
                .map(|_| ScanExecutionMetrics::default())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            serial_runs: AtomicU64::new(0),
            parallel_runs: AtomicU64::new(0),
            maximum_workers: AtomicU64::new(0),
            worker_metric_reports: AtomicU64::new(0),
            expected_worker_metric_reports: AtomicU64::new(0),
            maximum_worker_fragment_peak_memory_bytes: AtomicU64::new(0),
            maximum_worker_participant_peak_memory_bytes: AtomicU64::new(0),
        }
    }

    pub(super) fn record_serial_run(&self) {
        self.serial_runs.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_parallel_run(&self, workers: u32) {
        self.parallel_runs.fetch_add(1, Ordering::Relaxed);
        self.maximum_workers
            .fetch_max(u64::from(workers), Ordering::Relaxed);
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

    pub(super) fn record_worker_input(
        &self,
        scan: ScanId,
        batches: u64,
        rows: u64,
        bytes: u64,
    ) {
        let metrics = &self.scans[scan.index()];
        metrics.input_batches.fetch_add(batches, Ordering::Relaxed);
        metrics.input_rows.fetch_add(rows, Ordering::Relaxed);
        metrics
            .arrow_batch_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn record_worker_metric_reports(
        &self,
        received: usize,
        expected: usize,
    ) {
        self.worker_metric_reports.fetch_add(
            u64::try_from(received).expect("worker report count fits in u64"),
            Ordering::Relaxed,
        );
        self.expected_worker_metric_reports.fetch_add(
            u64::try_from(expected).expect("worker report count fits in u64"),
            Ordering::Relaxed,
        );
    }

    pub(super) fn record_worker_memory_peaks(
        &self,
        fragment_peak_memory_bytes: u64,
        participant_peak_memory_bytes: u64,
    ) {
        self.maximum_worker_fragment_peak_memory_bytes
            .fetch_max(fragment_peak_memory_bytes, Ordering::Relaxed);
        self.maximum_worker_participant_peak_memory_bytes
            .fetch_max(participant_peak_memory_bytes, Ordering::Relaxed);
    }

    pub(super) fn snapshot(
        &self,
        local_peak_memory_bytes: usize,
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
            local_peak_memory_bytes: u64::try_from(local_peak_memory_bytes)
                .expect("DataFusion memory usage fits in u64"),
            serial_runs: self.serial_runs.load(Ordering::Relaxed),
            parallel_runs: self.parallel_runs.load(Ordering::Relaxed),
            maximum_workers: self.maximum_workers.load(Ordering::Relaxed),
            worker_metric_reports: self.worker_metric_reports.load(Ordering::Relaxed),
            expected_worker_metric_reports: self
                .expected_worker_metric_reports
                .load(Ordering::Relaxed),
            maximum_worker_fragment_peak_memory_bytes: self
                .maximum_worker_fragment_peak_memory_bytes
                .load(Ordering::Relaxed),
            maximum_worker_participant_peak_memory_bytes: self
                .maximum_worker_participant_peak_memory_bytes
                .load(Ordering::Relaxed),
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
    /// Peak memory reserved by execution pools owned by the backend running the
    /// CustomScan, across both serial and parallel-coordinator executions.
    pub local_peak_memory_bytes: u64,
    pub serial_runs: u64,
    pub parallel_runs: u64,
    pub maximum_workers: u64,
    pub worker_metric_reports: u64,
    pub expected_worker_metric_reports: u64,
    pub maximum_worker_fragment_peak_memory_bytes: u64,
    pub maximum_worker_participant_peak_memory_bytes: u64,
}

/// Planned or observed execution modes across statement runs and rescans.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryExecutionMode {
    NotStarted,
    Serial,
    Parallel,
    Mixed,
}

impl ExecutionMetricsSnapshot {
    pub fn execution_mode(&self) -> QueryExecutionMode {
        match (self.serial_runs != 0, self.parallel_runs != 0) {
            (false, false) => QueryExecutionMode::NotStarted,
            (true, false) => QueryExecutionMode::Serial,
            (false, true) => QueryExecutionMode::Parallel,
            (true, true) => QueryExecutionMode::Mixed,
        }
    }
    pub fn scan(&self, scan: ScanId) -> ScanExecutionMetricsSnapshot {
        self.scans[scan.index()]
    }
}
