//! Statement-cumulative S1M metrics collected at batch and scalar boundaries.

use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;

#[derive(Debug, Default)]
pub(super) struct ExecutionMetrics {
    input_batches: AtomicU64,
    input_rows: AtomicU64,
    arrow_batch_bytes: AtomicU64,
    output_rows: AtomicU64,
}

impl ExecutionMetrics {
    pub(super) fn record_input(&self, batch: &RecordBatch) {
        self.input_batches.fetch_add(1, Ordering::Relaxed);
        self.input_rows
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.arrow_batch_bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Relaxed);
    }

    pub(super) fn record_output_row(&self) {
        self.output_rows.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(
        &self,
        engine_peak_memory_bytes: usize,
    ) -> ExecutionMetricsSnapshot {
        ExecutionMetricsSnapshot {
            input_batches: self.input_batches.load(Ordering::Relaxed),
            input_rows: self.input_rows.load(Ordering::Relaxed),
            arrow_batch_bytes: self.arrow_batch_bytes.load(Ordering::Relaxed),
            output_rows: self.output_rows.load(Ordering::Relaxed),
            engine_peak_memory_bytes: engine_peak_memory_bytes as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionMetricsSnapshot {
    pub input_batches: u64,
    pub input_rows: u64,
    pub arrow_batch_bytes: u64,
    pub output_rows: u64,
    pub engine_peak_memory_bytes: u64,
}
