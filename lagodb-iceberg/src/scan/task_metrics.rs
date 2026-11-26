//! Physical task inventory facts shared by PostgreSQL and query-offload scans.

use std::collections::HashSet;

use iceberg_lite::scan::FileScanTask;
use lagodb_core::runtime_api::TableScanTaskMetrics;

pub(crate) trait IcebergTaskMetrics {
    fn task_metrics(&self, total_data_files: Option<u64>) -> TableScanTaskMetrics;
}

impl IcebergTaskMetrics for [FileScanTask] {
    fn task_metrics(&self, total_data_files: Option<u64>) -> TableScanTaskMetrics {
        let planned_files = self
            .iter()
            .map(FileScanTask::data_file_path)
            .collect::<HashSet<_>>()
            .len() as u64;
        let planned_bytes = self
            .iter()
            .fold(0_u64, |bytes, task| bytes.saturating_add(task.length));
        let pruned_files =
            total_data_files.and_then(|total| total.checked_sub(planned_files));
        TableScanTaskMetrics::new(self.len() as u64, planned_files, planned_bytes)
            .with_pruned_files(pruned_files)
    }
}
