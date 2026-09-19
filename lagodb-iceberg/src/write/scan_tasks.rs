//! Mutation scan tasks retained for row-delete finalization.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::scan::FileScanTask;

use crate::error::{IcebergError, IcebergResult};

/// Planned mutation tasks for one concrete predicate and projection.
///
/// The shared task slice is consumed by the reader and retained by
/// `IcebergModifyScanContext`. The path index is used later when v3 deletion
/// vectors need the original task metadata for one referenced data file.
#[derive(Debug)]
pub(crate) struct PlannedMutationTasks {
    tasks: Arc<[FileScanTask]>,
    tasks_by_path: HashMap<Box<str>, Vec<usize>>,
}

impl PlannedMutationTasks {
    pub(crate) fn new(tasks: Vec<FileScanTask>) -> Self {
        let mut tasks_by_path: HashMap<Box<str>, Vec<usize>> = HashMap::new();
        for (task_index, task) in tasks.iter().enumerate() {
            tasks_by_path
                .entry(Box::<str>::from(task.data_file_path.as_str()))
                .or_default()
                .push(task_index);
        }
        Self {
            tasks: Arc::from(tasks.into_boxed_slice()),
            tasks_by_path,
        }
    }

    pub(crate) fn shared_tasks(&self) -> Arc<[FileScanTask]> {
        Arc::clone(&self.tasks)
    }

    pub(crate) fn tasks_for_path(
        &self,
        path: &str,
    ) -> IcebergResult<Vec<&FileScanTask>> {
        let task_indices = self.tasks_by_path.get(path).ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "cannot find Iceberg scan task metadata for deletion target {path}"
            ))
        })?;
        let mut tasks = Vec::with_capacity(task_indices.len());
        for task_index in task_indices {
            let task = self.tasks.get(*task_index).ok_or(
                IcebergError::InvariantViolated(
                    "mutation task path index is inconsistent",
                ),
            )?;
            tasks.push(task);
        }
        Ok(tasks)
    }
}
