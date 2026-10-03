//! Mutation scan tasks retained for row-delete finalization.

use std::collections::HashMap;
use std::{iter, sync::Arc};

use iceberg_lite::scan::FileScanTask;
use iceberg_lite::spec::{PartitionKey, Struct};

use crate::error::{IcebergError, IcebergResult};

/// Planned mutation tasks for one concrete predicate and projection.
///
/// The shared task slice is consumed by the reader and retained by
/// `IcebergModifyScanContext`. The path index is used later when v2 position
/// deletes and v3 deletion vectors need the original task metadata for one
/// referenced data file.
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

    /// Build the source data file's partition identity for a v2 position
    /// delete file. `None` represents the original zero-field spec.
    ///
    /// iceberg-lite supplies the current table schema here. A historical
    /// identity/truncate Int literal is paired with Unknown if its source was
    /// removed, or Long after int -> long promotion. Both pairs reach
    /// `Datum::fmt`'s unreachable arm while formatting a partition path.
    /// Upstream iceberg-rust carries the snapshot schema in scan tasks rather
    /// than the manifest's schema; a snapshot using the evolved schema exposes
    /// the same `PartitionKey`/`partition_to_path` defect with old manifests.
    /// iceberg-lite exposes it after schema-only evolution too. Issues #2842
    /// and #2844 fixed related read-side failures only; #2530 tracks binding a
    /// partition spec to its schema. The durable fix is a manifest-bound result type
    /// in the scan task, not a second type resolver in LagoDB. Wait for that
    /// upstream fix, then merge it into iceberg-lite and remove this limitation.
    ///
    /// https://github.com/apache/iceberg-rust/issues/2530
    /// https://github.com/apache/iceberg-rust/issues/2842
    /// https://github.com/apache/iceberg-rust/issues/2844
    pub(crate) fn partition_key_for_path(
        &self,
        path: &str,
    ) -> IcebergResult<Option<PartitionKey>> {
        let task_index = self
            .tasks_by_path
            .get(path)
            .and_then(|indices| indices.first())
            .ok_or_else(|| {
                IcebergError::MetadataTracker(format!(
                    "cannot find Iceberg scan task metadata for deletion target {path}"
                ))
            })?;
        let task =
            self.tasks
                .get(*task_index)
                .ok_or(IcebergError::InvariantViolated(
                    "mutation task path index is inconsistent",
                ))?;
        let Some(spec) = task.partition_spec.as_ref() else {
            return Ok(None);
        };
        let data = task.partition.clone().unwrap_or_else(|| {
            Struct::from_iter(iter::repeat_n(None, spec.fields().len()))
        });
        Ok(Some(PartitionKey::new(
            spec.as_ref().clone(),
            Arc::clone(&task.schema),
            data,
        )))
    }
}
