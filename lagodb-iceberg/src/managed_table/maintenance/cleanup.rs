use std::collections::HashSet;

use iceberg_lite::io::FileIO;
use pg_lakebase_core::object_cleanup::{
    ObjectCleanupContext, ObjectCleanupItemRef, ObjectCleanupQueue,
    object_cleanup_batch_items,
};
use pg_lakebase_core::table_maintenance::TableMaintenanceReport;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult, IcebergVacuumError};
use crate::storage::local_file_wal::record::{
    MAX_DELETE_FILES_PAYLOAD_BYTES, MAX_DELETE_FILES_PER_RECORD, delete_file_fits_wal,
};
use crate::storage::{
    LocalStorage, ObjectStorage, PostCommitDeletePurpose, PostCommitFileDeleteBatch,
};

use super::record_metric;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct VacuumCleanupRegistration {
    objects: u64,
    local_pending_paths: u64,
    local_wal_batches: u64,
    remote_queue_rows: u64,
}

impl VacuumCleanupRegistration {
    pub(crate) fn record(
        self,
        report: &mut TableMaintenanceReport,
    ) -> IcebergResult<()> {
        record_metric(report, c"local_pending_paths", self.local_pending_paths)?;
        record_metric(report, c"local_wal_batches", self.local_wal_batches)?;
        record_metric(report, c"remote_queue_rows", self.remote_queue_rows)?;
        debug_assert_eq!(
            self.objects,
            self.local_pending_paths + self.remote_queue_rows
        );
        Ok(())
    }
}

pub(crate) struct VacuumCleanup;

impl VacuumCleanup {
    fn local_wal_batch_count(paths: &[String]) -> IcebergResult<u64> {
        let mut batches = 0_u64;
        let mut batch_paths = 0_usize;
        let mut payload_bytes = 0_usize;
        for path in paths.iter().filter(|path| delete_file_fits_wal(path)) {
            let encoded = std::mem::size_of::<u32>()
                .checked_add(path.len())
                .ok_or_else(|| IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "DELETE_FILES WAL payload size overflow".to_owned(),
                    ),
                })?;
            if batch_paths > 0
                && (batch_paths >= MAX_DELETE_FILES_PER_RECORD
                    || payload_bytes
                        .checked_add(encoded)
                        .is_none_or(|value| value > MAX_DELETE_FILES_PAYLOAD_BYTES))
            {
                batches =
                    batches.checked_add(1).ok_or_else(|| IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "DELETE_FILES WAL batch count overflow".to_owned(),
                        ),
                    })?;
                batch_paths = 0;
                payload_bytes = 0;
            }
            batch_paths += 1;
            payload_bytes = payload_bytes.checked_add(encoded).ok_or_else(|| {
                IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "DELETE_FILES WAL payload size overflow".to_owned(),
                    ),
                }
            })?;
        }
        if batch_paths > 0 {
            batches = batches.checked_add(1).ok_or_else(|| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "DELETE_FILES WAL batch count overflow".to_owned(),
                ),
            })?;
        }
        Ok(batches)
    }

    pub(crate) fn register(
        relid: pg_sys::Oid,
        file_io: &FileIO,
        candidates: HashSet<String>,
    ) -> IcebergResult<VacuumCleanupRegistration> {
        if candidates.is_empty() {
            return Ok(VacuumCleanupRegistration::default());
        }
        let count =
            u64::try_from(candidates.len()).map_err(|_| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "cleanup candidate count does not fit u64".to_owned(),
                ),
            })?;
        if let Some(local) = file_io.storage().as_any().downcast_ref::<LocalStorage>()
        {
            let mut paths: Vec<String> = candidates.into_iter().collect();
            paths.sort_unstable();
            let local_wal_batches = if local.needs_wal() {
                Self::local_wal_batch_count(&paths)?
            } else {
                0
            };
            PostCommitFileDeleteBatch::register(
                file_io.clone(),
                paths,
                PostCommitDeletePurpose::Vacuum,
            );
            return Ok(VacuumCleanupRegistration {
                objects: count,
                local_pending_paths: count,
                local_wal_batches,
                remote_queue_rows: 0,
            });
        }
        if let Some(object) =
            file_io.storage().as_any().downcast_ref::<ObjectStorage>()
        {
            let batch_size = object_cleanup_batch_items();
            let mut targets = Vec::with_capacity(batch_size);
            for path in candidates {
                targets.push(object.maintenance_target_owned(path)?);
                if targets.len() < batch_size {
                    continue;
                }
                let items: Vec<ObjectCleanupItemRef<'_>> = targets
                    .iter()
                    .map(|target| ObjectCleanupItemRef::DeleteObject {
                        target,
                        context: ObjectCleanupContext {
                            producer: "lagodb_iceberg.vacuum",
                            source_relid: Some(relid),
                            source_name: None,
                        },
                    })
                    .collect();
                ObjectCleanupQueue::enqueue_batch(&items)?;
                targets.clear();
            }
            if !targets.is_empty() {
                let items: Vec<ObjectCleanupItemRef<'_>> = targets
                    .iter()
                    .map(|target| ObjectCleanupItemRef::DeleteObject {
                        target,
                        context: ObjectCleanupContext {
                            producer: "lagodb_iceberg.vacuum",
                            source_relid: Some(relid),
                            source_name: None,
                        },
                    })
                    .collect();
                ObjectCleanupQueue::enqueue_batch(&items)?;
            }
            return Ok(VacuumCleanupRegistration {
                objects: count,
                local_pending_paths: 0,
                local_wal_batches: 0,
                remote_queue_rows: count,
            });
        }
        Err(IcebergError::InvariantViolated(
            "VACUUM FileIO uses an unknown storage implementation",
        ))
    }
}
