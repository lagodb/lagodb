//! Register deletion of files selected by Iceberg maintenance.
//!
//! Snapshot expiration follows Iceberg's table and branch/tag retention
//! policies. The reachability planner excludes files still referenced by
//! retained snapshots; metadata-log retention and age-gated orphan cleanup
//! have their own candidate rules.
//! See <https://iceberg.apache.org/spec/#snapshot-retention-policy>.
//!
//! Iceberg retention does not require a PostgreSQL-style active-reader or xmin
//! deletion barrier. LagoDB deliberately follows that contract: a PG snapshot
//! selects visible managed-table metadata, and an IcebergReadSnapshot keeps a
//! scan's read view stable, but neither pins its files against expiration.
//! Retention must therefore cover the intended query lifetime; a query that
//! outlives retention has no guarantee that its expired files remain available.
//!
//! Local deletion waits for commit, and remote deletion is queued
//! transactionally, so rollback does not delete these candidates. This protects
//! publication of the maintenance result, not completion of active readers;
//! do not interpret the absence of reader tracking here as missing PG MVCC GC.

use std::collections::HashSet;

use iceberg_lite::io::FileIO;
use lagodb_core::object_cleanup::{
    ObjectCleanupContext, ObjectCleanupItemRef, ObjectCleanupQueue,
    object_cleanup_batch_items,
};
use lagodb_core::table_maintenance::TableMaintenanceReport;
use pgrx::pg_sys;

use crate::managed_table::storage::ManagedFileIoBackend;
use crate::managed_table::{ManagedResult, ManagedTableError};
use crate::storage::PostCommitFileDeleteBatch;

use super::error::IcebergVacuumError;
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
    ) -> ManagedResult<()> {
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
    pub(crate) fn register(
        relid: pg_sys::Oid,
        file_io: &FileIO,
        candidates: HashSet<String>,
    ) -> ManagedResult<VacuumCleanupRegistration> {
        if candidates.is_empty() {
            return Ok(VacuumCleanupRegistration::default());
        }
        let count = u64::try_from(candidates.len()).map_err(|_| {
            ManagedTableError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "cleanup candidate count does not fit u64".to_owned(),
                ),
            }
        })?;
        match ManagedFileIoBackend::from_file_io(file_io)? {
            ManagedFileIoBackend::Local(_) => {
                let mut paths: Vec<String> = candidates.into_iter().collect();
                paths.sort_unstable();
                let local_wal_batches =
                    PostCommitFileDeleteBatch::register_vacuum_files(
                        file_io.clone(),
                        paths,
                    );
                Ok(VacuumCleanupRegistration {
                    objects: count,
                    local_pending_paths: count,
                    local_wal_batches,
                    remote_queue_rows: 0,
                })
            }
            ManagedFileIoBackend::Object(object) => {
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
                Ok(VacuumCleanupRegistration {
                    objects: count,
                    local_pending_paths: 0,
                    local_wal_batches: 0,
                    remote_queue_rows: count,
                })
            }
        }
    }
}
