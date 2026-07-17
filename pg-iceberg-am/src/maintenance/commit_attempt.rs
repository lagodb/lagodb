//! One attempt to materialize and assess an Iceberg VACUUM commit.
//!
//! The value is deliberately rebuilt after every catalog CAS conflict. It owns
//! VACUUM action materialization, attempt-local reporting, reachability, and
//! the typed distinction between metadata-changing and no-metadata outcomes.

use std::collections::HashSet;
use std::time::Instant;

use iceberg_lite::io::FileIO;
use iceberg_lite::table::Table;
use iceberg_lite::transaction::{ApplyTransactionAction, Transaction};
use pg_lakebase_core::table_maintenance::TableMaintenanceReport;

use crate::error::{IcebergError, IcebergResult, IcebergVacuumError};

use super::{
    IcebergReachabilityPlanner, PreparedVacuum, ReachabilityDeletionCandidates,
    VacuumCleanup, record_metric,
};

pub(crate) struct VacuumAttemptResult {
    pub(crate) report: TableMaintenanceReport,
    pub(crate) cleanup_candidates: HashSet<String>,
}

impl VacuumAttemptResult {
    pub(crate) fn has_cleanup(&self) -> bool {
        !self.cleanup_candidates.is_empty()
    }

    pub(crate) fn register_cleanup(
        &mut self,
        relid: pgrx::pg_sys::Oid,
        file_io: &FileIO,
    ) -> IcebergResult<()> {
        self.report.objects_scheduled_for_deletion =
            u64::try_from(self.cleanup_candidates.len()).map_err(|_| {
                IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "cleanup candidate count does not fit u64".to_owned(),
                    ),
                }
            })?;
        let registration = VacuumCleanup::register(
            relid,
            file_io,
            std::mem::take(&mut self.cleanup_candidates),
        )?;
        registration.record(&mut self.report)
    }

    pub(crate) fn report_success(
        mut self,
        vacuum: &PreparedVacuum,
        commit_started: Option<&Instant>,
    ) -> IcebergResult<()> {
        if let Some(started) = commit_started {
            record_metric(
                &mut self.report,
                c"commit_ms",
                u64::try_from(started.elapsed().as_millis()).map_err(|_| {
                    IcebergError::Vacuum {
                        source: IcebergVacuumError::ResourceLimit(
                            "VACUUM commit duration does not fit u64 milliseconds"
                                .to_owned(),
                        ),
                    }
                })?,
            )?;
        }
        vacuum.report_success(&self.report);
        Ok(())
    }
}

pub(crate) enum VacuumAttemptOutcome {
    NoMetadataChange(VacuumAttemptResult),
    MetadataUpdate(VacuumAttemptResult),
}

impl VacuumAttemptOutcome {
    pub(crate) fn into_result(self) -> VacuumAttemptResult {
        match self {
            Self::NoMetadataChange(result) | Self::MetadataUpdate(result) => result,
        }
    }

    pub(crate) fn changes_metadata(&self) -> bool {
        matches!(self, Self::MetadataUpdate(_))
    }
}

pub(crate) struct VacuumCommitAttempt<'a> {
    vacuum: &'a PreparedVacuum,
    base_table: &'a Table,
    latest_metadata_location: &'a str,
    retry_count: u32,
}

impl<'a> VacuumCommitAttempt<'a> {
    pub(crate) fn new(
        vacuum: &'a PreparedVacuum,
        base_table: &'a Table,
        latest_metadata_location: &'a str,
        retry_count: u32,
    ) -> Self {
        Self {
            vacuum,
            base_table,
            latest_metadata_location,
            retry_count,
        }
    }

    pub(crate) fn apply_actions(
        &self,
        mut transaction: Transaction,
    ) -> IcebergResult<Transaction> {
        self.vacuum
            .owned_table_root
            .ensure_table_location(self.base_table.metadata().location())?;
        self.vacuum
            .owned_table_root
            .ensure_path(self.latest_metadata_location)?;

        if let Some(rewrite) = &self.vacuum.rewrite {
            transaction = transaction
                .rewrite_files(
                    rewrite.starting_snapshot_id,
                    rewrite.starting_sequence_number,
                )
                .rewrite_data_files(
                    rewrite.input_files.clone(),
                    rewrite.output_files.clone(),
                )
                .rewrite_delete_file_identities(
                    rewrite.materialized_delete_identities.clone(),
                )
                .apply(transaction)?;
        }
        if let Some(manifests) = self.vacuum.manifest_rewrite {
            transaction = transaction
                .rewrite_manifests(
                    manifests.min_count_to_merge,
                    manifests.target_size_bytes,
                )
                .apply(transaction)?;
        }
        transaction = transaction
            .expire_snapshots()
            .as_of_ms(self.vacuum.expiration.as_of_ms)
            .apply(transaction)?;
        Ok(transaction)
    }

    pub(crate) fn finish(
        &self,
        updated_table: &Table,
    ) -> IcebergResult<VacuumAttemptOutcome> {
        let new_metadata_location = updated_table
            .metadata_location()
            .ok_or(IcebergError::MetadataLocationNull)?;
        self.vacuum
            .owned_table_root
            .ensure_table_location(updated_table.metadata().location())?;
        self.vacuum
            .owned_table_root
            .ensure_path(new_metadata_location)?;

        let mut report = self.vacuum.report.clone();
        self.record_snapshot_report(updated_table, &mut report)?;
        report.cas_retries = u64::from(self.retry_count);
        record_metric(&mut report, c"validation_conflicts", report.cas_retries)?;

        let cleanup_started = Instant::now();
        let orphan_cutoff = self
            .vacuum
            .orphan_cleanup
            .map(|orphan| orphan.older_than_ms);
        let (expiration_candidates, orphan_candidates) = if new_metadata_location
            != self.latest_metadata_location
            || orphan_cutoff.is_some()
        {
            IcebergReachabilityPlanner::default().cleanup_candidates(
                self.base_table,
                updated_table,
                orphan_cutoff,
                &self.vacuum.owned_table_root,
            )?
        } else {
            (ReachabilityDeletionCandidates::default(), HashSet::new())
        };
        self.record_cleanup_report(
            &mut report,
            &expiration_candidates,
            orphan_cutoff.map(|_| orphan_candidates.len()),
            cleanup_started,
        )?;
        let mut cleanup_candidates = expiration_candidates.paths;
        cleanup_candidates.extend(orphan_candidates);

        let result = VacuumAttemptResult {
            report,
            cleanup_candidates,
        };
        if new_metadata_location == self.latest_metadata_location {
            Ok(VacuumAttemptOutcome::NoMetadataChange(result))
        } else {
            Ok(VacuumAttemptOutcome::MetadataUpdate(result))
        }
    }

    fn record_snapshot_report(
        &self,
        updated_table: &Table,
        report: &mut TableMaintenanceReport,
    ) -> IcebergResult<()> {
        let base_snapshot_ids: HashSet<i64> = self
            .base_table
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect();
        let expired_snapshot_count = self
            .base_table
            .metadata()
            .snapshots()
            .filter(|snapshot| {
                updated_table
                    .metadata()
                    .snapshot_by_id(snapshot.snapshot_id())
                    .is_none()
            })
            .count();
        report.snapshots_expired =
            u64::try_from(expired_snapshot_count).map_err(|_| {
                IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "expired snapshot count does not fit u64".to_owned(),
                    ),
                }
            })?;
        let expired_refs = self
            .base_table
            .metadata()
            .snapshot_references()
            .filter(|(name, _)| {
                updated_table.metadata().snapshot_reference(name).is_none()
            })
            .count();
        record_metric(
            report,
            c"expired_refs",
            u64::try_from(expired_refs).map_err(|_| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(
                    "expired reference count does not fit u64".to_owned(),
                ),
            })?,
        )?;

        let created_snapshot_count = updated_table
            .metadata()
            .snapshots()
            .filter(|snapshot| !base_snapshot_ids.contains(&snapshot.snapshot_id()))
            .count();
        let rewrite_snapshot_count = usize::from(self.vacuum.rewrite.is_some());
        if self.vacuum.manifest_rewrite.is_some()
            && created_snapshot_count > rewrite_snapshot_count
        {
            let current = updated_table.metadata().current_snapshot().ok_or(
                IcebergError::InvariantViolated(
                    "manifest rewrite committed without a current snapshot",
                ),
            )?;
            let manifests = current.load_manifest_list(
                updated_table.file_io(),
                &updated_table.metadata_ref(),
            )?;
            let rewritten = manifests
                .entries()
                .iter()
                .filter(|manifest| {
                    manifest.added_snapshot_id == current.snapshot_id()
                })
                .count();
            report.manifests_rewritten =
                u64::try_from(rewritten).map_err(|_| IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "rewritten manifest count does not fit u64".to_owned(),
                    ),
                })?;
        }
        Ok(())
    }

    fn record_cleanup_report(
        &self,
        report: &mut TableMaintenanceReport,
        expiration: &ReachabilityDeletionCandidates,
        orphan_count: Option<usize>,
        started: Instant,
    ) -> IcebergResult<()> {
        record_metric(
            report,
            c"expiration_unreachable_objects",
            u64::try_from(expiration.paths.len()).map_err(|_| {
                IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "expiration cleanup count does not fit u64".to_owned(),
                    ),
                }
            })?,
        )?;
        for (name, value) in [
            (c"unreachable_data_files", expiration.data),
            (c"unreachable_delete_files", expiration.delete),
            (c"unreachable_manifests", expiration.manifest),
            (c"unreachable_manifest_lists", expiration.manifest_list),
            (c"unreachable_statistics", expiration.statistics),
            (c"unreachable_metadata_files", expiration.metadata),
        ] {
            record_metric(report, name, value)?;
        }
        if let Some(orphan_count) = orphan_count {
            record_metric(
                report,
                c"orphan_candidates",
                u64::try_from(orphan_count).map_err(|_| IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "orphan candidate count does not fit u64".to_owned(),
                    ),
                })?,
            )?;
        }
        record_metric(
            report,
            c"cleanup_planning_ms",
            u64::try_from(started.elapsed().as_millis()).map_err(|_| {
                IcebergError::Vacuum {
                    source: IcebergVacuumError::ResourceLimit(
                        "cleanup planning duration does not fit u64 milliseconds"
                            .to_owned(),
                    ),
                }
            })?,
        )
    }
}
