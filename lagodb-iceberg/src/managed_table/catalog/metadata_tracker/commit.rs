//! Per-table optimistic catalog commit and VACUUM attempt materialization.
//!
//! A coordinator owns one immutable transaction action plan. Every loop
//! iteration rebuilds its base table and all metadata/manifest state, so no
//! attempt-scoped cache can survive a failed catalog CAS.

use std::time::Instant;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::TableMetadata;
use iceberg_lite::table::Table;
use iceberg_lite::transaction::Transaction;
use pg_lakebase_core::diag;
use pgrx::pg_sys;

use crate::engine::write::TxTableCommitPlan;
use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::catalog::bridge::{IcebergTableId, StagedCatalog};
use crate::managed_table::catalog::metadata_table::{
    CasUpdate, IcebergMetadata, MaintenanceScheduleUpdate,
};
use crate::managed_table::gucs;
use crate::managed_table::maintenance::{
    PreparedVacuum, VacuumAttemptOutcome, VacuumAttemptResult, VacuumCommitAttempt,
};
use crate::storage::transaction_resources::{
    MetadataMaterializationAttempt, register_canceled_files_for_commit,
};

use super::{TableCommitInput, TxMetadata};

struct TableCommitCoordinator<'a> {
    relid: pg_sys::Oid,
    plan: TxTableCommitPlan<'a, PreparedVacuum, String>,
    file_io: FileIO,
}

impl<'a> TableCommitCoordinator<'a> {
    fn new(
        relid: pg_sys::Oid,
        plan: TxTableCommitPlan<'a, PreparedVacuum, String>,
        file_io: FileIO,
    ) -> Self {
        Self {
            relid,
            plan,
            file_io,
        }
    }

    fn commit(self) -> IcebergResult<bool> {
        let Self {
            relid,
            plan,
            file_io,
        } = self;
        let has_maintenance_action = plan.has_data_change();
        let mut retries = 0_u32;
        let max_retries = gucs::max_commit_retries();
        let max_retry_count = u32::try_from(max_retries)
            .expect("max_commit_retries is constrained to non-negative values");
        let vacuum_commit_started = plan.exclusive_action.map(|_| Instant::now());
        let write_maintenance_schedule = if has_maintenance_action {
            let naptime = gucs::auto_maintenance_naptime();
            let micros = i64::try_from(naptime.as_micros()).unwrap_or(i64::MAX);
            let due_at = unsafe { pg_sys::GetCurrentTransactionStartTimestamp() }
                .saturating_add(micros);
            MaintenanceScheduleUpdate::ScheduleNoLaterThan(due_at)
        } else {
            MaintenanceScheduleUpdate::Preserve
        };

        let outcome = loop {
            if retries > max_retry_count {
                return Err(IcebergError::MetadataCommitConflict {
                    relid,
                    max_retries,
                });
            }
            retries += 1;

            let latest_global_location = IcebergMetadata::get(relid)?
                .metadata_location
                .ok_or(IcebergError::MetadataLocationNull)?;
            let maintenance_schedule = match plan.exclusive_action {
                Some(vacuum)
                    if vacuum.completion_token.metadata_location.as_str()
                        == latest_global_location.as_str() =>
                {
                    MaintenanceScheduleUpdate::CompleteIfDueMatches(
                        vacuum.completion_token.maintenance_due_at,
                    )
                }
                Some(_) => MaintenanceScheduleUpdate::Preserve,
                None => write_maintenance_schedule,
            };
            if plan.truncate_guard.is_some_and(|expected| {
                expected.as_str() != latest_global_location.as_str()
            }) {
                return Err(IcebergError::TruncateCommitConflict { relid });
            }
            let metadata =
                TableMetadata::read_from(&file_io, &latest_global_location)?;
            let base_table = Table::builder()
                .metadata_location(latest_global_location.clone())
                .metadata(metadata)
                .identifier(IcebergTableId::for_relation(relid).into_table_ident())
                .file_io(file_io.clone())
                .build()?;

            let catalog = StagedCatalog::new(&base_table);
            // Manifest, manifest-list and metadata files can be created while
            // transaction actions are applied, before `tx.commit()` writes the
            // final metadata JSON. Scope the whole materialization so every
            // failed CAS attempt is discarded as one unit.
            let metadata_attempt = MetadataMaterializationAttempt::begin()?;
            let mut tx = Transaction::new(&base_table);
            let vacuum_attempt = plan.exclusive_action.map(|vacuum| {
                VacuumCommitAttempt::new(
                    vacuum,
                    &base_table,
                    &latest_global_location,
                    retries.saturating_sub(1),
                )
            });
            if let Some(attempt) = &vacuum_attempt {
                tx = attempt.apply_actions(tx)?;
            }
            tx = plan.apply_to_transaction(tx, base_table.metadata())?;
            let updated_table = tx.commit(&catalog)?;
            let new_metadata_location = updated_table
                .metadata_location()
                .ok_or(IcebergError::MetadataLocationNull)?;
            let vacuum_outcome = vacuum_attempt
                .map(|attempt| attempt.finish(&updated_table))
                .transpose()?;
            if let Some(outcome) = &vacuum_outcome {
                debug_assert_eq!(
                    outcome.changes_metadata(),
                    new_metadata_location != latest_global_location
                );
            }
            let mut vacuum_result =
                vacuum_outcome.map(VacuumAttemptOutcome::into_result);
            if new_metadata_location == latest_global_location {
                metadata_attempt.discard()?;
                if plan.exclusive_action.is_some() {
                    match IcebergMetadata::finish_maintenance(
                        relid,
                        &latest_global_location,
                        maintenance_schedule,
                    ) {
                        Ok(()) => {
                            if vacuum_result
                                .as_ref()
                                .is_some_and(VacuumAttemptResult::has_cleanup)
                            {
                                vacuum_result
                                    .as_mut()
                                    .expect("checked VACUUM result")
                                    .register_cleanup(relid, &file_io)?;
                            }
                        }
                        Err(IcebergError::MetadataCatalogConflict) => {
                            diag::report_notice(
                                "Concurrent Iceberg update detected, rebasing...",
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if let (Some(vacuum), Some(result)) =
                    (plan.exclusive_action, vacuum_result.take())
                {
                    result.report_success(vacuum, vacuum_commit_started.as_ref())?;
                }
                register_canceled_files_for_commit(
                    file_io.clone(),
                    plan.canceled_created_paths.clone(),
                );
                break false;
            }

            match IcebergMetadata::cas_update(
                relid,
                Some(&latest_global_location),
                CasUpdate {
                    metadata_location: Some(new_metadata_location),
                    previous_metadata_location: Some(&latest_global_location),
                    maintenance_schedule,
                },
            ) {
                Ok(maintenance_deadline_advanced) => {
                    metadata_attempt.promote()?;
                    if let Some(result) = &mut vacuum_result {
                        result.register_cleanup(relid, &file_io)?;
                    }
                    if let (Some(vacuum), Some(result)) =
                        (plan.exclusive_action, vacuum_result.take())
                    {
                        result
                            .report_success(vacuum, vacuum_commit_started.as_ref())?;
                    }
                    register_canceled_files_for_commit(
                        file_io.clone(),
                        plan.canceled_created_paths.clone(),
                    );
                    break maintenance_deadline_advanced;
                }
                Err(IcebergError::MetadataCatalogConflict) => {
                    metadata_attempt.discard()?;
                    diag::report_notice(
                        "Concurrent Iceberg update detected, rebasing...",
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        };

        Ok(outcome)
    }
}

impl TxMetadata {
    /// Detach one immutable action snapshot at a time, then hand all I/O,
    /// optimistic retry, artifact and cleanup work to the table coordinator.
    pub(super) fn commit_all(&self) -> IcebergResult<bool> {
        let mut table_oids: Vec<pg_sys::Oid> =
            self.inner.borrow().tables.keys().copied().collect();
        table_oids.sort_unstable_by_key(|oid| u32::from(*oid));

        let mut automatic_maintenance_wakeup = false;
        for relid in table_oids {
            let Some(TableCommitInput { actions, file_io }) =
                self.commit_input(relid)?
            else {
                continue;
            };
            automatic_maintenance_wakeup |=
                TableCommitCoordinator::new(relid, actions.commit_plan()?, file_io)
                    .commit()?;
        }
        Ok(automatic_maintenance_wakeup)
    }
}
