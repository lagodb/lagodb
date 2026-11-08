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
use lagodb_core::diag;
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};
use crate::managed_table::catalog::bridge::{IcebergTableId, StagedCatalog};
use crate::managed_table::catalog::error::MetadataCatalogError;
use crate::managed_table::catalog::metadata_table::{
    CasUpdate, IcebergMetadata, MaintenanceScheduleUpdate,
};
use crate::managed_table::gucs;
use crate::managed_table::maintenance::{
    PreparedVacuum, VacuumAttemptResult, VacuumCommitAttempt,
};
use crate::storage::transaction_resources::{
    MetadataMaterializationAttempt, register_canceled_files_for_commit,
};
use crate::write::TxTableCommitPlan;

use super::{TableCommitInput, TxMetadata};

/// Catalog-accepted result of one materialization attempt.
struct PublishedAttempt<'a> {
    maintenance_deadline_advanced: bool,
    vacuum: Option<VacuumAttemptResult<'a>>,
}

struct TableCommitCoordinator<'a> {
    relid: pg_sys::Oid,
    plan: TxTableCommitPlan<'a, PreparedVacuum, String>,
    file_io: FileIO,
    /// Schedule published by a non-VACUUM plan; VACUUM derives its own per
    /// attempt from the catalog location it rebased on.
    data_schedule: MaintenanceScheduleUpdate,
    commit_started: Instant,
}

impl<'a> TableCommitCoordinator<'a> {
    fn new(
        relid: pg_sys::Oid,
        plan: TxTableCommitPlan<'a, PreparedVacuum, String>,
        file_io: FileIO,
    ) -> Self {
        let data_schedule = if plan.has_data_change() {
            let naptime = gucs::auto_maintenance_naptime();
            let micros = i64::try_from(naptime.as_micros()).unwrap_or(i64::MAX);
            let due_at = unsafe { pg_sys::GetCurrentTransactionStartTimestamp() }
                .saturating_add(micros);
            MaintenanceScheduleUpdate::ScheduleNoLaterThan(due_at)
        } else {
            MaintenanceScheduleUpdate::Preserve
        };
        Self {
            relid,
            plan,
            file_io,
            data_schedule,
            commit_started: Instant::now(),
        }
    }

    fn commit(self) -> IcebergResult<bool> {
        let max_retries = gucs::max_commit_retries();
        let max_retry_count = u32::try_from(max_retries)
            .expect("max_commit_retries is constrained to non-negative values");
        for retry_count in 0..=max_retry_count {
            if let Some(published) = self.attempt(retry_count)? {
                return self.complete(published);
            }
            diag::report_notice("Concurrent Iceberg update detected, rebasing...");
        }
        Err(IcebergError::MetadataCommitConflict {
            relid: self.relid,
            max_retries,
        })
    }

    /// Rebase on the current catalog location, materialize the plan, and
    /// publish it. `None` means a concurrent catalog update rejected the
    /// attempt and it must be rebuilt from scratch.
    fn attempt(
        &self,
        retry_count: u32,
    ) -> IcebergResult<Option<PublishedAttempt<'a>>> {
        let latest_location = IcebergMetadata::get(self.relid)?
            .metadata_location
            .ok_or(IcebergError::MetadataLocationNull)?;
        let schedule = match self.plan.exclusive_action {
            Some(vacuum) => vacuum
                .completion_token
                .completion_schedule(&latest_location),
            None => self.data_schedule,
        };
        if self
            .plan
            .truncate_guard
            .is_some_and(|expected| expected.as_str() != latest_location.as_str())
        {
            return Err(IcebergError::TruncateCommitConflict { relid: self.relid });
        }
        let base_table = self.base_table(&latest_location)?;

        let catalog = StagedCatalog::new(&base_table);
        // Manifest, manifest-list and metadata files can be created while
        // transaction actions are applied, before `tx.commit()` writes the
        // final metadata JSON. Scope the whole materialization so every
        // failed CAS attempt is discarded as one unit.
        let metadata_attempt = MetadataMaterializationAttempt::begin()?;
        let mut tx = Transaction::new(&base_table);
        let vacuum_attempt = self.plan.exclusive_action.map(|vacuum| {
            VacuumCommitAttempt::new(
                vacuum,
                &base_table,
                &latest_location,
                retry_count,
            )
        });
        if let Some(attempt) = &vacuum_attempt {
            tx = attempt.apply_actions(tx)?;
        }
        let updated_table = self
            .plan
            .apply_to_transaction(tx, base_table.metadata())?
            .commit(&catalog)?;
        let new_location = updated_table
            .metadata_location()
            .ok_or(IcebergError::MetadataLocationNull)?;
        let vacuum = vacuum_attempt
            .map(|attempt| attempt.finish(&updated_table))
            .transpose()?;

        let accepted = if new_location == latest_location {
            metadata_attempt.discard()?;
            self.publish_unchanged(&latest_location, schedule)?
        } else {
            self.publish_metadata(
                metadata_attempt,
                &latest_location,
                new_location,
                schedule,
            )?
        };
        Ok(
            accepted.map(|maintenance_deadline_advanced| PublishedAttempt {
                maintenance_deadline_advanced,
                vacuum,
            }),
        )
    }

    fn base_table(&self, metadata_location: &str) -> IcebergResult<Table> {
        let metadata = TableMetadata::read_from(&self.file_io, metadata_location)?;
        Ok(Table::builder()
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(IcebergTableId::for_relation(self.relid).into_table_ident())
            .file_io(self.file_io.clone())
            .build()?)
    }

    /// An attempt that produced no new metadata publishes no location. Only
    /// VACUUM still completes its maintenance deadline against the location
    /// it was assessed on. Returns `None` on a catalog conflict.
    fn publish_unchanged(
        &self,
        latest_location: &str,
        schedule: MaintenanceScheduleUpdate,
    ) -> IcebergResult<Option<bool>> {
        if self.plan.exclusive_action.is_none() {
            return Ok(Some(false));
        }
        match IcebergMetadata::finish_maintenance(
            self.relid,
            latest_location,
            schedule,
        ) {
            Ok(()) => Ok(Some(false)),
            Err(IcebergError::MetadataCatalog(MetadataCatalogError::Conflict)) => {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// CAS the new metadata location and resolve the materialization attempt
    /// with the catalog outcome. Returns whether the maintenance deadline
    /// advanced, or `None` on a catalog conflict.
    fn publish_metadata(
        &self,
        metadata_attempt: MetadataMaterializationAttempt,
        latest_location: &str,
        new_location: &str,
        schedule: MaintenanceScheduleUpdate,
    ) -> IcebergResult<Option<bool>> {
        match IcebergMetadata::cas_update(
            self.relid,
            Some(latest_location),
            CasUpdate {
                metadata_location: Some(new_location),
                previous_metadata_location: Some(latest_location),
                maintenance_schedule: schedule,
            },
        ) {
            Ok(maintenance_deadline_advanced) => {
                metadata_attempt.promote()?;
                Ok(Some(maintenance_deadline_advanced))
            }
            Err(IcebergError::MetadataCatalog(MetadataCatalogError::Conflict)) => {
                metadata_attempt.discard()?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Post-publication work for the accepted attempt.
    fn complete(self, published: PublishedAttempt<'a>) -> IcebergResult<bool> {
        if let Some(vacuum) = published.vacuum {
            vacuum.complete(self.relid, &self.file_io, self.commit_started)?;
        }
        register_canceled_files_for_commit(
            self.file_io,
            self.plan.canceled_created_paths,
        );
        Ok(published.maintenance_deadline_advanced)
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
