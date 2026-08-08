//! Bounded opportunistic Iceberg maintenance scheduled by `pg_lakebase_runtime`.

use std::time::Duration;

use pg_lakebase_core::diag::{PgReportError, report_warning};
use pg_lakebase_core::extension_worker::{
    WorkerContext, WorkerSchedule, WorkerTransaction,
};
use pg_lakebase_core::handles::RelationGuard;
use pg_lakebase_core::table_maintenance::{
    LakebaseTableMaintenanceProvider, TableMaintenanceBudget,
    TableMaintenanceCommandTime, TableMaintenanceMode, TableMaintenanceOptions,
    TableMaintenanceRequest,
};
use pgrx::datum::Internal;
use pgrx::prelude::*;

use super::IcebergTableMaintenanceProvider;
use super::provider::MaintenanceExecution;
use crate::catalog::metadata_table::{IcebergMetadata, MaintenanceCandidate};
use crate::error::IcebergError;

#[derive(Clone, Copy)]
struct SchedulerPolicy {
    naptime: Duration,
    max_tables: usize,
}

impl SchedulerPolicy {
    fn configured() -> Self {
        Self {
            naptime: crate::gucs::auto_maintenance_naptime(),
            max_tables: crate::gucs::auto_maintenance_max_tables(),
        }
    }

    fn timestamp_after_now(self) -> pg_sys::TimestampTz {
        let micros = i64::try_from(self.naptime.as_micros()).unwrap_or(i64::MAX);
        unsafe { pg_sys::GetCurrentTimestamp() }.saturating_add(micros)
    }

    fn delay_until(timestamp: pg_sys::TimestampTz) -> Duration {
        let now = unsafe { pg_sys::GetCurrentTimestamp() };
        Duration::from_micros(
            u64::try_from(timestamp.saturating_sub(now)).unwrap_or_default(),
        )
        .max(Duration::from_millis(1))
    }
}

enum MaintenanceAttempt {
    Completed,
    LockSkipped,
    StaleCandidate,
}

fn maintain_relation(
    candidate: &MaintenanceCandidate,
) -> Result<MaintenanceAttempt, PgReportError> {
    let relid = candidate.relid;
    let metadata_location = candidate
        .metadata_location
        .as_deref()
        .ok_or(IcebergError::MetadataLocationNull)
        .map_err(PgReportError::from_domain_error)?;
    let locked = unsafe {
        pg_sys::ConditionalLockRelationOid(
            relid,
            pg_sys::ShareUpdateExclusiveLock as pg_sys::LOCKMODE,
        )
    };
    if !locked {
        return Ok(MaintenanceAttempt::LockSkipped);
    }
    let relation = RelationGuard::open(relid, pg_sys::NoLock as pg_sys::LOCKMODE)
        .map_err(PgReportError::from_domain_error)?;
    let relation = relation.as_handle();
    let expected_am =
        <IcebergTableMaintenanceProvider as LakebaseTableMaintenanceProvider>::access_method_oid();
    if expected_am != Some(relation.access_method_oid()) {
        IcebergMetadata::finish_maintenance(
            relid,
            metadata_location,
            crate::catalog::metadata_table::MaintenanceScheduleUpdate::CompleteIfDueMatches(
                Some(candidate.due_at),
            ),
        )
        .map_err(PgReportError::from_domain_error)?;
        return Ok(MaintenanceAttempt::Completed);
    }
    let command_time = TableMaintenanceCommandTime::now()
        .map_err(PgReportError::from_domain_error)?;
    match IcebergTableMaintenanceProvider::execute_scheduled(
        TableMaintenanceRequest {
            relation: &relation,
            mode: TableMaintenanceMode::Routine,
            options: TableMaintenanceOptions {
                skip_locked: true,
                process_main: true,
                ..TableMaintenanceOptions::default()
            },
            budget: TableMaintenanceBudget::configured(),
            command_time,
        },
        candidate,
    )
    .map_err(PgReportError::from_domain_error)?
    {
        MaintenanceExecution::Executed(_) => Ok(MaintenanceAttempt::Completed),
        MaintenanceExecution::StaleCandidate => {
            Ok(MaintenanceAttempt::StaleCandidate)
        }
    }
}

fn load_candidates(
    policy: SchedulerPolicy,
) -> Result<Vec<MaintenanceCandidate>, PgReportError> {
    let now = unsafe { pg_sys::GetCurrentTimestamp() };
    IcebergMetadata::maintenance_candidates(policy.max_tables.saturating_add(1), now)
        .map_err(PgReportError::from_domain_error)
}

fn defer_candidate(
    candidate: &MaintenanceCandidate,
    policy: SchedulerPolicy,
) -> Result<(), PgReportError> {
    IcebergMetadata::defer_maintenance(candidate, policy.timestamp_after_now())
        .map(|_| ())
        .map_err(PgReportError::from_domain_error)
}

fn next_worker_schedule() -> Result<WorkerSchedule, PgReportError> {
    let next = IcebergMetadata::next_maintenance_due_at()
        .map_err(PgReportError::from_domain_error)?;
    Ok(match next {
        None => WorkerSchedule::Idle,
        Some(timestamp) if timestamp <= unsafe { pg_sys::GetCurrentTimestamp() } => {
            WorkerSchedule::RunImmediately
        }
        Some(timestamp) => {
            WorkerSchedule::RunAfter(SchedulerPolicy::delay_until(timestamp))
        }
    })
}

fn run(worker_context: &WorkerContext<'_>) -> WorkerSchedule {
    worker_context.process_config_reload_if_pending();
    if !crate::gucs::auto_maintenance_enabled() {
        return WorkerSchedule::Idle;
    }
    let policy = SchedulerPolicy::configured();
    let candidates = match WorkerTransaction::run(|| load_candidates(policy)) {
        Ok(candidates) => candidates,
        Err(error) => {
            report_warning(format_args!(
                "automatic Iceberg maintenance could not load due tables: {error}"
            ));
            return WorkerSchedule::RunAfter(policy.naptime);
        }
    };

    for candidate in candidates.iter().take(policy.max_tables) {
        worker_context.process_config_reload_if_pending();
        pg_sys::check_for_interrupts!();
        match WorkerTransaction::run(|| maintain_relation(candidate)) {
            Ok(
                MaintenanceAttempt::Completed | MaintenanceAttempt::StaleCandidate,
            ) => {}
            Ok(MaintenanceAttempt::LockSkipped) => {
                if let Err(error) =
                    WorkerTransaction::run(|| defer_candidate(candidate, policy))
                {
                    report_warning(format_args!(
                        "automatic Iceberg maintenance could not defer locked relation {}: {}",
                        candidate.relid.to_u32(),
                        error,
                    ));
                }
            }
            Err(error) => {
                if let Err(defer_error) =
                    WorkerTransaction::run(|| defer_candidate(candidate, policy))
                {
                    report_warning(format_args!(
                        "automatic Iceberg maintenance could not defer failed relation {}: {}",
                        candidate.relid.to_u32(),
                        defer_error,
                    ));
                }
                report_warning(format_args!(
                    "automatic Iceberg maintenance skipped relation {}: {}",
                    candidate.relid.to_u32(),
                    error,
                ));
            }
        }
        pg_sys::check_for_interrupts!();
    }

    WorkerTransaction::run(next_worker_schedule).unwrap_or_else(|error| {
        report_warning(format_args!(
            "automatic Iceberg maintenance could not determine its next deadline: {error}"
        ));
        WorkerSchedule::RunAfter(policy.naptime)
    })
}

#[pg_schema]
mod iceberg {
    use super::*;

    #[pg_extern]
    fn maintenance_worker(worker_context: Internal) -> i64 {
        // SAFETY: this SQL-inaccessible `internal` argument is supplied only
        // by the pg-lakebase runtime extension-worker wrapper.
        let worker_context = unsafe { WorkerContext::from_internal(&worker_context) };
        run(&worker_context).into_raw()
    }
}
