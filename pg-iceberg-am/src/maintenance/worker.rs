//! Periodic logical Iceberg maintenance scheduled by `pg_lakebase_runtime`.

use std::time::Duration;

use pg_lakebase_core::diag::{PgReportError, report_warning};
use pg_lakebase_core::extension_worker::{
    WorkerContext, WorkerExit, WorkerTransaction,
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
use crate::catalog::automatic_maintenance::{
    AutomaticMaintenanceCatalog, AutomaticMaintenanceOutcome,
};
use crate::error::IcebergResult;

fn candidate_relations() -> Result<Vec<pg_sys::Oid>, PgReportError> {
    let limit = crate::gucs::auto_maintenance_max_tables();
    let catalog = AutomaticMaintenanceCatalog::open()
        .map_err(PgReportError::from_domain_error)?;
    // SAFETY: this worker runs inside a connected PostgreSQL backend.
    let backend_seed =
        u64::try_from(unsafe { pg_sys::MyProcPid }).unwrap_or_default();
    // SAFETY: the worker transaction is active and PostgreSQL initializes its
    // timestamp subsystem before invoking extension code.
    let now = unsafe { pg_sys::GetCurrentTimestamp() };
    catalog
        .eligible_relations(limit, now, backend_seed)
        .map_err(PgReportError::from_domain_error)
}

#[derive(Clone, Copy)]
struct SchedulerPolicy {
    interval: Duration,
    failure_backoff_max: Duration,
    jitter_percent: u32,
}

impl SchedulerPolicy {
    fn configured() -> Self {
        Self {
            interval: crate::gucs::auto_maintenance_interval(),
            failure_backoff_max: crate::gucs::auto_maintenance_failure_backoff_max(),
            jitter_percent: crate::gucs::auto_maintenance_jitter_percent(),
        }
    }

    fn jittered(self, delay: Duration, seed: u64) -> Duration {
        if self.jitter_percent == 0 || delay.is_zero() {
            return delay;
        }
        let spread = delay
            .as_millis()
            .saturating_mul(u128::from(self.jitter_percent))
            / 100;
        if spread == 0 {
            return delay;
        }
        let mixed = seed
            .wrapping_add(0x9e37_79b9_7f4a_7c15)
            .wrapping_mul(0xbf58_476d_1ce4_e5b9)
            ^ seed.rotate_left(23);
        let width = spread.saturating_mul(2).saturating_add(1);
        let offset = u128::from(mixed) % width;
        let base = delay.as_millis();
        let millis = base.saturating_sub(spread).saturating_add(offset);
        Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
    }

    fn relation_delay(self, relid: pg_sys::Oid) -> Duration {
        self.jittered(self.interval, u64::from(relid.to_u32()))
    }

    fn failure_delay(
        self,
        relid: pg_sys::Oid,
        consecutive_failures: u32,
    ) -> Duration {
        let shift = consecutive_failures.saturating_sub(1).min(31);
        let multiplied = self
            .interval
            .checked_mul(1_u32 << shift)
            .unwrap_or(self.failure_backoff_max);
        self.jittered(
            multiplied.min(self.failure_backoff_max),
            u64::from(relid.to_u32()) ^ u64::from(consecutive_failures),
        )
        .min(self.failure_backoff_max)
    }

    fn relation_next_attempt_at(
        self,
        relid: pg_sys::Oid,
        attempted_at: pg_sys::TimestampTz,
    ) -> pg_sys::TimestampTz {
        Self::timestamp_after(attempted_at, self.relation_delay(relid))
    }

    fn failure_next_attempt_at(
        self,
        relid: pg_sys::Oid,
        failures: u32,
        attempted_at: pg_sys::TimestampTz,
    ) -> pg_sys::TimestampTz {
        Self::timestamp_after(attempted_at, self.failure_delay(relid, failures))
    }

    fn timestamp_after(
        timestamp: pg_sys::TimestampTz,
        delay: Duration,
    ) -> pg_sys::TimestampTz {
        let micros = i64::try_from(delay.as_micros()).unwrap_or(i64::MAX);
        timestamp.saturating_add(micros)
    }
}

fn record_success(
    relid: pg_sys::Oid,
    outcome: AutomaticMaintenanceOutcome,
    policy: SchedulerPolicy,
) -> IcebergResult<()> {
    // SAFETY: record_success executes inside WorkerTransaction::run.
    let attempted_at = unsafe { pg_sys::GetCurrentTimestamp() };
    let next_attempt_at = policy.relation_next_attempt_at(relid, attempted_at);
    let catalog = AutomaticMaintenanceCatalog::open()?;
    catalog.record_success(relid, outcome, attempted_at, next_attempt_at)
}

fn record_failure(
    relid: pg_sys::Oid,
    error: &str,
    policy: SchedulerPolicy,
) -> IcebergResult<()> {
    let catalog = AutomaticMaintenanceCatalog::open()?;
    let failures = catalog.consecutive_failures(relid)?.saturating_add(1);
    // SAFETY: record_failure executes inside WorkerTransaction::run.
    let attempted_at = unsafe { pg_sys::GetCurrentTimestamp() };
    let next_attempt_at =
        policy.failure_next_attempt_at(relid, failures, attempted_at);
    catalog.record_failure(relid, failures, error, attempted_at, next_attempt_at)
}

fn maintain_relation(
    relid: pg_sys::Oid,
) -> Result<AutomaticMaintenanceOutcome, PgReportError> {
    let locked = unsafe {
        pg_sys::ConditionalLockRelationOid(
            relid,
            pg_sys::ShareUpdateExclusiveLock as pg_sys::LOCKMODE,
        )
    };
    if !locked {
        return Ok(AutomaticMaintenanceOutcome::LockSkipped);
    }
    let relation = RelationGuard::open(relid, pg_sys::NoLock as pg_sys::LOCKMODE)
        .map_err(PgReportError::from_domain_error)?;
    let relation = relation.as_handle();
    let expected_am = <IcebergTableMaintenanceProvider as LakebaseTableMaintenanceProvider>::access_method_oid();
    if expected_am != Some(relation.access_method_oid()) {
        return Ok(AutomaticMaintenanceOutcome::NoWork);
    }
    let command_time = TableMaintenanceCommandTime::now()
        .map_err(PgReportError::from_domain_error)?;
    let report =
        <IcebergTableMaintenanceProvider as LakebaseTableMaintenanceProvider>::execute(
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
    )
    .map_err(PgReportError::from_domain_error)?;
    let did_work = report.groups_rewritten != 0
        || report.snapshots_expired != 0
        || report.manifests_rewritten != 0
        || report.objects_scheduled_for_deletion != 0;
    Ok(if did_work {
        AutomaticMaintenanceOutcome::Maintained
    } else {
        AutomaticMaintenanceOutcome::NoWork
    })
}

#[pg_schema]
mod iceberg {
    use super::*;

    #[pg_extern]
    fn automatic_maintenance_worker(worker_context: Internal) -> i64 {
        let context = unsafe { WorkerContext::from_internal(&worker_context) }
            .map_err(|source| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("invalid Iceberg maintenance worker context: {source}"),
                )
            })
            .unwrap_or_else(|error| error.report());
        let database_oid = context.database_oid();
        let policy = SchedulerPolicy::configured();

        if !crate::gucs::auto_maintenance_enabled() {
            return WorkerExit::RestartAfter(
                policy.jittered(policy.interval, u64::from(database_oid)),
            )
            .encode();
        }
        let relations = WorkerTransaction::run(candidate_relations)
            .unwrap_or_else(|error| error.report());
        for relid in relations {
            pgrx::pg_sys::check_for_interrupts!();
            match WorkerTransaction::run(|| maintain_relation(relid)) {
                Ok(outcome) => {
                    if let Err(error) = WorkerTransaction::run(|| {
                        record_success(relid, outcome, policy)
                            .map_err(PgReportError::from_domain_error)
                    }) {
                        report_warning(format_args!(
                            "automatic Iceberg maintenance could not persist state for relation {}: {}",
                            relid.to_u32(),
                            error,
                        ));
                    }
                }
                Err(error) => {
                    let message = error.to_string();
                    if let Err(state_error) = WorkerTransaction::run(|| {
                        record_failure(relid, &message, policy)
                            .map_err(PgReportError::from_domain_error)
                    }) {
                        report_warning(format_args!(
                            "automatic Iceberg maintenance could not persist failure state for relation {}: {}",
                            relid.to_u32(),
                            state_error,
                        ));
                    }
                    report_warning(format_args!(
                        "automatic Iceberg maintenance skipped relation {}: {}",
                        relid.to_u32(),
                        error,
                    ));
                }
            }
        }
        let seed = u64::from(database_oid)
            ^ u64::try_from(unsafe { pg_sys::MyProcPid }).unwrap_or_default();
        WorkerExit::RestartAfter(policy.jittered(policy.interval, seed)).encode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> SchedulerPolicy {
        SchedulerPolicy {
            interval: Duration::from_secs(100),
            failure_backoff_max: Duration::from_secs(1_000),
            jitter_percent: 0,
        }
    }

    #[test]
    fn failure_backoff_is_exponential_and_capped() {
        let relid = pg_sys::Oid::from(42_u32);
        assert_eq!(policy().failure_delay(relid, 1), Duration::from_secs(100));
        assert_eq!(policy().failure_delay(relid, 2), Duration::from_secs(200));
        assert_eq!(policy().failure_delay(relid, 5), Duration::from_secs(1_000));
        assert_eq!(
            policy().failure_delay(relid, 32),
            Duration::from_secs(1_000)
        );
    }

    #[test]
    fn jitter_is_bounded_and_stable_for_a_relation() {
        let policy = SchedulerPolicy {
            jitter_percent: 20,
            ..policy()
        };
        let delay = policy.relation_delay(pg_sys::Oid::from(7_u32));
        assert_eq!(delay, policy.relation_delay(pg_sys::Oid::from(7_u32)));
        assert!(delay >= Duration::from_secs(80));
        assert!(delay <= Duration::from_secs(120));
    }
}
