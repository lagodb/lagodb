use std::collections::HashSet;
use std::time::{Duration, Instant};

use pg_lakebase_core::diag::{PgError, PgReportError};
use pg_lakebase_core::extension_worker::WorkerTransaction;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use crate::{diag, gucs};

use super::bgworker::{DynamicWorkerRegistration, DynamicWorkerStartResult};
use super::lock::DatabaseLifecycleLock;
use super::scheduler::Scheduler;
use super::store::{CoordinatorRegistration, Store};
use super::{CAPACITY_RETRY, SUPERVISOR_ERROR_RETRY};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoordinatorStart {
    Started,
    AlreadyActive,
    NoWork,
    Deferred,
    RetrySoon,
    Failed,
}

struct DatabaseCatalogSnapshot {
    live: HashSet<u32>,
    coordinator_candidates: HashSet<u32>,
}

pub(super) struct Supervisor;

impl Supervisor {
    pub(super) fn run() {
        // SAFETY: the callback clears only this supervisor's shared-memory identity.
        unsafe {
            pg_sys::before_shmem_exit(
                Some(supervisor_exit_callback),
                pg_sys::Datum::from(0usize),
            )
        };
        super::signals::BackgroundWorkerSignals::install_supervisor();
        BackgroundWorker::connect_worker_to_spi(None, None);

        let store = Store::new();
        store.claim_supervisor();

        let mut last_full_scan = Instant::now()
            .checked_sub(gucs::reconcile_interval())
            .unwrap_or_else(Instant::now);
        let mut scheduler = Scheduler::new();

        loop {
            super::signals::BackgroundWorkerSignals::process_config_reload();
            scheduler.promote_due_retries();

            let explicit_full_scan = store.take_full_scan_request();
            let interval_elapsed =
                last_full_scan.elapsed() >= gucs::reconcile_interval();
            if explicit_full_scan || interval_elapsed {
                Self::reconcile_database_list(
                    &store,
                    &mut scheduler,
                    explicit_full_scan,
                );
                last_full_scan = Instant::now();
            }

            for database_oid in store.requested_databases() {
                scheduler.enqueue(database_oid);
            }

            Self::start_pending_coordinators(&store, &mut scheduler);
            let wait_timeout = Self::next_wait_timeout(&scheduler, last_full_scan);
            if !super::signals::BackgroundWorkerSignals::wait_latch(wait_timeout) {
                break;
            }
        }
    }

    fn start_coordinator(store: &Store, database_oid: u32) -> CoordinatorStart {
        match store.register_coordinator(database_oid) {
            CoordinatorRegistration::Registered(registration) => {
                match registration.wait_for_startup() {
                    DynamicWorkerStartResult::Started(_) => CoordinatorStart::Started,
                    DynamicWorkerStartResult::Stopped
                    | DynamicWorkerStartResult::PostmasterDied => {
                        store.coordinator_registration_failed(database_oid);
                        CoordinatorStart::RetrySoon
                    }
                }
            }
            CoordinatorRegistration::AlreadyActive => CoordinatorStart::AlreadyActive,
            CoordinatorRegistration::NoWork => CoordinatorStart::NoWork,
            CoordinatorRegistration::Failed(error) => {
                diag::warning(format_args!(
                    "failed to register pg-lakebase coordinator: database_oid={database_oid}, error={error}"
                ));
                CoordinatorStart::RetrySoon
            }
        }
    }

    fn reconcile_database_list(
        store: &Store,
        scheduler: &mut Scheduler,
        probe_all: bool,
    ) {
        let databases = match WorkerTransaction::run(|| {
            Self::scan_databases().map_err(PgReportError::from)
        }) {
            Ok(databases) => databases,
            Err(error) => {
                diag::warning(format_args!(
                    "failed to scan pg_database for Lakebase workers: {error}"
                ));
                for database_oid in store.all_worker_databases() {
                    scheduler.enqueue(database_oid);
                }
                store.request_full_rescan();
                return;
            }
        };
        let discovered = scheduler.reconcile_live(&databases.coordinator_candidates);
        let scheduled = if probe_all {
            databases.coordinator_candidates.iter().copied().collect()
        } else {
            let mut scheduled: Vec<_> = store
                .all_worker_databases()
                .into_iter()
                .filter(|database_oid| {
                    databases.coordinator_candidates.contains(database_oid)
                })
                .collect();
            scheduled.extend(discovered);
            scheduled
        };
        for database_oid in scheduled {
            scheduler.enqueue(database_oid);
        }
        store.remove_dropped_databases(&databases.live);
    }

    fn next_wait_timeout(scheduler: &Scheduler, last_full_scan: Instant) -> Duration {
        let reconcile_interval = gucs::reconcile_interval();
        let mut timeout = gucs::supervisor_naptime();
        for candidate in [
            scheduler.next_deadline_delay(),
            Some(reconcile_interval.saturating_sub(last_full_scan.elapsed())),
        ]
        .into_iter()
        .flatten()
        {
            timeout = timeout.min(candidate);
        }
        timeout
    }

    fn scan_databases() -> Result<DatabaseCatalogSnapshot, PgError> {
        use pg_lakebase_core::catalog::{CatalogRelation, CatalogSnapshot};

        let relation = CatalogRelation::open(
            pg_sys::DatabaseRelationId,
            pg_sys::AccessShareLock as _,
        )?;
        let mut scan = relation.begin_scan(
            pg_sys::InvalidOid,
            false,
            CatalogSnapshot::Default,
            std::iter::empty(),
        )?;
        let mut live = HashSet::new();
        let mut coordinator_candidates = HashSet::new();
        while let Some(tuple) = scan.get_next()? {
            // SAFETY: the tuple remains valid until the next scan call and
            // FormData_pg_database fixed fields are available through GETSTRUCT.
            let database = unsafe {
                &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_database)
            };
            let database_oid = database.oid.to_u32();
            live.insert(database_oid);
            if !database.datistemplate {
                coordinator_candidates.insert(database_oid);
            }
        }
        Ok(DatabaseCatalogSnapshot {
            live,
            coordinator_candidates,
        })
    }

    fn start_pending_coordinators(store: &Store, scheduler: &mut Scheduler) {
        let attempts = scheduler.len();
        for _ in 0..attempts {
            let Some(database_oid) = scheduler.pop_front() else {
                return;
            };
            match Self::try_start_coordinator(store, database_oid) {
                CoordinatorStart::Started
                | CoordinatorStart::AlreadyActive
                | CoordinatorStart::NoWork => {}
                CoordinatorStart::Deferred => scheduler.enqueue(database_oid),
                CoordinatorStart::RetrySoon => {
                    scheduler.defer_retry(database_oid, CAPACITY_RETRY);
                    return;
                }
                CoordinatorStart::Failed => {
                    scheduler.defer_retry(database_oid, SUPERVISOR_ERROR_RETRY);
                    return;
                }
            }
        }
    }

    fn try_start_coordinator(store: &Store, database_oid: u32) -> CoordinatorStart {
        let mut start = CoordinatorStart::Failed;
        DynamicWorkerRegistration::with_transient_context(|| {
            start = match WorkerTransaction::run(|| {
                if !DatabaseLifecycleLock::new(database_oid).try_acquire_shared() {
                    return Ok::<_, PgReportError>(CoordinatorStart::Deferred);
                }
                Ok::<_, PgReportError>(Self::start_coordinator(store, database_oid))
            }) {
                Ok(start) => start,
                Err(error) => {
                    diag::warning(format_args!(
                        "failed to start pg-lakebase coordinator transaction: database_oid={database_oid}, error={error}"
                    ));
                    CoordinatorStart::Failed
                }
            };
        });
        start
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn supervisor_exit_callback(
    _code: i32,
    _arg: pg_sys::Datum,
) {
    Store::new().clear_supervisor();
}
