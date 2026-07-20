use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::time::Instant;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;

use crate::gucs;

use super::bgworker::DynamicWorkerSpawner;
use super::locks::DatabaseLifecycleLock;
use super::store::{ReconcilerReservation, RuntimeStore};
use super::{CRASH_BACKOFF, RECONCILER_FUNCTION};

const RECONCILER_WORKER_NAME: &str = "pg-lakebase-runtime database reconciler";

pub(super) struct Launcher;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconcilerStart {
    Started,
    AlreadyActive,
    Deferred,
    AtCapacity,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DatabaseSchedule {
    Idle,
    Pending,
    RetryAt(Instant),
}

struct DatabaseScheduler {
    pending: VecDeque<u32>,
    databases: HashMap<u32, DatabaseSchedule>,
}

impl DatabaseScheduler {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            databases: HashMap::new(),
        }
    }

    fn enqueue(&mut self, database_oid: u32) {
        if database_oid == 0 {
            return;
        }
        let schedule = self
            .databases
            .entry(database_oid)
            .or_insert(DatabaseSchedule::Idle);
        if *schedule == DatabaseSchedule::Idle {
            *schedule = DatabaseSchedule::Pending;
            self.pending.push_back(database_oid);
        }
    }

    fn defer_retry(&mut self, database_oid: u32) {
        self.pending
            .retain(|queued_oid| *queued_oid != database_oid);
        self.databases.insert(
            database_oid,
            DatabaseSchedule::RetryAt(Instant::now() + CRASH_BACKOFF),
        );
    }

    fn promote_due_retries(&mut self) {
        let now = Instant::now();
        let due: Vec<_> = self
            .databases
            .iter()
            .filter_map(|(&database_oid, &schedule)| {
                matches!(schedule, DatabaseSchedule::RetryAt(not_before) if not_before <= now)
                    .then_some(database_oid)
            })
            .collect();
        for database_oid in due {
            self.databases.insert(database_oid, DatabaseSchedule::Idle);
            self.enqueue(database_oid);
        }
    }

    fn reconcile_live(&mut self, live: &HashSet<u32>, probe_all: bool) {
        self.pending
            .retain(|database_oid| live.contains(database_oid));
        self.databases
            .retain(|database_oid, _| live.contains(database_oid));
        for &database_oid in live {
            let is_new = !self.databases.contains_key(&database_oid);
            self.databases
                .entry(database_oid)
                .or_insert(DatabaseSchedule::Idle);
            if probe_all || is_new {
                self.enqueue(database_oid);
            }
        }
    }

    fn len(&self) -> usize {
        self.pending.len()
    }

    fn pop_front(&mut self) -> Option<u32> {
        let database_oid = self.pending.pop_front()?;
        self.databases.insert(database_oid, DatabaseSchedule::Idle);
        Some(database_oid)
    }

    fn push_front(&mut self, database_oid: u32) {
        let schedule = self
            .databases
            .entry(database_oid)
            .or_insert(DatabaseSchedule::Idle);
        if *schedule == DatabaseSchedule::Idle {
            *schedule = DatabaseSchedule::Pending;
            self.pending.push_front(database_oid);
        }
    }
}

impl Launcher {
    pub(super) fn run() {
        BackgroundWorker::attach_signal_handlers(
            SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
        );
        BackgroundWorker::connect_worker_to_spi(None, None);
        unsafe {
            pg_sys::before_shmem_exit(
                Some(launcher_exit_callback),
                pg_sys::Datum::from(0_usize),
            );
        }

        let store = RuntimeStore::new();
        store.set_launcher_running();

        let mut last_full_scan = Instant::now()
            .checked_sub(gucs::reconcile_interval())
            .unwrap_or_else(Instant::now);
        let mut database_scheduler = DatabaseScheduler::new();

        loop {
            if BackgroundWorker::sighup_received() {
                unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) };
            }

            database_scheduler.promote_due_retries();
            store.recover_timed_out_reconcilers();
            for database_oid in store.drain_completed_reconcilers() {
                database_scheduler.defer_retry(database_oid);
            }

            let explicit_full_scan = store.take_full_scan_request();
            let interval_elapsed =
                last_full_scan.elapsed() >= gucs::reconcile_interval();
            if explicit_full_scan || interval_elapsed {
                Self::reconcile_database_list(
                    &store,
                    &mut database_scheduler,
                    explicit_full_scan,
                );
                if interval_elapsed && !explicit_full_scan {
                    for database_oid in store.all_worker_databases() {
                        database_scheduler.enqueue(database_oid);
                    }
                }
                last_full_scan = Instant::now();
            }

            store.schedule_due_workers();
            for database_oid in store.requested_databases() {
                database_scheduler.enqueue(database_oid);
            }

            Self::start_pending_reconcilers(&store, &mut database_scheduler);

            if !BackgroundWorker::wait_latch(Some(gucs::launcher_naptime())) {
                break;
            }
        }
    }

    fn reconcile_database_list(
        store: &RuntimeStore,
        database_scheduler: &mut DatabaseScheduler,
        probe_all: bool,
    ) {
        let databases = match BackgroundWorker::transaction(AssertUnwindSafe(
            Self::scan_databases,
        )) {
            Ok(databases) => databases,
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to scan pg_database for Lakebase workers: {error}"
                ));
                store.request_full_rescan();
                return;
            }
        };
        database_scheduler.reconcile_live(&databases, probe_all);
        for pid in store.remove_dropped_databases(&databases) {
            // PostgreSQL 17's pg_signal_backend() likewise validates shared
            // process state and calls kill(2) directly, explicitly accepting
            // the same tiny PID-reuse window. ProcSendSignal(ProcNumber) only
            // sets a latch; postmaster mediation requires a retained
            // BackgroundWorkerHandle.
            // Generation fencing protects our slots, not this OS-level race.
            // SAFETY: PIDs came from generation-fenced reconciler/worker slots.
            // ESRCH is harmless because the process may have exited concurrently.
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }

    fn scan_databases() -> Result<HashSet<u32>, pg_lakebase_core::diag::PgError> {
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
        let mut databases = HashSet::new();
        while let Some(tuple) = scan.get_next()? {
            // SAFETY: the catalog scan tuple is valid until the next scan call;
            // FormData_pg_database fixed fields are available through GETSTRUCT.
            let database = unsafe {
                &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_database)
            };
            if !database.datistemplate && database.datallowconn {
                databases.insert(database.oid.to_u32());
            }
        }
        Ok(databases)
    }

    fn start_pending_reconcilers(
        store: &RuntimeStore,
        database_scheduler: &mut DatabaseScheduler,
    ) {
        let attempts = database_scheduler.len();
        for _ in 0..attempts {
            let Some(database_oid) = database_scheduler.pop_front() else {
                return;
            };
            match Self::try_start_reconciler(store, database_oid) {
                ReconcilerStart::Started | ReconcilerStart::AlreadyActive => {}
                ReconcilerStart::Deferred => database_scheduler.enqueue(database_oid),
                ReconcilerStart::AtCapacity => {
                    database_scheduler.push_front(database_oid);
                    return;
                }
                ReconcilerStart::Failed => return,
            }
        }
    }

    fn try_start_reconciler(
        store: &RuntimeStore,
        database_oid: u32,
    ) -> ReconcilerStart {
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            if !DatabaseLifecycleLock::new(database_oid).try_acquire_shared() {
                return ReconcilerStart::Deferred;
            }
            let token = match store.reserve_reconciler(database_oid) {
                ReconcilerReservation::Reserved(token) => token,
                ReconcilerReservation::AlreadyActive => {
                    return ReconcilerStart::AlreadyActive;
                }
                ReconcilerReservation::AtCapacity => {
                    return ReconcilerStart::AtCapacity;
                }
            };
            if let Err(error) = DynamicWorkerSpawner::start_reconciler(
                RECONCILER_FUNCTION,
                RECONCILER_WORKER_NAME,
                token,
            ) {
                crate::diag::warning(format_args!(
                    "failed to start pg-lakebase-runtime database reconciler: {error}"
                ));
                store.reconciler_start_failed(token);
                ReconcilerStart::Failed
            } else {
                ReconcilerStart::Started
            }
        }))
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn launcher_exit_callback(_code: i32, _arg: pg_sys::Datum) {
    RuntimeStore::new().clear_launcher();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_queue_deduplicates_pending_work() {
        let mut queue = DatabaseScheduler::new();
        queue.enqueue(42);
        queue.enqueue(42);

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.pop_front(), Some(42));
        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn database_queue_suppresses_retry_until_promoted_or_removed() {
        let mut queue = DatabaseScheduler::new();
        queue.enqueue(42);
        assert_eq!(queue.pop_front(), Some(42));

        queue.defer_retry(42);
        queue.enqueue(42);
        assert_eq!(queue.len(), 0);

        queue.reconcile_live(&HashSet::new(), false);
        queue.enqueue(42);
        assert_eq!(queue.pop_front(), Some(42));
    }

    #[test]
    fn database_queue_removes_dropped_databases() {
        let mut queue = DatabaseScheduler::new();
        queue.enqueue(42);
        queue.enqueue(43);
        queue.reconcile_live(&HashSet::from([43]), false);

        assert_eq!(queue.pop_front(), Some(43));
        assert_eq!(queue.pop_front(), None);
    }
}
