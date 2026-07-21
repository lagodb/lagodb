use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use crate::gucs;
use crate::state::INVALID_OID;

use super::CRASH_BACKOFF;
use super::locks::DatabaseLifecycleLock;
use super::store::{ReconcilerReservation, ReconcilerRetry, RuntimeStore};
use super::supervisor::{LauncherExitState, LauncherSupervisor};

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
    due_retries: Vec<u32>,
}

impl DatabaseScheduler {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            databases: HashMap::new(),
            due_retries: Vec::new(),
        }
    }

    fn enqueue(&mut self, database_oid: u32) {
        if database_oid == INVALID_OID {
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

    fn enqueue_immediate(&mut self, database_oid: u32) {
        if database_oid == INVALID_OID {
            return;
        }
        self.pending
            .retain(|queued_oid| *queued_oid != database_oid);
        self.databases
            .insert(database_oid, DatabaseSchedule::Pending);
        self.pending.push_back(database_oid);
    }

    fn promote_due_retries(&mut self) {
        let now = Instant::now();
        self.due_retries.clear();
        self.due_retries.extend(self.databases.iter().filter_map(
            |(&database_oid, &schedule)| {
                matches!(schedule, DatabaseSchedule::RetryAt(not_before) if not_before <= now)
                    .then_some(database_oid)
            },
        ));
        for database_oid in self.due_retries.drain(..) {
            self.databases.insert(database_oid, DatabaseSchedule::Idle);
            let schedule = self
                .databases
                .get_mut(&database_oid)
                .expect("promoted database remains scheduled");
            *schedule = DatabaseSchedule::Pending;
            self.pending.push_back(database_oid);
        }
    }

    fn next_deadline_delay(&self) -> Option<Duration> {
        let now = Instant::now();
        self.databases
            .values()
            .filter_map(|schedule| match schedule {
                DatabaseSchedule::RetryAt(deadline) => {
                    Some(deadline.saturating_duration_since(now))
                }
                DatabaseSchedule::Idle | DatabaseSchedule::Pending => None,
            })
            .min()
    }

    fn reconcile_live(&mut self, live: &HashSet<u32>) -> Vec<u32> {
        self.pending
            .retain(|database_oid| live.contains(database_oid));
        self.databases
            .retain(|database_oid, _| live.contains(database_oid));
        let mut discovered = Vec::new();
        for &database_oid in live {
            let is_new = !self.databases.contains_key(&database_oid);
            self.databases
                .entry(database_oid)
                .or_insert(DatabaseSchedule::Idle);
            if is_new {
                discovered.push(database_oid);
            }
        }
        discovered
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
        let exit_state = Box::into_raw(Box::new(LauncherExitState::new()));
        unsafe {
            pg_sys::before_shmem_exit(
                Some(launcher_exit_callback),
                pg_sys::Datum::from(exit_state as usize),
            );
        }
        super::signals::BackgroundWorkerSignals::install_launcher();
        BackgroundWorker::connect_worker_to_spi(None, None);

        let store = RuntimeStore::new();
        let launcher_epoch = store
            .claim_launcher()
            .unwrap_or_else(|error| error.report());
        let mut supervisor = LauncherSupervisor::new(exit_state);
        while !supervisor.drain_previous_generation(&store, launcher_epoch) {
            super::signals::BackgroundWorkerSignals::process_config_reload();
            if !super::signals::BackgroundWorkerSignals::wait_latch(
                gucs::launcher_naptime(),
            ) {
                return;
            }
        }
        if !store.begin_reconciliation() {
            crate::diag::warning(
                "Lakebase launcher lost ownership before database reconciliation",
            );
            return;
        }

        let mut last_full_scan = Instant::now()
            .checked_sub(gucs::reconcile_interval())
            .unwrap_or_else(Instant::now);
        let mut database_scheduler = DatabaseScheduler::new();
        let mut recovery_catalog_scan_complete = false;
        let mut recovery_finished = false;

        loop {
            super::signals::BackgroundWorkerSignals::process_config_reload();

            database_scheduler.promote_due_retries();
            for (database_oid, retry) in supervisor.reap_stopped(&store) {
                match retry {
                    ReconcilerRetry::Immediate => {
                        database_scheduler.enqueue_immediate(database_oid)
                    }
                    ReconcilerRetry::Backoff => {
                        database_scheduler.defer_retry(database_oid)
                    }
                    ReconcilerRetry::None => {}
                }
            }
            supervisor.request_terminations(&store);

            let explicit_full_scan = store.take_full_scan_request();
            let interval_elapsed =
                last_full_scan.elapsed() >= gucs::reconcile_interval();
            if explicit_full_scan || interval_elapsed {
                let scan_complete = Self::reconcile_database_list(
                    &store,
                    &mut database_scheduler,
                    explicit_full_scan,
                );
                if !recovery_finished && scan_complete {
                    recovery_catalog_scan_complete = true;
                }
                last_full_scan = Instant::now();
            }

            store.promote_due_workers();
            for database_oid in store.requested_databases() {
                database_scheduler.enqueue(database_oid);
            }

            Self::start_pending_reconcilers(
                &store,
                &mut supervisor,
                &mut database_scheduler,
            );
            if !recovery_finished && recovery_catalog_scan_complete {
                recovery_finished = store.complete_recovery();
            }
            let remaining =
                supervisor.remaining_worker_capacity(gucs::max_active_workers());
            for launch in store.reserve_ready_workers(remaining) {
                supervisor.start_worker(&store, launch);
            }
            let wait_timeout =
                Self::next_wait_timeout(&store, &database_scheduler, last_full_scan);
            if !super::signals::BackgroundWorkerSignals::wait_latch(wait_timeout) {
                break;
            }
        }
    }

    fn reconcile_database_list(
        store: &RuntimeStore,
        database_scheduler: &mut DatabaseScheduler,
        probe_all: bool,
    ) -> bool {
        let databases = match BackgroundWorker::transaction(AssertUnwindSafe(
            Self::scan_databases,
        )) {
            Ok(databases) => databases,
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to scan pg_database for Lakebase workers: {error}"
                ));
                for database_oid in store.all_worker_databases() {
                    database_scheduler.enqueue(database_oid);
                }
                store.request_full_rescan();
                return false;
            }
        };
        let discovered = database_scheduler.reconcile_live(&databases);
        let scheduled = if probe_all {
            databases.iter().copied().collect()
        } else {
            let mut scheduled = store.all_worker_databases();
            scheduled.extend(discovered);
            scheduled
        };
        for database_oid in scheduled {
            database_scheduler.enqueue(database_oid);
        }
        store.remove_dropped_databases(&databases);
        true
    }

    fn next_wait_timeout(
        store: &RuntimeStore,
        database_scheduler: &DatabaseScheduler,
        last_full_scan: Instant,
    ) -> Duration {
        let reconcile_interval = gucs::reconcile_interval();
        let mut timeout = gucs::launcher_naptime();
        for candidate in [
            store.next_deadline_delay(),
            database_scheduler.next_deadline_delay(),
            Some(reconcile_interval.saturating_sub(last_full_scan.elapsed())),
        ]
        .into_iter()
        .flatten()
        {
            timeout = timeout.min(candidate);
        }
        timeout
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
        supervisor: &mut LauncherSupervisor,
        database_scheduler: &mut DatabaseScheduler,
    ) {
        let attempts = database_scheduler.len();
        for _ in 0..attempts {
            let Some(database_oid) = database_scheduler.pop_front() else {
                return;
            };
            match Self::try_start_reconciler(store, supervisor, database_oid) {
                ReconcilerStart::Started => {}
                ReconcilerStart::AlreadyActive => {
                    database_scheduler.defer_retry(database_oid)
                }
                ReconcilerStart::Deferred => database_scheduler.enqueue(database_oid),
                ReconcilerStart::AtCapacity => {
                    database_scheduler.push_front(database_oid);
                    return;
                }
                ReconcilerStart::Failed => {
                    database_scheduler.defer_retry(database_oid);
                    return;
                }
            }
        }
    }

    fn try_start_reconciler(
        store: &RuntimeStore,
        supervisor: &mut LauncherSupervisor,
        database_oid: u32,
    ) -> ReconcilerStart {
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            if !DatabaseLifecycleLock::new(database_oid).try_acquire_shared() {
                return ReconcilerStart::Deferred;
            }
            let token = match store
                .reserve_reconciler(database_oid, supervisor.active_reconcilers())
            {
                ReconcilerReservation::Reserved(token) => token,
                ReconcilerReservation::AlreadyActive => {
                    return ReconcilerStart::AlreadyActive;
                }
                ReconcilerReservation::AtCapacity => {
                    return ReconcilerStart::AtCapacity;
                }
                ReconcilerReservation::Recovering => {
                    return ReconcilerStart::Deferred;
                }
            };
            if !supervisor.start_reconciler(store, database_oid, token) {
                ReconcilerStart::Failed
            } else {
                ReconcilerStart::Started
            }
        }))
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn launcher_exit_callback(_code: i32, _arg: pg_sys::Datum) {
    let exit_state = _arg.value() as *mut LauncherExitState;
    // SAFETY: the launcher leaks this process-lifetime registry before
    // registering the callback, so the pointer remains valid through proc_exit.
    if let Some(exit_state) = unsafe { exit_state.as_ref() } {
        exit_state.request_all_terminations();
    }
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

        queue.reconcile_live(&HashSet::new());
        queue.enqueue(42);
        assert_eq!(queue.pop_front(), Some(42));
    }

    #[test]
    fn database_queue_removes_dropped_databases() {
        let mut queue = DatabaseScheduler::new();
        queue.enqueue(42);
        queue.enqueue(43);
        queue.reconcile_live(&HashSet::from([43]));

        assert_eq!(queue.pop_front(), Some(43));
        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn database_queue_retains_retry_without_blocking_other_databases() {
        let mut queue = DatabaseScheduler::new();
        queue.enqueue(42);
        assert_eq!(queue.pop_front(), Some(42));

        queue.defer_retry(42);
        queue.enqueue(43);
        assert_eq!(queue.pop_front(), Some(43));
        assert!(queue.next_deadline_delay().is_some());
    }

    #[test]
    fn durable_request_after_active_reconciler_preempts_retry_delay() {
        let mut queue = DatabaseScheduler::new();
        queue.enqueue(42);
        assert_eq!(queue.pop_front(), Some(42));
        queue.defer_retry(42);

        queue.enqueue_immediate(42);

        assert_eq!(queue.pop_front(), Some(42));
    }
}
