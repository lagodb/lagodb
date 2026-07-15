//! Database-local, on-demand maintenance worker supervisor.

use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_lakebase_storage::{StorageClient, StorageError};
use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use super::actor::{ActorResult, ActorRuntimeConfig, MaintenanceActorPool};
use super::gucs;
use super::item::{MaintenanceItem, MaintenanceItemId};
use super::repository::{MaintenanceRepository, QueuePoll};
use super::runner::MaintenanceExecutionOutcome;
use crate::bgworker::BackendLatch;
use crate::extension_worker::WorkerExit;
use crate::storage_service::StorageEndpoint;

const RESULT_PERSISTENCE_RETRY: Duration = Duration::from_secs(5);

pub fn init_gucs() {
    gucs::init();
}

/// Drain maintenance work in the worker's already-connected database.
pub fn run_database_worker() -> WorkerExit {
    if !gucs::enabled() {
        return WorkerExit::Dormant;
    }

    match current_schedule() {
        Schedule::Dormant => return WorkerExit::Dormant,
        Schedule::RestartAfter(delay) => {
            return WorkerExit::RestartAfter(delay);
        }
        Schedule::Ready => {}
    }

    let mut supervisor = match MaintenanceWorkerSupervisor::from_gucs() {
        Ok(supervisor) => supervisor,
        Err(error) => {
            return error.worker_exit();
        }
    };
    crate::diag::report_info("database-local maintenance worker started");
    let directive = supervisor.run_until_idle();
    supervisor.shutdown();
    crate::diag::report_info("database-local maintenance worker stopped");
    directive
}

enum Schedule {
    Ready,
    Dormant,
    RestartAfter(Duration),
}

fn current_schedule() -> Schedule {
    let poll = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        MaintenanceRepository::fetch_ready_batch(1, &HashSet::new())
    }));
    match poll {
        Ok(QueuePoll::Ready(batch))
            if !batch.tasks.is_empty() || !batch.invalid.is_empty() =>
        {
            Schedule::Ready
        }
        Ok(QueuePoll::Ready(_)) => next_schedule(),
        Ok(QueuePoll::Unavailable) => Schedule::Dormant,
        Err(error) => {
            crate::diag::report_warning(format_args!(
                "failed to inspect maintenance queue: {error}"
            ));
            Schedule::RestartAfter(Duration::from_secs(5))
        }
    }
}

fn next_schedule() -> Schedule {
    let next = BackgroundWorker::transaction(AssertUnwindSafe(
        MaintenanceRepository::next_pending_at,
    ));
    match next {
        Ok(Some(timestamp)) => {
            let now = unsafe { pg_sys::GetCurrentTimestamp() };
            if timestamp <= now {
                Schedule::Ready
            } else {
                let micros =
                    u64::try_from(timestamp.saturating_sub(now)).unwrap_or(u64::MAX);
                Schedule::RestartAfter(Duration::from_micros(micros))
            }
        }
        Ok(None) => Schedule::Dormant,
        Err(error) => {
            crate::diag::report_warning(format_args!(
                "failed to schedule maintenance queue: {error}"
            ));
            Schedule::RestartAfter(Duration::from_secs(5))
        }
    }
}

struct MaintenanceWorkerSupervisor {
    actors: MaintenanceActorPool,
    in_flight: HashMap<MaintenanceItemId, Arc<MaintenanceItem>>,
    pending_results: VecDeque<ActorResult>,
    persistence_warning_emitted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistenceStatus {
    Complete,
    PersistenceFailed,
}

#[derive(Debug, thiserror::Error)]
enum MaintenanceWorkerStartupError {
    #[error("failed to start maintenance actor pool: {0}")]
    ActorPool(#[source] std::io::Error),

    #[error("storage server is disabled")]
    StorageDisabled,

    #[error("failed to resolve storage endpoint: {0}")]
    StorageConfig(#[source] StorageError),

    #[error("storage server is unavailable at {}: {source}", socket_path.display())]
    StorageUnavailable {
        socket_path: PathBuf,
        #[source]
        source: StorageError,
    },
}

impl MaintenanceWorkerStartupError {
    fn worker_exit(&self) -> WorkerExit {
        match self {
            Self::StorageDisabled => {
                crate::diag::report_warning(
                    "database-local maintenance worker skipped: storage server is disabled",
                );
                WorkerExit::Dormant
            }
            Self::StorageConfig(_) | Self::StorageUnavailable { .. } => {
                crate::diag::report_warning(format_args!(
                    "database-local maintenance worker waiting for storage: {self}"
                ));
                WorkerExit::RestartAfter(Duration::from_secs(5))
            }
            Self::ActorPool(_) => {
                crate::diag::report_warning(format_args!(
                    "database-local maintenance worker startup failed: {self}"
                ));
                WorkerExit::RestartAfter(Duration::from_secs(5))
            }
        }
    }
}

impl MaintenanceWorkerSupervisor {
    fn from_gucs() -> Result<Self, MaintenanceWorkerStartupError> {
        let endpoint = StorageEndpoint::from_pg_gucs()
            .map_err(MaintenanceWorkerStartupError::StorageConfig)?;
        if !endpoint.is_enabled() {
            return Err(MaintenanceWorkerStartupError::StorageDisabled);
        }

        let runtime_config = actor_runtime_config();
        StorageClient::connect_with_timeout(
            endpoint.socket_path(),
            runtime_config.request_timeout,
        )
        .map_err(|source| {
            MaintenanceWorkerStartupError::StorageUnavailable {
                socket_path: endpoint.socket_path().to_path_buf(),
                source,
            }
        })?;

        let actors = MaintenanceActorPool::start(
            gucs::actor_threads(),
            endpoint.socket_path().to_path_buf(),
            runtime_config,
        )
        .map_err(MaintenanceWorkerStartupError::ActorPool)?;
        Ok(Self {
            actors,
            in_flight: HashMap::new(),
            pending_results: VecDeque::new(),
            persistence_warning_emitted: false,
        })
    }

    fn run_until_idle(&mut self) -> WorkerExit {
        loop {
            self.collect_actor_results();
            if self.apply_pending_results() == PersistenceStatus::PersistenceFailed {
                return WorkerExit::RestartAfter(RESULT_PERSISTENCE_RETRY);
            }

            if self.actors.has_finished_actor() {
                crate::diag::report_warning("maintenance actor exited unexpectedly");
                return WorkerExit::RestartAfter(Duration::from_secs(5));
            }

            if self.dispatch_ready_tasks() == PersistenceStatus::PersistenceFailed {
                return WorkerExit::RestartAfter(RESULT_PERSISTENCE_RETRY);
            }

            if self.in_flight.is_empty() && self.pending_results.is_empty() {
                match next_schedule() {
                    Schedule::Ready => continue,
                    Schedule::Dormant => return WorkerExit::Dormant,
                    Schedule::RestartAfter(delay) => {
                        return WorkerExit::RestartAfter(delay);
                    }
                }
            }

            if !BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
                return WorkerExit::Dormant;
            }
            if BackgroundWorker::sighup_received() {
                unsafe { process_config_reload() };
                self.actors.reload(actor_runtime_config());
            }
        }
    }

    fn collect_actor_results(&mut self) {
        while let Some(result) = self.actors.try_result() {
            self.pending_results.push_back(result);
        }
    }

    fn apply_pending_results(&mut self) -> PersistenceStatus {
        while let Some(result) = self.pending_results.front() {
            let Some(item) = self.in_flight.get(&result.item_id) else {
                self.pending_results.pop_front();
                continue;
            };
            if matches!(&result.outcome, MaintenanceExecutionOutcome::Cancelled) {
                let item_id = result.item_id;
                self.pending_results.pop_front();
                self.in_flight.remove(&item_id);
                continue;
            }

            let update = BackgroundWorker::transaction(AssertUnwindSafe(|| {
                match &result.outcome {
                    MaintenanceExecutionOutcome::Complete => {
                        MaintenanceRepository::complete(item.as_ref())
                    }
                    MaintenanceExecutionOutcome::Retryable(error) => {
                        MaintenanceRepository::retry(
                            item.as_ref(),
                            &error.to_string(),
                        )
                    }
                    MaintenanceExecutionOutcome::Permanent(error) => {
                        MaintenanceRepository::fail(item.as_ref(), &error.to_string())
                    }
                    MaintenanceExecutionOutcome::Cancelled => unreachable!(),
                }
            }));
            if let Err(error) = update {
                if !self.persistence_warning_emitted {
                    crate::diag::report_warning(format_args!(
                        "failed to persist maintenance result: {error}"
                    ));
                    self.persistence_warning_emitted = true;
                }
                return PersistenceStatus::PersistenceFailed;
            }
            self.persistence_warning_emitted = false;
            let item_id = result.item_id;
            self.pending_results.pop_front();
            self.in_flight.remove(&item_id);
        }
        PersistenceStatus::Complete
    }

    fn dispatch_ready_tasks(&mut self) -> PersistenceStatus {
        let capacity = self.actors.capacity();
        if capacity == 0 {
            return PersistenceStatus::Complete;
        }
        let in_flight: HashSet<_> = self.in_flight.keys().copied().collect();
        let poll = BackgroundWorker::transaction(AssertUnwindSafe(|| {
            MaintenanceRepository::fetch_ready_batch(capacity, &in_flight)
        }));
        let Ok(QueuePoll::Ready(batch)) = poll else {
            return PersistenceStatus::Complete;
        };

        for invalid in batch.invalid {
            if let Err(error) =
                BackgroundWorker::transaction(AssertUnwindSafe(|| {
                    MaintenanceRepository::fail_invalid(invalid.id, &invalid.error)
                }))
            {
                if !self.persistence_warning_emitted {
                    crate::diag::report_warning(format_args!(
                        "failed to quarantine invalid maintenance item {}: {error}",
                        invalid.id
                    ));
                    self.persistence_warning_emitted = true;
                }
                return PersistenceStatus::PersistenceFailed;
            }
            self.persistence_warning_emitted = false;
        }

        for item in batch.tasks {
            let item = Arc::new(item);
            let item_id = item.id;
            match self.actors.dispatch(Arc::clone(&item)) {
                Ok(_) => {
                    self.in_flight.insert(item_id, item);
                }
                Err(_) => break,
            }
        }
        PersistenceStatus::Complete
    }

    fn shutdown(&mut self) {
        self.actors.request_shutdown();
        let deadline = Instant::now() + gucs::shutdown_timeout();
        let mut persistence_failed = false;
        while Instant::now() < deadline && !self.actors.all_finished() {
            self.collect_actor_results();
            if !persistence_failed
                && self.apply_pending_results()
                    == PersistenceStatus::PersistenceFailed
            {
                persistence_failed = true;
            }
            self.actors.join_finished();
            BackendLatch::teardown_tick(Duration::from_millis(20))
                .exit_on_postmaster_death();
        }
        self.collect_actor_results();
        let _ = self.apply_pending_results();
        self.actors.join_finished();
        if !self.actors.all_finished() {
            crate::diag::report_warning(
                "maintenance actors did not stop before shutdown deadline",
            );
        }
    }
}

fn actor_runtime_config() -> ActorRuntimeConfig {
    ActorRuntimeConfig {
        page_size: gucs::batch_items(),
        request_timeout: gucs::request_timeout(),
    }
}

unsafe fn process_config_reload() {
    unsafe {
        (&raw mut pg_sys::ConfigReloadPending)
            .write_volatile(0 as pg_sys::sig_atomic_t);
        pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
    }
}
