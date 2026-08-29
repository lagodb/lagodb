//! Database-local, on-demand object-cleanup worker supervisor.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lagodb_storage::StorageError;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use super::actor::{ActorResult, ActorRuntimeConfig, ObjectCleanupActorPool};
use super::item::{ObjectCleanupItem, ObjectCleanupItemId};
use super::repository::{ObjectCleanupRepository, QueuePoll};
use super::runner::ObjectCleanupExecutionOutcome;
use crate::extension_worker::{WorkerContext, WorkerSchedule, WorkerTransaction};
use crate::maintenance_config::MaintenanceSettings;
use crate::pg_latch::BackendLatch;
use crate::storage::service::StorageEndpoint;

const RESULT_PERSISTENCE_RETRY: Duration = Duration::from_secs(5);

/// Drain object-cleanup work in the worker's already-connected database.
pub fn run_object_cleanup_worker(
    worker_context: &WorkerContext<'_>,
) -> WorkerSchedule {
    worker_context.process_config_reload_if_pending();
    let settings = MaintenanceSettings::load();
    if !settings.enabled() {
        return WorkerSchedule::Idle;
    }

    match current_schedule() {
        Schedule::Idle => return WorkerSchedule::Idle,
        Schedule::RunAfter(delay) => {
            return WorkerSchedule::RunAfter(delay);
        }
        Schedule::Ready => {}
    }

    let mut supervisor =
        match ObjectCleanupWorkerSupervisor::from_runtime_settings(settings) {
            Ok(supervisor) => supervisor,
            Err(error) => {
                return error.worker_schedule();
            }
        };
    crate::diag::report_info("database-local maintenance worker started");
    let schedule = supervisor.run_until_idle(worker_context);
    supervisor.shutdown();
    crate::diag::report_info("database-local maintenance worker stopped");
    schedule
}

enum Schedule {
    Ready,
    Idle,
    RunAfter(Duration),
}

fn current_schedule() -> Schedule {
    let poll = WorkerTransaction::run(|| {
        ObjectCleanupRepository::fetch_ready_batch(1, &HashSet::new())
    });
    match poll {
        Ok(QueuePoll::Ready(batch))
            if !batch.tasks.is_empty() || !batch.invalid.is_empty() =>
        {
            Schedule::Ready
        }
        Ok(QueuePoll::Ready(_)) => next_schedule(),
        Ok(QueuePoll::Unavailable) => Schedule::Idle,
        Err(error) => {
            crate::diag::report_warning(format_args!(
                "failed to inspect maintenance queue: {error}"
            ));
            Schedule::RunAfter(Duration::from_secs(5))
        }
    }
}

fn next_schedule() -> Schedule {
    let next = WorkerTransaction::run(ObjectCleanupRepository::next_pending_at);
    match next {
        Ok(Some(timestamp)) => {
            let now = unsafe { pg_sys::GetCurrentTimestamp() };
            if timestamp <= now {
                Schedule::Ready
            } else {
                let micros =
                    u64::try_from(timestamp.saturating_sub(now)).unwrap_or(u64::MAX);
                Schedule::RunAfter(Duration::from_micros(micros))
            }
        }
        Ok(None) => Schedule::Idle,
        Err(error) => {
            crate::diag::report_warning(format_args!(
                "failed to schedule maintenance queue: {error}"
            ));
            Schedule::RunAfter(Duration::from_secs(5))
        }
    }
}

struct ObjectCleanupWorkerSupervisor {
    actors: ObjectCleanupActorPool,
    in_flight: HashMap<ObjectCleanupItemId, Arc<ObjectCleanupItem>>,
    pending_results: VecDeque<ActorResult>,
    persistence_warning_emitted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistenceStatus {
    Complete,
    PersistenceFailed,
}

#[derive(Debug, thiserror::Error)]
enum ObjectCleanupWorkerStartupError {
    #[error("failed to start maintenance actor pool: {0}")]
    ActorPool(#[source] std::io::Error),

    #[error("storage server is disabled")]
    StorageDisabled,

    #[error("failed to resolve storage endpoint: {0}")]
    StorageConfig(#[source] StorageError),
}

impl ObjectCleanupWorkerStartupError {
    fn worker_schedule(&self) -> WorkerSchedule {
        match self {
            Self::StorageDisabled => {
                crate::diag::report_warning(
                    "database-local maintenance worker skipped: storage server is disabled",
                );
                WorkerSchedule::Idle
            }
            Self::StorageConfig(_) => {
                crate::diag::report_warning(format_args!(
                    "database-local maintenance worker waiting for storage: {self}"
                ));
                WorkerSchedule::RunAfter(Duration::from_secs(5))
            }
            Self::ActorPool(_) => {
                crate::diag::report_warning(format_args!(
                    "database-local maintenance worker startup failed: {self}"
                ));
                WorkerSchedule::RunAfter(Duration::from_secs(5))
            }
        }
    }
}

impl ObjectCleanupWorkerSupervisor {
    fn from_runtime_settings(
        settings: MaintenanceSettings,
    ) -> Result<Self, ObjectCleanupWorkerStartupError> {
        let endpoint = StorageEndpoint::from_pg_gucs()
            .map_err(ObjectCleanupWorkerStartupError::StorageConfig)?;
        if !endpoint.is_enabled() {
            return Err(ObjectCleanupWorkerStartupError::StorageDisabled);
        }

        let runtime_config = actor_runtime_config(settings);
        let actors = ObjectCleanupActorPool::start(
            settings.actor_threads(),
            endpoint.socket_path().to_path_buf(),
            runtime_config,
        )
        .map_err(ObjectCleanupWorkerStartupError::ActorPool)?;
        Ok(Self {
            actors,
            in_flight: HashMap::new(),
            pending_results: VecDeque::new(),
            persistence_warning_emitted: false,
        })
    }

    fn run_until_idle(
        &mut self,
        worker_context: &WorkerContext<'_>,
    ) -> WorkerSchedule {
        loop {
            self.collect_actor_results();
            if self.apply_pending_results() == PersistenceStatus::PersistenceFailed {
                return WorkerSchedule::RunAfter(RESULT_PERSISTENCE_RETRY);
            }

            if self.actors.has_finished_actor() {
                crate::diag::report_warning("maintenance actor exited unexpectedly");
                return WorkerSchedule::RunAfter(Duration::from_secs(5));
            }

            if self.dispatch_ready_tasks() == PersistenceStatus::PersistenceFailed {
                return WorkerSchedule::RunAfter(RESULT_PERSISTENCE_RETRY);
            }

            if self.in_flight.is_empty() && self.pending_results.is_empty() {
                match next_schedule() {
                    Schedule::Ready => continue,
                    Schedule::Idle => return WorkerSchedule::Idle,
                    Schedule::RunAfter(delay) => {
                        return WorkerSchedule::RunAfter(delay);
                    }
                }
            }

            if !BackgroundWorker::wait_latch(Some(Duration::from_millis(50))) {
                return WorkerSchedule::Idle;
            }
            if worker_context.process_config_reload_if_pending() {
                self.actors
                    .reload(actor_runtime_config(MaintenanceSettings::load()));
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
            if matches!(&result.outcome, ObjectCleanupExecutionOutcome::Cancelled) {
                let item_id = result.item_id;
                self.pending_results.pop_front();
                self.in_flight.remove(&item_id);
                continue;
            }

            let update = WorkerTransaction::run(|| match &result.outcome {
                ObjectCleanupExecutionOutcome::Complete => {
                    ObjectCleanupRepository::complete(item.as_ref())
                }
                ObjectCleanupExecutionOutcome::Retryable(error) => {
                    ObjectCleanupRepository::retry(item.as_ref(), &error.to_string())
                }
                ObjectCleanupExecutionOutcome::Permanent(error) => {
                    ObjectCleanupRepository::fail(item.as_ref(), &error.to_string())
                }
                ObjectCleanupExecutionOutcome::Cancelled => {
                    unreachable!(
                        "cancelled cleanup outcomes are filtered out before persisting"
                    )
                }
            });
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
        let poll = WorkerTransaction::run(|| {
            ObjectCleanupRepository::fetch_ready_batch(capacity, &in_flight)
        });
        let batch = match poll {
            Ok(QueuePoll::Ready(batch)) => batch,
            Ok(QueuePoll::Unavailable) => return PersistenceStatus::Complete,
            Err(error) => {
                if !self.persistence_warning_emitted {
                    crate::diag::report_warning(format_args!(
                        "failed to fetch maintenance work: {error}"
                    ));
                    self.persistence_warning_emitted = true;
                }
                return PersistenceStatus::PersistenceFailed;
            }
        };

        for invalid in batch.invalid {
            if let Err(error) = WorkerTransaction::run(|| {
                ObjectCleanupRepository::fail_invalid(invalid.id, &invalid.error)
            }) {
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
        let deadline =
            Instant::now() + MaintenanceSettings::load().shutdown_timeout();
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

fn actor_runtime_config(settings: MaintenanceSettings) -> ActorRuntimeConfig {
    ActorRuntimeConfig {
        page_size: settings.batch_items(),
        request_timeout: settings.request_timeout(),
    }
}
