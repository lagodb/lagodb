//! PostgreSQL-facing supervisor for the bounded maintenance actor pool.

use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;

use super::actor::{ActorResult, ActorRuntimeConfig, MaintenanceActorPool};
use super::gucs;
use super::item::{MaintenanceItem, MaintenanceItemId};
use super::repository::{MaintenanceRepository, QueuePoll};
use super::runner::MaintenanceExecutionOutcome;
use crate::worker::storage;

/// Initialize GUCs and register the single designated maintenance process.
pub fn init_worker_host(library_name: &str) {
    gucs::init();
    if !gucs::enabled() {
        return;
    }

    let _keep =
        pg_lakebase_maintenance_bgworker_main as extern "C-unwind" fn(pg_sys::Datum);
    BackgroundWorkerBuilder::new("pg-lakebase-maintenance")
        .set_type("pg-lakebase-maintenance")
        .set_library(library_name)
        .set_function("pg_lakebase_maintenance_bgworker_main")
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_lakebase_maintenance_bgworker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );
    MaintenanceWorkerSupervisor::from_gucs().run();
}

struct MaintenanceWorkerSupervisor {
    actors: MaintenanceActorPool,
    in_flight: HashMap<MaintenanceItemId, Arc<MaintenanceItem>>,
    pending_results: VecDeque<ActorResult>,
    catalog_warning_emitted: bool,
    update_warning_emitted: bool,
}

impl MaintenanceWorkerSupervisor {
    fn from_gucs() -> Self {
        let actors = MaintenanceActorPool::start(
            gucs::actor_threads(),
            storage::resolved_socket_path(),
            actor_runtime_config(),
        )
        .unwrap_or_else(|error| {
            crate::diag::report_warning(&format!(
                "failed to start maintenance actor pool: {error}"
            ));
            exit_for_postmaster_restart()
        });
        Self {
            actors,
            in_flight: HashMap::new(),
            pending_results: VecDeque::new(),
            catalog_warning_emitted: false,
            update_warning_emitted: false,
        }
    }

    fn run(mut self) {
        let database = gucs::database();
        BackgroundWorker::connect_worker_to_spi(Some(&database), None);
        crate::diag::report_info("maintenance background worker started");
        let mut restart_after_shutdown = false;

        loop {
            self.collect_actor_results();
            let database_healthy = self.apply_pending_results();

            if self.actors.has_finished_actor() {
                crate::diag::report_warning(
                    "maintenance actor exited unexpectedly; restarting maintenance worker",
                );
                restart_after_shutdown = true;
                break;
            }

            if database_healthy {
                self.dispatch_ready_tasks();
            }

            let should_continue =
                BackgroundWorker::wait_latch(Some(self.wait_timeout()));
            if BackgroundWorker::sighup_received() {
                unsafe { process_config_reload() };
                self.actors.reload(actor_runtime_config());
            }
            if !should_continue {
                break;
            }
        }

        self.shutdown();
        if restart_after_shutdown {
            crate::diag::report_warning(
                "maintenance background worker exiting with failure status for postmaster restart",
            );
            exit_for_postmaster_restart();
        }
        crate::diag::report_info("maintenance background worker stopped");
    }

    fn collect_actor_results(&mut self) {
        while let Some(result) = self.actors.try_result() {
            self.pending_results.push_back(result);
        }
    }

    fn apply_pending_results(&mut self) -> bool {
        while let Some(result) = self.pending_results.front() {
            let Some(item) = self.in_flight.get(&result.item_id) else {
                crate::diag::report_warning(&format!(
                    "maintenance actor returned unknown item {}",
                    result.item_id
                ));
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
                if !self.update_warning_emitted {
                    crate::diag::report_warning(&format!(
                        "failed to persist maintenance result: {error}"
                    ));
                    self.update_warning_emitted = true;
                }
                return false;
            }

            self.update_warning_emitted = false;
            let completed =
                matches!(&result.outcome, MaintenanceExecutionOutcome::Complete);
            let item_id = result.item_id;
            self.pending_results.pop_front();
            if let Some(item) = self.in_flight.remove(&item_id)
                && completed
            {
                crate::diag::log_debug1(&format!(
                    "maintenance item completed: item_id={} operation={:?} producer={}",
                    item.id,
                    item.target.operation(),
                    item.producer,
                ));
            }
        }
        true
    }

    fn dispatch_ready_tasks(&mut self) {
        let capacity = self.actors.capacity();
        if capacity == 0 {
            return;
        }
        let in_flight: HashSet<_> = self.in_flight.keys().copied().collect();
        let poll = BackgroundWorker::transaction(AssertUnwindSafe(|| {
            MaintenanceRepository::fetch_ready_batch(capacity, &in_flight)
        }));
        let batch = match poll {
            Ok(QueuePoll::Ready(batch)) => {
                self.catalog_warning_emitted = false;
                batch
            }
            Ok(QueuePoll::Unavailable) => {
                if !self.catalog_warning_emitted {
                    crate::diag::report_warning(
                        "maintenance queue is not installed in the configured database",
                    );
                    self.catalog_warning_emitted = true;
                }
                return;
            }
            Err(error) => {
                if !self.catalog_warning_emitted {
                    crate::diag::report_warning(&format!(
                        "maintenance queue unavailable: {error}"
                    ));
                    self.catalog_warning_emitted = true;
                }
                return;
            }
        };

        for invalid in batch.invalid {
            let result = BackgroundWorker::transaction(AssertUnwindSafe(|| {
                MaintenanceRepository::fail_invalid(invalid.id, &invalid.error)
            }));
            if let Err(error) = result {
                crate::diag::report_warning(&format!(
                    "failed to quarantine invalid maintenance item {}: {error}",
                    invalid.id
                ));
                return;
            }
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
    }

    fn wait_timeout(&self) -> Duration {
        if self.in_flight.is_empty() && self.pending_results.is_empty() {
            gucs::naptime()
        } else {
            Duration::from_millis(50)
        }
    }

    fn shutdown(&mut self) {
        self.actors.request_shutdown();
        let deadline = Instant::now() + gucs::shutdown_timeout();
        while Instant::now() < deadline && !self.actors.all_finished() {
            self.collect_actor_results();
            let _ = self.apply_pending_results();
            self.actors.join_finished();
            std::thread::sleep(Duration::from_millis(20));
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

fn exit_for_postmaster_restart() -> ! {
    // SAFETY: this is a PostgreSQL background worker process. A non-zero
    // proc_exit status is the documented signal that the postmaster should use
    // the registered bgw_restart_time instead of treating the worker as
    // intentionally terminated.
    unsafe { pg_sys::proc_exit(1) }
}

unsafe fn process_config_reload() {
    unsafe {
        (&raw mut pg_sys::ConfigReloadPending)
            .write_volatile(0 as pg_sys::sig_atomic_t);
        pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
    }
}
