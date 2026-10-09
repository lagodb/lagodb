//! Background worker main-thread supervisor.
//!
//! PostgreSQL FFI, shared-state reporting, lifecycle sweeps and log draining
//! stay on this thread. Owned Rust tasks perform asynchronous reconciliation,
//! startup and serving while the main loop remains responsive to PG signals.

use std::fmt::Arguments;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use pgrx::bgworkers::BackgroundWorker;
use pgrx::{PgTryBuilder, pg_sys};
use tokio::runtime::{Builder, Runtime};

use lagodb_core::diag::PgReportError;
use lagodb_core::pg_latch::BackendLatch;
use lagodb_storage::{ManagedStoreRegistry, StorageRuntime};

use super::catalog::VolumeConfigSource;
use super::config::StorageWorkerConfig;
use super::error::StorageWorkerError;
use super::logging::{self, PgLogBridge};
use super::reload::{ReconcilePhase, StorageReconciler, SupervisorReloadState};
use super::state::StorageStatusStore;
use super::tasks::{StorageTaskEvent, StorageWorkerTasks};
use super::volume_config::StorageVolumeConfigStore;

pub struct StorageWorkerSupervisor {
    config: StorageWorkerConfig,
    log_bridge: PgLogBridge,
}

impl StorageWorkerSupervisor {
    /// Snapshot GUCs and install the tracing subscriber on the bgworker main thread.
    pub fn from_gucs() -> Self {
        let config = StorageWorkerConfig::from_gucs();
        let (log_bridge, log_writer) =
            logging::new_bridge(config.startup.log_channel_capacity);

        unsafe { set_worker_log_min_messages() };
        if !logging::install_tracing_subscriber(log_writer) {
            logging::emit_pg_log(
                pg_sys::WARNING as i32,
                "global tracing subscriber already installed; \
                 storage logs may not use PG log bridge",
            );
        }
        Self { config, log_bridge }
    }

    /// Run until shutdown or task failure, then make a bounded attempt to drain
    /// Rust resources before PostgreSQL terminates this worker process.
    pub fn run(mut self) -> Result<(), PgReportError> {
        BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);
        // Bootstrap has no running tasks. Construct the Tokio runtime last so
        // every bootstrap error can return without asynchronous teardown.
        let startup = (|| -> Result<_, StorageWorkerError> {
            let config_store = StorageVolumeConfigStore::for_current_data_directory();
            config_store
                .initialize_if_missing()
                .map_err(StorageWorkerError::ConfigInitialization)?;
            let storage_runtime =
                StorageRuntime::new(self.config.runtime.storage.clone())
                    .map_err(StorageWorkerError::RuntimeConfig)?;
            let startup_sweep_pending = match config_store.try_sweep_due_volumes() {
                Ok(Some(())) => false,
                Ok(None) => true,
                Err(error) => {
                    self.record_error(format_args!(
                        "storage volume lifecycle sweep failed at startup: {}",
                        error.diagnostic_message(),
                    ));
                    false
                }
            };
            let runtime = Builder::new_multi_thread()
                .worker_threads(self.config.startup.worker_threads)
                .enable_all()
                .build()
                .map_err(StorageWorkerError::RuntimeBuild)?;
            Ok((
                config_store,
                storage_runtime,
                runtime,
                startup_sweep_pending,
            ))
        })();
        let (config_store, storage_runtime, runtime, startup_sweep_pending) =
            match startup {
                Ok(startup) => startup,
                Err(error) => {
                    self.mark_failed(&error);
                    self.log_bridge.drain_to_pg_log();
                    return Err(error.into_report());
                }
            };
        let registry = ManagedStoreRegistry::new();
        let reconciler = StorageReconciler::new(
            VolumeConfigSource::new(config_store.clone()),
            registry.clone(),
        );
        let mut tasks = StorageWorkerTasks::new(registry, reconciler);
        tasks.request_reconcile(ReconcilePhase::Startup, false);
        let mut runtime_state = SupervisorReloadState::new(
            self.config.runtime.clone(),
            startup_sweep_pending,
        );
        // Borrow the resources across this boundary: unwinding the loop must
        // not drop the runtime before explicit shutdown. AssertUnwindSafe is
        // appropriate because a captured failure only leads to teardown;
        // partially updated supervisor state is never used to resume serving.
        let outcome = PgTryBuilder::new(AssertUnwindSafe(|| {
            self.run_supervisor_loop(
                &runtime,
                &mut tasks,
                &config_store,
                &storage_runtime,
                &mut runtime_state,
                startup_sweep_pending,
            )
        }))
        .catch_others(|error| {
            Err(StorageWorkerError::Postgres(PgReportError::from_caught(
                error,
            )))
        })
        .execute();
        self.shutdown(runtime, tasks, runtime_state.shutdown_timeout(), &outcome);
        // PostgreSQL's worker boundary reports once and classifies ERROR as a
        // nonzero exit. Neither task failures nor captured PG errors exit here.
        outcome.map_err(StorageWorkerError::into_report)
    }

    fn run_supervisor_loop(
        &mut self,
        runtime: &Runtime,
        tasks: &mut StorageWorkerTasks,
        config_store: &StorageVolumeConfigStore,
        storage_runtime: &StorageRuntime,
        runtime_state: &mut SupervisorReloadState,
        mut startup_sweep_pending: bool,
    ) -> Result<(), StorageWorkerError> {
        // Check PG lifecycle state before dispatching even the initial task.
        let mut timeout = Duration::ZERO;
        loop {
            if !BackgroundWorker::wait_latch(Some(timeout)) {
                return Ok(());
            }
            self.log_bridge.drain_to_pg_log();
            if BackgroundWorker::sighup_received() {
                unsafe { SupervisorReloadState::reload_postgres_config() };
                runtime_state.reload_from_gucs(storage_runtime);
                tasks.request_reconcile(ReconcilePhase::Sighup, true);
            }

            // Handle completed tasks before attempting lifecycle maintenance.
            let result = tasks.poll();
            self.handle_task_result(result, runtime, tasks, storage_runtime)?;

            let now = Instant::now();
            if runtime_state.volume_sweep_due(now) {
                // This mutation acquires a PG advisory lock, so it cannot move
                // into the Rust management task. Keep a contended sweep due so
                // the existing supervisor tick retries it before reconciliation.
                let phase = if startup_sweep_pending {
                    "startup"
                } else {
                    "periodic"
                };
                let sweep_completed = match config_store.try_sweep_due_volumes() {
                    Ok(Some(())) => true,
                    Ok(None) => false,
                    Err(error) => {
                        self.record_error(format_args!(
                            "storage volume lifecycle sweep failed at {phase}: {}",
                            error.diagnostic_message(),
                        ));
                        true
                    }
                };
                if sweep_completed {
                    if !startup_sweep_pending {
                        tasks.request_reconcile(ReconcilePhase::Periodic, false);
                    }
                    startup_sweep_pending = false;
                    runtime_state.schedule_volume_sweep(now);
                }
            }
            if let Some(force) = StorageStatusStore::new().take_reload_request() {
                tasks.request_reconcile(ReconcilePhase::Runtime, force);
            }

            // Dispatch after the sweep attempt so reconciliation reads the
            // latest snapshot.
            if !startup_sweep_pending {
                let result = tasks.dispatch_reconcile(runtime).map(|()| None);
                self.handle_task_result(result, runtime, tasks, storage_runtime)?;
            }
            timeout = runtime_state.wait_timeout();
        }
    }

    fn handle_task_result(
        &mut self,
        result: Result<Option<StorageTaskEvent>, StorageWorkerError>,
        runtime: &Runtime,
        tasks: &mut StorageWorkerTasks,
        storage_runtime: &StorageRuntime,
    ) -> Result<(), StorageWorkerError> {
        let event = match result {
            Ok(Some(event)) => event,
            Ok(None) => return Ok(()),
            Err(error) if error.is_fatal() => return Err(error),
            Err(error) => {
                self.record_error(format_args!("{}", error.diagnostic_message()));
                return Ok(());
            }
        };
        match event {
            StorageTaskEvent::Reconciled { phase, report } => {
                SupervisorReloadState::report_reconcile(phase, &report);
                StorageStatusStore::new().mark_reload(&report);
                if phase == ReconcilePhase::Startup {
                    tasks.start_server(
                        runtime,
                        &self.config.startup,
                        storage_runtime.clone(),
                    );
                }
            }
            StorageTaskEvent::ServerReady => {
                // Readiness is a PG lifecycle decision, never a side effect of
                // collecting a task result. The server stays on its Rust task.
                tasks.serve();
                StorageStatusStore::new().mark_running();
                logging::emit_pg_log(
                    pg_sys::INFO as i32,
                    "storage background worker started",
                );
            }
            StorageTaskEvent::ServerExited => {
                return Err(StorageWorkerError::ServerExited);
            }
        }
        Ok(())
    }

    /// Share one wait budget between task draining and runtime shutdown.
    /// The budget starts here, after shutdown is accepted. A spontaneous server
    /// failure is intentionally collected only after its task releases resources;
    /// preceding release time is outside this budget (see `ServerTask::run`).
    /// Server release stays on the runtime, including synchronous redb close.
    /// Once the budget expires, stop waiting and let process exit terminate
    /// unfinished work; do not wait indefinitely for a normal database close.
    fn shutdown(
        &mut self,
        runtime: Runtime,
        mut tasks: StorageWorkerTasks,
        timeout: Duration,
        outcome: &Result<(), StorageWorkerError>,
    ) {
        let deadline = Instant::now() + timeout;
        tasks.request_shutdown();
        // wait_latch reports either SIGTERM or postmaster death. Before any
        // final PG reporting, distinguish death using the existing teardown wait.
        BackendLatch::teardown_tick(Duration::ZERO).exit_on_postmaster_death();
        match outcome {
            Ok(()) => {
                StorageStatusStore::new().mark_stopping();
                logging::emit_pg_log(
                    pg_sys::INFO as i32,
                    "storage background worker shutdown requested",
                );
            }
            Err(error) => self.mark_failed(error),
        }
        self.wait_for_tasks(&mut tasks, deadline, outcome);
        // Handles return only reports/errors, never the server or its index.
        // Runtime shutdown and process exit terminate accepted connections and
        // blocking operations that did not finish within the remaining budget.
        drop(tasks);
        runtime.shutdown_timeout(deadline.saturating_duration_since(Instant::now()));
        self.log_bridge.drain_until(deadline);
        logging::emit_pg_log(
            pg_sys::INFO as i32,
            "storage background worker stopped",
        );
    }

    fn wait_for_tasks(
        &mut self,
        tasks: &mut StorageWorkerTasks,
        deadline: Instant,
        outcome: &Result<(), StorageWorkerError>,
    ) {
        while !tasks.is_drained() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let message = "storage tasks did not stop before shutdown timeout; forcing runtime shutdown";
                if outcome.is_ok() {
                    StorageStatusStore::new().record_error(message);
                }
                logging::emit_pg_log(pg_sys::WARNING as i32, message);
                break;
            }
            self.log_bridge.drain_until(deadline);
            match tasks.poll() {
                Ok(Some(_)) => continue,
                Err(error) if error.is_cancelled() => continue,
                Err(error) => {
                    let message = format!(
                        "storage task failed during shutdown: {}",
                        error.diagnostic_message(),
                    );
                    // An accepted stop remains a stop. Preserve the first
                    // fatal outcome when teardown reports additional errors.
                    if outcome.is_ok() {
                        StorageStatusStore::new().record_error(&message);
                    }
                    logging::emit_pg_log(pg_sys::WARNING as i32, &message);
                    continue;
                }
                Ok(None) => {}
            }
            BackendLatch::teardown_tick(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(50)),
            )
            .exit_on_postmaster_death();
        }
    }

    fn record_error(&self, args: Arguments<'_>) {
        let message = args.to_string();
        StorageStatusStore::new().record_error(&message);
        logging::emit_pg_log(pg_sys::WARNING as i32, &message);
    }

    /// Publish the first fatal failure before teardown. Its structured report
    /// is returned to the worker entrypoint after resources have shut down.
    fn mark_failed(&self, error: &StorageWorkerError) {
        StorageStatusStore::new().mark_failed(&error.diagnostic_message());
    }
}

/// Set `log_min_messages` to INFO for this worker process so that INFO-level
/// storage logs are visible in the PostgreSQL log.  This mirrors how the Neon
/// communicator process configures its own log verbosity.
///
/// Only affects the bgworker process; other backends are unaffected.
unsafe fn set_worker_log_min_messages() {
    unsafe {
        pg_sys::SetConfigOption(
            c"log_min_messages".as_ptr(),
            c"INFO".as_ptr(),
            pg_sys::GucContext::PGC_SUSET,
            pg_sys::GucSource::PGC_S_OVERRIDE,
        );
    }
}
