//! Background worker main-thread supervisor.
//!
//! Owns the Tokio runtime, the storage server task, the PG log bridge, and the
//! storage-volume registry reconciler. The main thread loop handles signals,
//! drains logs, and drives reconcile cycles.
//!
//! All PostgreSQL FFI is confined to this thread. The Tokio runtime only sees
//! plain Rust values and the `StoreRegistry` (which is internally a
//! synchronized handle that is safe to share across threads).

use std::time::{Duration, Instant};

use pgrx::bgworkers::BackgroundWorker;
use pgrx::pg_sys;
use tokio_util::sync::CancellationToken;

use pg_lakebase_core::pg_latch::BackendLatch;
use pg_lakebase_storage::{StorageRuntime, StoreRegistry};

use super::catalog::VolumeConfigSource;
use super::config::StorageWorkerConfig;
use super::logging::{self, PgLogBridge};
use super::reload::{StorageReconciler, SupervisorReloadState};
use super::state::StorageStatusStore;
use super::volume_config::StorageVolumeConfigStore;

type ServerTask = tokio::task::JoinHandle<pg_lakebase_storage::StorageResult<()>>;
pub struct StorageWorkerSupervisor {
    config: StorageWorkerConfig,
    log_bridge: PgLogBridge,
    shutdown: CancellationToken,
}

impl StorageWorkerSupervisor {
    /// Build a supervisor by snapshotting GUCs and installing the tracing subscriber.
    ///
    /// Must be called from the bgworker main thread.
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

        Self {
            config,
            log_bridge,
            shutdown: CancellationToken::new(),
        }
    }

    /// Run the storage server until SIGTERM or server-task failure.
    ///
    /// This method does not return until the bgworker is ready to exit.
    pub fn run(mut self) {
        // Establish a normal PostgreSQL backend environment for GUC reload,
        // timestamps and shared-state reporting. Volume desired state still
        // comes exclusively from the machine-managed config file.
        BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

        let config_store = StorageVolumeConfigStore::for_current_data_directory();
        if let Err(error) = config_store.initialize_if_missing() {
            let message = format!(
                "storage volume config initialization failed: {}",
                error.diagnostic_message(),
            );
            StorageStatusStore::new().mark_failed(&message);
            logging::emit_pg_log(pg_sys::PGERROR as i32, &message);
            self.log_bridge.drain_to_pg_log();
            unsafe { pg_sys::proc_exit(1) };
        }

        let runtime = self.build_runtime();
        let registry = StoreRegistry::new();
        let mut reconciler = Self::build_reconciler(registry.clone(), config_store);
        self.initial_reconcile_or_exit(&mut reconciler);

        let storage_runtime = self.storage_runtime_or_exit();
        let storage_runtime_control = storage_runtime.clone();
        let mut server_handle =
            Some(self.spawn_storage_server(&runtime, registry, storage_runtime));

        logging::emit_pg_log(
            pg_sys::INFO as i32,
            "storage background worker started",
        );

        let mut runtime_state =
            SupervisorReloadState::new(self.config.runtime.clone());
        StorageStatusStore::new().mark_running();
        self.run_supervisor_loop(
            &runtime,
            &mut server_handle,
            &mut reconciler,
            &storage_runtime_control,
            &mut runtime_state,
        );
        self.shutdown_storage_server(
            runtime,
            &mut server_handle,
            runtime_state.shutdown_timeout(),
        );
    }

    fn build_runtime(&self) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.config.startup.worker_threads)
            .enable_all()
            .build()
            .expect("failed to create pg-lakebase-storage tokio runtime")
    }

    fn build_reconciler(
        registry: StoreRegistry,
        config_store: StorageVolumeConfigStore,
    ) -> StorageReconciler {
        StorageReconciler::new(VolumeConfigSource::new(config_store), registry)
    }

    fn initial_reconcile_or_exit(&mut self, reconciler: &mut StorageReconciler) {
        match SupervisorReloadState::reconcile(reconciler, "startup", false) {
            Ok(report) => StorageStatusStore::new().mark_reload(&report),
            Err(error) => {
                let message = format!(
                    "storage worker startup reconcile failed: {}",
                    error.diagnostic_message(),
                );
                StorageStatusStore::new().mark_failed(&message);
                logging::emit_pg_log(pg_sys::PGERROR as i32, &message);
                self.log_bridge.drain_to_pg_log();
                unsafe { pg_sys::proc_exit(1) };
            }
        }
    }

    fn storage_runtime_or_exit(&mut self) -> StorageRuntime {
        match StorageRuntime::new(self.config.runtime.storage.clone()) {
            Ok(rt) => rt,
            Err(error) => {
                let message = format!(
                    "storage runtime config invalid, worker cannot start: {error}",
                );
                StorageStatusStore::new().mark_failed(&message);
                logging::emit_pg_log(pg_sys::WARNING as i32, &message);
                self.log_bridge.drain_to_pg_log();
                unsafe { pg_sys::proc_exit(1) };
            }
        }
    }

    fn spawn_storage_server(
        &self,
        runtime: &tokio::runtime::Runtime,
        registry: StoreRegistry,
        storage_runtime: StorageRuntime,
    ) -> ServerTask {
        let shutdown = self.shutdown.clone();
        let startup_config = &self.config.startup;

        let socket_path = startup_config.socket_path.clone();
        let cache_dir = startup_config.cache_dir.clone();
        let server_config = startup_config.server_config.clone();
        let service_config = startup_config.service_config.clone();

        runtime.spawn(async move {
            let server = pg_lakebase_storage::StorageServerBuilder::new(
                &socket_path,
                &cache_dir,
            )
            .with_server_config(server_config)
            .with_service_config(service_config.with_externally_managed_registry())
            .with_store_registry(registry)
            .with_runtime(storage_runtime)
            .with_tracing_request_observer()
            .bind()
            .await?;

            server.serve_until(shutdown).await
        })
    }

    fn run_supervisor_loop(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        server_handle: &mut Option<ServerTask>,
        reconciler: &mut StorageReconciler,
        storage_runtime: &StorageRuntime,
        runtime_state: &mut SupervisorReloadState,
    ) {
        loop {
            self.log_bridge.drain_to_pg_log();
            self.exit_if_server_finished(runtime, server_handle);

            let reload_request = StorageStatusStore::new().take_reload_request();
            if reload_request.is_some()
                || runtime_state.periodic_reconcile_due(Instant::now())
            {
                match SupervisorReloadState::reconcile(
                    reconciler,
                    "runtime",
                    reload_request.unwrap_or(false),
                ) {
                    Ok(report) => {
                        StorageStatusStore::new().mark_reload(&report);
                    }
                    Err(error) => {
                        // Snapshot-level runtime failures are non-fatal: keep
                        // the current registry and re-read on the next reload
                        // request or periodic timer. Per-Volume apply failures
                        // are isolated inside a successful report.
                        let message = format!(
                            "storage worker reconcile failed: {}",
                            error.diagnostic_message(),
                        );
                        StorageStatusStore::new().record_error(&message);
                        logging::emit_pg_log(pg_sys::WARNING as i32, &message);
                    }
                }

                runtime_state.schedule_next_reconcile(Instant::now());
            }

            let timeout = runtime_state.wait_timeout();
            let should_continue = BackgroundWorker::wait_latch(Some(timeout));

            if BackgroundWorker::sighup_received() {
                unsafe { SupervisorReloadState::reload_postgres_config() };
                runtime_state.reload_from_gucs(storage_runtime, Instant::now());
                match SupervisorReloadState::reconcile(reconciler, "SIGHUP", true) {
                    Ok(report) => StorageStatusStore::new().mark_reload(&report),
                    Err(error) => {
                        let message = format!(
                            "storage worker SIGHUP reload failed: {}",
                            error.diagnostic_message(),
                        );
                        StorageStatusStore::new().record_error(&message);
                        logging::emit_pg_log(pg_sys::WARNING as i32, &message);
                    }
                }
            }

            if !should_continue {
                logging::emit_pg_log(
                    pg_sys::INFO as i32,
                    "storage background worker shutdown requested",
                );
                StorageStatusStore::new().mark_stopping();
                self.shutdown.cancel();
                break;
            }
        }
    }

    fn exit_if_server_finished(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        server_handle: &mut Option<ServerTask>,
    ) {
        if !server_handle.as_ref().is_some_and(|h| h.is_finished()) {
            return;
        }

        let handle = server_handle.take().unwrap();
        self.shutdown.cancel();

        match runtime.block_on(handle) {
            Ok(Ok(())) => {
                StorageStatusStore::new()
                    .mark_failed("storage server exited unexpectedly");
                logging::emit_pg_log(pg_sys::INFO as i32, "storage server exited")
            }
            Ok(Err(e)) => {
                let message = format!("storage server failed: {e}");
                StorageStatusStore::new().mark_failed(&message);
                logging::emit_pg_log(pg_sys::PGERROR as i32, &message);
            }
            Err(e) => {
                let message = format!("storage server task panicked: {e}");
                StorageStatusStore::new().mark_failed(&message);
                logging::emit_pg_log(pg_sys::PGERROR as i32, &message);
            }
        }

        self.log_bridge.drain_to_pg_log();
        unsafe { pg_sys::proc_exit(1) };
    }

    fn shutdown_storage_server(
        &mut self,
        runtime: tokio::runtime::Runtime,
        server_handle: &mut Option<ServerTask>,
        shutdown_timeout: Duration,
    ) {
        let deadline = Instant::now() + shutdown_timeout;
        self.wait_for_server_shutdown(&runtime, server_handle, deadline);
        let runtime_budget = deadline
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(50));
        runtime.shutdown_timeout(runtime_budget);
        self.log_bridge.drain_to_pg_log();

        logging::emit_pg_log(
            pg_sys::INFO as i32,
            "storage background worker stopped",
        );
    }

    fn wait_for_server_shutdown(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        server_handle: &mut Option<ServerTask>,
        deadline: Instant,
    ) {
        while Instant::now() < deadline {
            self.log_bridge.drain_to_pg_log();

            if server_handle.as_ref().is_some_and(|h| h.is_finished()) {
                let handle = server_handle.take().unwrap();
                match runtime.block_on(handle) {
                    Ok(Ok(())) => logging::emit_pg_log(
                        pg_sys::INFO as i32,
                        "storage server stopped",
                    ),
                    Ok(Err(e)) => logging::emit_pg_log(
                        pg_sys::PGERROR as i32,
                        &record_storage_error(format_args!(
                            "storage server failed during shutdown: {e}"
                        )),
                    ),
                    Err(e) => logging::emit_pg_log(
                        pg_sys::PGERROR as i32,
                        &record_storage_error(format_args!(
                            "storage server task panicked during shutdown: {e}"
                        )),
                    ),
                }
                return;
            }

            BackendLatch::teardown_tick(Duration::from_millis(50))
                .exit_on_postmaster_death();
        }

        logging::emit_pg_log(
            pg_sys::WARNING as i32,
            &record_storage_error(format_args!(
                "storage server did not stop before shutdown timeout; forcing runtime shutdown"
            )),
        );
    }
}

fn record_storage_error(args: std::fmt::Arguments<'_>) -> String {
    let message = args.to_string();
    StorageStatusStore::new().record_error(&message);
    message
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
