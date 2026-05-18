//! Background worker main-thread supervisor.
//!
//! Owns the Tokio runtime, the storage server task, the PG log bridge, and the
//! distributed-tablespace store reconciler. The main thread loop handles
//! signals, drains logs, and drives reconcile cycles.
//!
//! All PostgreSQL FFI is confined to this thread. The Tokio runtime only sees
//! plain Rust values and the `StoreRegistry` (which is internally a
//! synchronized handle that is safe to share across threads).

use std::time::{Duration, Instant};

use pgrx::bgworkers::BackgroundWorker;
use pgrx::pg_sys;
use tokio_util::sync::CancellationToken;

use pg_lakebase_storage::{StorageRuntime, StorageRuntimeConfig, StoreRegistry};

use super::catalog::{self, PgTablespaceStoreCatalog};
use super::config::{StorageWorkerConfig, StorageWorkerRuntimeConfig};
use super::logging::{self, PgLogBridge};
use super::reconciler::{ReconcileReport, StoreCatalogReconciler};

type ServerTask = tokio::task::JoinHandle<pg_lakebase_storage::StorageResult<()>>;
type StorageReconciler = StoreCatalogReconciler<PgTablespaceStoreCatalog>;

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
        BackgroundWorker::connect_worker_to_spi(None, None);

        let runtime = self.build_runtime();
        let registry = StoreRegistry::new();
        let mut reconciler = Self::build_reconciler(registry.clone());
        self.initial_reconcile_or_exit(&mut reconciler);

        // Clear any syscache dirty bit accumulated during connect / initial
        // reconcile so the first main-loop iteration does not redundantly
        // re-scan the catalog we just read.
        let _ = catalog::take_dirty();

        let storage_runtime = self.storage_runtime_or_exit();
        let storage_runtime_control = storage_runtime.clone();
        let mut server_handle =
            Some(self.spawn_storage_server(&runtime, registry, storage_runtime));

        logging::emit_pg_log(
            pg_sys::INFO as i32,
            "storage background worker started",
        );

        let mut runtime_state =
            SupervisorRuntimeState::new(self.config.runtime.clone());
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
            runtime_state.config.shutdown_timeout,
        );
    }

    fn build_runtime(&self) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.config.startup.worker_threads)
            .enable_all()
            .build()
            .expect("failed to create pg-lakebase-storage tokio runtime")
    }

    fn build_reconciler(registry: StoreRegistry) -> StorageReconciler {
        StoreCatalogReconciler::new(PgTablespaceStoreCatalog::new(), registry)
    }

    fn initial_reconcile_or_exit(&mut self, reconciler: &mut StorageReconciler) {
        if let Err(error) = run_reconcile(reconciler, "startup") {
            logging::emit_pg_log(
                pg_sys::PGERROR as i32,
                &format!("storage worker startup reconcile failed: {error}"),
            );
            self.log_bridge.drain_to_pg_log();
            unsafe { pg_sys::proc_exit(1) };
        }
    }

    fn storage_runtime_or_exit(&mut self) -> StorageRuntime {
        match StorageRuntime::new(self.config.runtime.storage.clone()) {
            Ok(rt) => rt,
            Err(error) => {
                logging::emit_pg_log(
                    pg_sys::WARNING as i32,
                    &format!(
                        "storage runtime config invalid, worker cannot start: {error}",
                    ),
                );
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
        runtime_state: &mut SupervisorRuntimeState,
    ) {
        loop {
            self.log_bridge.drain_to_pg_log();
            self.exit_if_server_finished(runtime, server_handle);

            unsafe { pg_sys::AcceptInvalidationMessages() };

            if catalog::take_dirty()
                || runtime_state.periodic_reconcile_due(Instant::now())
            {
                if let Err(error) = run_reconcile(reconciler, "runtime") {
                    // Runtime reconcile failures are non-fatal: we keep the
                    // last good registry state and try again the next time
                    // the syscache fires or the periodic timer expires.
                    logging::emit_pg_log(
                        pg_sys::WARNING as i32,
                        &format!("storage worker reconcile failed: {error}"),
                    );
                }

                runtime_state.schedule_next_reconcile(Instant::now());
            }

            let timeout = runtime_state.wait_timeout();
            let should_continue = BackgroundWorker::wait_latch(Some(timeout));

            if BackgroundWorker::sighup_received() {
                unsafe { process_config_reload() };
                runtime_state.reload_from_gucs(storage_runtime, Instant::now());
            }

            if !should_continue {
                logging::emit_pg_log(
                    pg_sys::INFO as i32,
                    "storage background worker shutdown requested",
                );
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
                logging::emit_pg_log(pg_sys::INFO as i32, "storage server exited")
            }
            Ok(Err(e)) => logging::emit_pg_log(
                pg_sys::PGERROR as i32,
                &format!("storage server failed: {e}"),
            ),
            Err(e) => logging::emit_pg_log(
                pg_sys::PGERROR as i32,
                &format!("storage server task panicked: {e}"),
            ),
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
                        &format!("storage server failed during shutdown: {e}"),
                    ),
                    Err(e) => logging::emit_pg_log(
                        pg_sys::PGERROR as i32,
                        &format!("storage server task panicked during shutdown: {e}"),
                    ),
                }
                return;
            }

            std::thread::sleep(Duration::from_millis(50));
        }

        logging::emit_pg_log(
            pg_sys::WARNING as i32,
            "storage server did not stop before shutdown timeout; forcing runtime shutdown",
        );
    }
}

struct SupervisorRuntimeState {
    config: StorageWorkerRuntimeConfig,
    reconcile_interval: Option<Duration>,
    next_periodic_reconcile: Option<Instant>,
}

impl SupervisorRuntimeState {
    fn new(config: StorageWorkerRuntimeConfig) -> Self {
        let reconcile_interval = config.tablespace_reconcile_interval;
        Self {
            config,
            reconcile_interval,
            next_periodic_reconcile: next_reconcile_deadline(
                reconcile_interval,
                Instant::now(),
            ),
        }
    }

    fn periodic_reconcile_due(&self, now: Instant) -> bool {
        self.next_periodic_reconcile
            .is_some_and(|deadline| now >= deadline)
    }

    fn schedule_next_reconcile(&mut self, now: Instant) {
        self.next_periodic_reconcile =
            next_reconcile_deadline(self.reconcile_interval, now);
    }

    fn wait_timeout(&self) -> Duration {
        wait_timeout(self.reconcile_interval, self.next_periodic_reconcile)
    }

    fn reload_from_gucs(&mut self, storage_runtime: &StorageRuntime, now: Instant) {
        let new_config = StorageWorkerRuntimeConfig::from_gucs();
        if new_config == self.config {
            logging::emit_pg_log(
                pg_sys::INFO as i32,
                "SIGHUP received; runtime configuration unchanged",
            );
            return;
        }

        let old_config = self.config.clone();
        let old_storage = old_config.storage.clone();
        let interval_changed = new_config.tablespace_reconcile_interval
            != old_config.tablespace_reconcile_interval;

        let applied_storage = if new_config.storage != old_config.storage {
            Self::apply_storage_config(storage_runtime, new_config.storage.clone())
        } else {
            None
        };

        self.config = new_config;
        // For storage section, use the normalized value from the runtime if
        // apply succeeded, or keep the old value for retry.
        self.config.storage = applied_storage.unwrap_or(old_storage);

        log_runtime_config_change(&old_config, &self.config);

        if interval_changed {
            self.reconcile_interval = self.config.tablespace_reconcile_interval;
            self.next_periodic_reconcile =
                next_reconcile_deadline(self.reconcile_interval, now);
        }
    }

    fn apply_storage_config(
        storage_runtime: &StorageRuntime,
        config: StorageRuntimeConfig,
    ) -> Option<StorageRuntimeConfig> {
        match storage_runtime.apply(config) {
            Ok(report) if report.changed => {
                let snapshot = (*storage_runtime.snapshot()).clone();
                logging::emit_pg_log(
                    pg_sys::INFO as i32,
                    &format!(
                        "storage runtime config applied (version {})",
                        report.version,
                    ),
                );
                Some(snapshot)
            }
            Ok(_) => Some((*storage_runtime.snapshot()).clone()),
            Err(error) => {
                logging::emit_pg_log(
                    pg_sys::WARNING as i32,
                    &format!(
                        "storage runtime config apply failed, keeping old values: {error}",
                    ),
                );
                None
            }
        }
    }
}

/// Run one reconcile pass.
///
/// The PostgreSQL transaction wraps **only** the catalog scan
/// (`load_desired`). The desired snapshot is a plain Rust value, so the
/// transaction is committed before any [`StoreRegistry`] mutation happens.
/// This keeps PostgreSQL's transaction model and the registry's concurrency
/// model strictly separated: a transaction failure cannot leave the registry
/// partially mutated, and a registry mutation failure cannot roll back a
/// PostgreSQL transaction.
fn run_reconcile(
    reconciler: &mut StoreCatalogReconciler<PgTablespaceStoreCatalog>,
    phase: &str,
) -> Result<ReconcileReport, ReconcileRunError> {
    use std::panic::AssertUnwindSafe;

    let desired =
        BackgroundWorker::transaction(AssertUnwindSafe(|| reconciler.load_desired()))
            .map_err(|error| ReconcileRunError(error.to_string()))?;

    let report = reconciler
        .apply_desired(desired)
        .map_err(|error| ReconcileRunError(error.to_string()))?;

    // Always log the startup reconcile so operators can see how many stores
    // got registered. For runtime reconciles, suppress no-op rounds: with
    // the periodic safety-net resync running every 30s by default, logging
    // every iteration would otherwise produce one INFO line per interval
    // even when nothing changed.
    let changed = report.added + report.replaced + report.removed > 0;
    if phase == "startup" || changed {
        logging::emit_pg_log(
            pg_sys::INFO as i32,
            &format!(
                "storage worker {phase} reconcile: added={} replaced={} removed={} unchanged={}",
                report.added, report.replaced, report.removed, report.unchanged,
            ),
        );
    }
    Ok(report)
}

#[derive(Debug)]
struct ReconcileRunError(String);

impl std::fmt::Display for ReconcileRunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReconcileRunError {}

/// Compute the next periodic reconcile deadline, or `None` if periodic
/// reconciliation is disabled.
fn next_reconcile_deadline(
    interval: Option<Duration>,
    now: Instant,
) -> Option<Instant> {
    interval.map(|d| now + d)
}

/// Pick a `wait_latch` timeout for the next loop iteration.
///
/// The 100 ms ceiling sets the worker's responsiveness floor for SIGTERM,
/// SIGHUP, and the periodic reconcile timer. When the periodic deadline is
/// imminent (less than 100 ms away) we shorten the wait so the timer fires
/// on time; otherwise we just wake every 100 ms regardless of how far away
/// the next periodic reconcile is. The 1 ms minimum keeps the call from
/// returning instantly if the deadline has already elapsed; the next
/// iteration will then run the reconcile right away.
fn wait_timeout(
    reconcile_interval: Option<Duration>,
    next_periodic_reconcile: Option<Instant>,
) -> Duration {
    let base = Duration::from_millis(100);
    let Some(interval) = reconcile_interval else {
        return base;
    };
    let until_periodic = next_periodic_reconcile
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
        .unwrap_or(interval);
    base.min(until_periodic.max(Duration::from_millis(1)))
}

/// Tell PostgreSQL to re-read its configuration files and update GUC values
/// in the current process.
///
/// Must be called on the bgworker main thread after `sighup_received()`
/// returns `true`. Without this, `GucContext::Sighup` parameters would not
/// pick up new values from `postgresql.conf` / `ALTER SYSTEM`.
///
/// # Safety
///
/// Calls PostgreSQL FFI. Must run on the bgworker main thread.
unsafe fn process_config_reload() {
    unsafe {
        (&raw mut pg_sys::ConfigReloadPending)
            .write_volatile(0 as pg_sys::sig_atomic_t);
        pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
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

fn log_runtime_config_change(
    old: &StorageWorkerRuntimeConfig,
    new: &StorageWorkerRuntimeConfig,
) {
    let mut parts = Vec::new();

    if old.shutdown_timeout != new.shutdown_timeout {
        parts.push(format!(
            "shutdown_timeout: {}ms -> {}ms",
            old.shutdown_timeout.as_millis(),
            new.shutdown_timeout.as_millis(),
        ));
    }

    if old.tablespace_reconcile_interval != new.tablespace_reconcile_interval {
        let fmt = |v: &Option<Duration>| match v {
            Some(d) => format!("{}ms", d.as_millis()),
            None => "disabled".to_string(),
        };
        parts.push(format!(
            "tablespace_reconcile_interval: {} -> {}",
            fmt(&old.tablespace_reconcile_interval),
            fmt(&new.tablespace_reconcile_interval),
        ));
    }

    if old.storage != new.storage {
        if old.storage.cache.touch_granularity != new.storage.cache.touch_granularity
        {
            parts.push(format!(
                "cache_touch_granularity: {}ms -> {}ms",
                old.storage.cache.touch_granularity.as_millis(),
                new.storage.cache.touch_granularity.as_millis(),
            ));
        }
        let oc = &old.storage.cache.cleanup;
        let nc = &new.storage.cache.cleanup;
        if oc != nc {
            let fmt_bytes = |v: &Option<u64>| match v {
                Some(b) => format!("{}MiB", b / (1024 * 1024)),
                None => "disabled".to_string(),
            };
            let fmt_interval = |v: &Option<Duration>| match v {
                Some(d) => format!("{}ms", d.as_millis()),
                None => "disabled".to_string(),
            };
            parts.push(format!(
                "cache_cleanup: max={}->{} start={}%->{}% target={}%->{}% interval={}->{} batch_items={}->{} batch_mb={}->{}",
                fmt_bytes(&oc.max_cache_bytes), fmt_bytes(&nc.max_cache_bytes),
                oc.cleanup_start_percent, nc.cleanup_start_percent,
                oc.cleanup_target_percent, nc.cleanup_target_percent,
                fmt_interval(&oc.cleanup_interval), fmt_interval(&nc.cleanup_interval),
                oc.max_cleanup_batch_items, nc.max_cleanup_batch_items,
                oc.max_cleanup_batch_bytes / (1024 * 1024),
                nc.max_cleanup_batch_bytes / (1024 * 1024),
            ));
        }
    }

    if parts.is_empty() {
        return;
    }

    logging::emit_pg_log(
        pg_sys::INFO as i32,
        &format!(
            "SIGHUP: runtime configuration updated ({})",
            parts.join(", ")
        ),
    );
}
