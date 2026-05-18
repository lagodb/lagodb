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

use pg_lakebase_storage::StoreRegistry;

use super::catalog::{self, PgTablespaceStoreCatalog};
use super::config::StorageWorkerConfig;
use super::logging::{self, PgLogBridge};
use super::reconciler::{ReconcileReport, StoreCatalogReconciler};

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
            logging::new_bridge(config.log_channel_capacity);

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
        // Attach to PostgreSQL with SPI access so the reconciler can scan
        // `pg_tablespace`. We pass `None, None` to skip database binding;
        // `pg_tablespace` is a shared catalog and is reachable without a
        // connected database.
        BackgroundWorker::connect_worker_to_spi(None, None);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.config.worker_threads)
            .enable_all()
            .build()
            .expect("failed to create pg-lakebase-storage tokio runtime");

        // Build the shared registry first. The reconciler and the storage
        // server hold separate clones of the same internal Arc<RwLock<...>>,
        // so initial-reconcile registrations are visible to the very first
        // accepted connection.
        let registry = StoreRegistry::new();
        let mut reconciler =
            StoreCatalogReconciler::new(PgTablespaceStoreCatalog::new(), registry.clone());

        // Initial reconcile must succeed before we bind the listening socket.
        // Failing here is treated like a fatal startup error: the bgworker
        // exits and PostgreSQL administrators see the cause in the log.
        if let Err(error) = run_reconcile(&mut reconciler, "startup") {
            logging::emit_pg_log(
                pg_sys::PGERROR as i32,
                &format!("storage worker startup reconcile failed: {error}"),
            );
            self.log_bridge.drain_to_pg_log();
            unsafe { pg_sys::proc_exit(1) };
        }

        // Clear any syscache dirty bit accumulated during connect / initial
        // reconcile so the first main-loop iteration does not redundantly
        // re-scan the catalog we just read.
        let _ = catalog::take_dirty();

        let shutdown = self.shutdown.clone();
        let config = self.config.clone();
        let server_registry = registry.clone();

        let mut server_handle = Some(runtime.spawn(async move {
            let server = pg_lakebase_storage::StorageServerBuilder::new(
                &config.socket_path,
                &config.cache_dir,
            )
            .with_server_config(config.server_config)
            // Tell the service layer that the registry is owned by the
            // tablespace reconciler so wire-level RegisterStore /
            // UnregisterStore requests are rejected. Without this gate the
            // reconciler would not be the single writer of `StoreRegistry`
            // and a client unregister would silently persist (the next
            // reconcile would compute desired == applied and not restore).
            .with_service_config(
                config
                    .service_config
                    .with_externally_managed_registry(),
            )
            .with_store_registry(server_registry)
            .with_tracing_request_observer()
            .bind()
            .await?;

            server.serve_until(shutdown).await
        }));

        logging::emit_pg_log(
            pg_sys::INFO as i32,
            "storage background worker started",
        );

        let reconcile_interval = self.config.tablespace_reconcile_interval;
        let mut next_periodic_reconcile = reconcile_interval.map(|d| Instant::now() + d);

        loop {
            self.log_bridge.drain_to_pg_log();

            if server_handle.as_ref().is_some_and(|h| h.is_finished()) {
                let handle = server_handle.take().unwrap();
                self.shutdown.cancel();

                match runtime.block_on(handle) {
                    Ok(Ok(())) => logging::emit_pg_log(
                        pg_sys::INFO as i32,
                        "storage server exited",
                    ),
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

            // Pull any pending invalidation messages so the syscache callback
            // we registered fires before we look at the dirty flag. Without
            // this, the dirty flag could lag behind newly-committed catalog
            // changes from other backends.
            unsafe { pg_sys::AcceptInvalidationMessages() };

            let now = Instant::now();
            let periodic_due = match next_periodic_reconcile {
                Some(deadline) => now >= deadline,
                None => false,
            };

            if catalog::take_dirty() || periodic_due {
                if let Err(error) = run_reconcile(&mut reconciler, "runtime") {
                    // Runtime reconcile failures are non-fatal: we keep the
                    // last good registry state and try again the next time
                    // the syscache fires or the periodic timer expires.
                    logging::emit_pg_log(
                        pg_sys::WARNING as i32,
                        &format!("storage worker reconcile failed: {error}"),
                    );
                }

                if let Some(interval) = reconcile_interval {
                    next_periodic_reconcile = Some(Instant::now() + interval);
                }
            }

            let timeout = wait_timeout(reconcile_interval, next_periodic_reconcile);
            let should_continue = BackgroundWorker::wait_latch(Some(timeout));

            if BackgroundWorker::sighup_received() {
                logging::emit_pg_log(
                    pg_sys::INFO as i32,
                    "SIGHUP received; storage worker GUC changes require PostgreSQL restart",
                );
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

        // Single shutdown budget shared by `wait_for_server_shutdown` and the
        // final `runtime.shutdown_timeout` call.  Whatever the first phase did
        // not consume becomes the hard cap on the second phase, so the GUC
        // expresses one total stop budget rather than two implicit ones.
        let deadline = Instant::now() + self.config.shutdown_timeout;
        self.wait_for_server_shutdown(&runtime, &mut server_handle, deadline);
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
        server_handle: &mut Option<
            tokio::task::JoinHandle<pg_lakebase_storage::StorageResult<()>>,
        >,
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

    let desired = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        reconciler.load_desired()
    }))
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
