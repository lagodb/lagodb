//! Background worker main-thread supervisor.
//!
//! Owns the Tokio runtime, the storage server task, and the PG log bridge.
//! The main thread loop handles signals, drains logs, and monitors the server task.

use std::time::{Duration, Instant};

use pgrx::bgworkers::BackgroundWorker;
use pgrx::pg_sys;
use tokio_util::sync::CancellationToken;

use super::config::StorageWorkerConfig;
use super::logging::{self, PgLogBridge};

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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.config.worker_threads)
            .enable_all()
            .build()
            .expect("failed to create pg-lakebase-storage tokio runtime");

        let shutdown = self.shutdown.clone();
        let config = self.config.clone();

        let mut server_handle = Some(runtime.spawn(async move {
            let server = pg_lakebase_storage::StorageServerBuilder::new(
                &config.socket_path,
                &config.cache_dir,
            )
            .with_server_config(config.server_config)
            .with_service_config(config.service_config)
            .with_tracing_request_observer()
            .bind()
            .await?;

            server.serve_until(shutdown).await
        }));

        logging::emit_pg_log(
            pg_sys::INFO as i32,
            "storage background worker started",
        );

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

            let should_continue =
                BackgroundWorker::wait_latch(Some(Duration::from_millis(100)));

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
