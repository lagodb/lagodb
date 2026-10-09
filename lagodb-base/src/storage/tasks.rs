//! Rust tasks owned and polled by the PG main-thread supervisor.
//!
//! Only one management task runs at a time. Reconcile moves its state into the
//! task and returns it on completion. Result collection never waits or starts
//! another task; the supervisor decides how each completion advances lifecycle.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use lagodb_core::storage::tablespace::TablespaceStorage;
use lagodb_storage::{ManagedStoreRegistry, StorageRuntime, StorageServerBuilder};

use super::config::StorageWorkerStartupConfig;
use super::error::StorageWorkerError;
use super::reconciler::{ReconcileError, ReconcileReport};
use super::reload::{ReconcilePhase, StorageReconciler};

mod server;

use server::ServerTask;

type ReconcileResult = Result<ReconcileReport, ReconcileError>;

/// One owned task slot, with a single nonblocking result-collection operation.
struct WorkerTask<T> {
    name: &'static str,
    handle: Option<JoinHandle<T>>,
}

impl<T> WorkerTask<T> {
    fn new(name: &'static str) -> Self {
        Self { name, handle: None }
    }

    fn start(&mut self, handle: JoinHandle<T>) {
        self.handle = Some(handle);
    }

    fn try_result(&mut self) -> Result<Option<T>, StorageWorkerError> {
        let Some(handle) = self.handle.as_mut() else {
            return Ok(None);
        };
        // The PG loop checks completion between bounded latch waits. A no-op
        // waker is sufficient because that loop polls again; this reads only
        // the JoinHandle result and never drives the task's business future.
        let mut context = Context::from_waker(Waker::noop());
        let Poll::Ready(result) = Pin::new(handle).poll(&mut context) else {
            return Ok(None);
        };
        self.handle = None;
        result.map(Some).map_err(|source| StorageWorkerError::Task {
            task: self.name,
            source,
        })
    }

    fn is_active(&self) -> bool {
        self.handle.is_some()
    }

    fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

struct ReconcileRequest {
    phase: ReconcilePhase,
    force_default_chain: bool,
}

struct ReconcileTask {
    phase: ReconcilePhase,
    task: WorkerTask<(StorageReconciler, ReconcileResult)>,
}

pub(super) enum StorageTaskEvent {
    Reconciled {
        phase: ReconcilePhase,
        report: ReconcileReport,
    },
    ServerReady,
    ServerExited,
}

pub(super) struct StorageWorkerTasks {
    registry: ManagedStoreRegistry,
    reconciler: Option<StorageReconciler>,
    pending: Option<ReconcileRequest>,
    reconcile: ReconcileTask,
    server: ServerTask,
}

impl StorageWorkerTasks {
    pub(super) fn new(
        registry: ManagedStoreRegistry,
        reconciler: StorageReconciler,
    ) -> Self {
        Self {
            registry,
            reconciler: Some(reconciler),
            pending: None,
            reconcile: ReconcileTask {
                phase: ReconcilePhase::Startup,
                task: WorkerTask::new("storage reconcile"),
            },
            server: ServerTask::new(),
        }
    }

    pub(super) fn request_reconcile(
        &mut self,
        phase: ReconcilePhase,
        force_default_chain: bool,
    ) {
        // Keep the first trigger's diagnostic label and merge forced refresh
        // intent into the pending attempt. A burst of hints needs one snapshot.
        self.pending
            .get_or_insert(ReconcileRequest {
                phase,
                force_default_chain,
            })
            .force_default_chain |= force_default_chain;
    }

    /// Load the latest snapshot on the PG thread, then move apply into Tokio.
    pub(super) fn dispatch_reconcile(
        &mut self,
        runtime: &Runtime,
    ) -> Result<(), StorageWorkerError> {
        if self.reconcile.task.is_active() || self.server.is_starting() {
            return Ok(());
        }
        // Reload hints request one attempt, not retries until success. Consume
        // before loading: a snapshot read failure is reported without requeuing
        // the hint. Periodic reconciliation still runs, but does not retain the
        // force flag; another forced attempt requires a new SQL reload request
        // with force enabled or SIGHUP. Keeping pending here would retry on every
        // supervisor poll while a persistent configuration error remains.
        let Some(request) = self.pending.take() else {
            return Ok(());
        };
        let mut reconciler = self
            .reconciler
            .take()
            .expect("idle storage tasks own the reconciler");
        // Intentionally read, parse and validate the local volume config here.
        // This is control-plane maintenance, not per-row work. Synchronous file
        // I/O alone does not justify adding a blocking task; revisit this boundary
        // if measured config-load latency or observed signal-response delays
        // demonstrate a responsiveness problem.
        let desired = match reconciler.load_desired() {
            Ok(desired) => desired,
            Err(error) => {
                self.reconciler = Some(reconciler);
                return Err(StorageWorkerError::Reconcile {
                    phase: request.phase,
                    source: error,
                });
            }
        };
        // Explicit reload/SIGHUP refreshes backend caches after a successful
        // snapshot read, including after operator restoration of the config.
        // This does not authorize manual binding changes. Backend cache misses
        // read the config directly; reload is not an admission boundary.
        // Periodic reconciliation alone does not invalidate backend caches.
        if request.force_default_chain {
            TablespaceStorage::invalidate_all();
        }
        self.reconcile.phase = request.phase;
        self.reconcile.task.start(runtime.spawn(async move {
            let result = reconciler
                .apply_desired(desired, request.force_default_chain)
                .await;
            (reconciler, result)
        }));
        Ok(())
    }

    /// Called once, after the initial reconcile has completed successfully.
    pub(super) fn start_server(
        &mut self,
        runtime: &Runtime,
        config: &StorageWorkerStartupConfig,
        storage_runtime: StorageRuntime,
    ) {
        let builder =
            StorageServerBuilder::new(&config.socket_path, &config.cache_dir)
                .with_server_config(config.server_config.clone())
                .with_service_config(config.service_config.clone())
                .with_managed_store_registry(self.registry.clone())
                .with_runtime(storage_runtime)
                .with_tracing_request_observer();
        self.server.start(runtime, builder);
    }

    pub(super) fn serve(&mut self) {
        self.server.serve();
    }

    /// Shared by normal supervision and shutdown. Pending tasks stay owned;
    /// completed handles are consumed once, including panics and cancellation.
    pub(super) fn poll(
        &mut self,
    ) -> Result<Option<StorageTaskEvent>, StorageWorkerError> {
        if let Some(event) = self.server.poll()? {
            return Ok(Some(event));
        }
        if let Some((reconciler, result)) = self.reconcile.task.try_result()? {
            self.reconciler = Some(reconciler);
            let report = result.map_err(|source| StorageWorkerError::Reconcile {
                phase: self.reconcile.phase,
                source,
            })?;
            return Ok(Some(StorageTaskEvent::Reconciled {
                phase: self.reconcile.phase,
                report,
            }));
        }
        Ok(None)
    }

    pub(super) fn request_shutdown(&mut self) {
        self.pending = None;
        self.server.stop();
        self.reconcile.task.abort();
    }

    pub(super) fn is_drained(&self) -> bool {
        !self.reconcile.task.is_active() && !self.server.is_active()
    }
}

impl Drop for StorageWorkerTasks {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}
