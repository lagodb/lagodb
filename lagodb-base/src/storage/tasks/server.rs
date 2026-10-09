//! One Rust task owns the server from startup through final release.
//!
//! The PG supervisor exchanges only readiness and permission to serve. Neither
//! completed task outputs nor dropped notification channels can own the server.

use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use lagodb_storage::StorageServerBuilder;

use super::super::error::{ServerTaskError, StorageWorkerError};
use super::{StorageTaskEvent, WorkerTask};

pub(super) struct ServerTask {
    task: WorkerTask<Result<(), ServerTaskError>>,
    ready: Option<oneshot::Receiver<()>>,
    start_serving: Option<oneshot::Sender<()>>,
    shutdown: CancellationToken,
}

impl ServerTask {
    pub(super) fn new() -> Self {
        Self {
            task: WorkerTask::new("storage server lifecycle"),
            ready: None,
            start_serving: None,
            shutdown: CancellationToken::new(),
        }
    }

    pub(super) fn start(&mut self, runtime: &Runtime, builder: StorageServerBuilder) {
        let (ready, ready_receiver) = oneshot::channel();
        let (start_serving, start_receiver) = oneshot::channel();
        self.ready = Some(ready_receiver);
        self.start_serving = Some(start_serving);
        self.task.start(runtime.spawn(Self::run(
            builder,
            self.shutdown.clone(),
            ready,
            start_receiver,
        )));
    }

    async fn run(
        builder: StorageServerBuilder,
        shutdown: CancellationToken,
        ready: oneshot::Sender<()>,
        start_serving: oneshot::Receiver<()>,
    ) -> Result<(), ServerTaskError> {
        let server = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            result = builder.start() => result.map_err(ServerTaskError::Startup)?,
        };
        if ready.send(()).is_err() {
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            permission = start_serving => {
                if permission.is_err() {
                    return Ok(());
                }
            }
        }
        // Design tradeoff: a spontaneous serving failure completes resource
        // release before the supervisor receives the error and exits for restart.
        // If this task releases the final index reference, synchronous redb close
        // is part of task completion. We accept that delay in error reporting and
        // restart, including retaining the last published Running state, to keep
        // resource ownership and the final result in one lifecycle task.
        // The shutdown budget starts only when the supervisor accepts shutdown;
        // it does not bound this preceding close. The PG main thread remains
        // responsive to external SIGTERM, which enters bounded shutdown even if
        // close has not returned. That deadline limits waiting, not disk I/O;
        // unfinished work is terminated by process exit.
        server
            .serve_until(shutdown)
            .await
            .map_err(ServerTaskError::Serve)
    }

    pub(super) fn serve(&mut self) {
        let permission = self
            .start_serving
            .take()
            .expect("a ready server waits for the supervisor's permission to serve");
        // Any task failure that closed the receiver is reported by poll().
        let _ = permission.send(());
    }

    pub(super) fn poll(
        &mut self,
    ) -> Result<Option<StorageTaskEvent>, StorageWorkerError> {
        // A task failure wins over a readiness notification already in flight.
        if let Some(result) = self.task.try_result()? {
            self.stop();
            result?;
            return Ok(Some(StorageTaskEvent::ServerExited));
        }
        if self
            .ready
            .as_mut()
            .is_some_and(|ready| ready.try_recv().is_ok())
        {
            self.ready = None;
            return Ok(Some(StorageTaskEvent::ServerReady));
        }
        Ok(None)
    }

    pub(super) fn is_starting(&self) -> bool {
        self.start_serving.is_some()
    }

    pub(super) fn is_active(&self) -> bool {
        self.task.is_active()
    }

    pub(super) fn stop(&mut self) {
        self.shutdown.cancel();
        self.ready = None;
        self.start_serving = None;
    }
}
