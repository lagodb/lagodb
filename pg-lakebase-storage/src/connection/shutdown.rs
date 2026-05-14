//! Shutdown policy and exit reasons.
//!
//! [`ConnectionShutdown`] is the policy value — it owns the drain budget and knows how to apply
//! that budget to a clean inbound close. [`ConnectionExit`] is the vocabulary that the main loop
//! uses to communicate why it left, so shutdown branching can pattern-match on it.

use tokio::task::JoinError;
use tokio::time::{Duration, Instant};
use tracing::debug;

use crate::config::DEFAULT_CONNECTION_DRAIN_TIMEOUT;
use crate::error::{StorageError, StorageResult};

use super::request_tasks::RequestTasks;
use super::writer::ResponseWriter;

#[derive(Clone, Copy)]
pub(super) struct ConnectionShutdown {
    pub(super) drain_timeout: Duration,
}

impl Default for ConnectionShutdown {
    fn default() -> Self {
        Self {
            drain_timeout: DEFAULT_CONNECTION_DRAIN_TIMEOUT,
        }
    }
}

impl ConnectionShutdown {
    /// Drains in-flight request tasks and then waits for the writer to flush any remaining
    /// responses, bounded by a single total deadline so a slow writer cannot extend the drain
    /// budget beyond what `drain_timeout` promises.
    pub(super) async fn drain_on_inbound_closed(
        self,
        request_tasks: &mut RequestTasks,
        writer: &mut ResponseWriter,
        client_addr: &str,
    ) -> StorageResult<()> {
        debug!(
            client_addr,
            "storage connection inbound closed; draining request tasks"
        );
        let drain_deadline = Instant::now() + self.drain_timeout;
        if let Err(error) =
            request_tasks.drain_until(drain_deadline, client_addr).await
        {
            writer.close_sender();
            writer.abort().await;
            return Err(error);
        }
        writer.close_sender();
        writer.wait_until(drain_deadline).await.unwrap_or(Ok(()))
    }
}

/// Reason a connection's main select loop exited. Drives the shutdown sequence.
pub(super) enum ConnectionExit {
    /// Peer closed the read half; any in-flight requests should be drained within the timeout.
    InboundClosed,
    /// Reading or decoding a frame failed; propagate the underlying error.
    ReaderFailed(StorageError),
    /// The writer task completed (usually because its channel closed or a write errored).
    WriterFinished(StorageResult<()>),
    /// A spawned request task panicked or was cancelled abnormally.
    RequestTaskFailed(JoinError),
}
