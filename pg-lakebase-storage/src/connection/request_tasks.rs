use std::io;
use std::sync::Arc;

#[cfg(test)]
use std::future::Future;

use tokio::sync::{Semaphore, mpsc};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Instant, timeout_at};
use tracing::warn;

use crate::cache::CacheIndex;
use crate::error::{StorageError, StorageResult};
use crate::protocol::WireRequest;
use crate::session::StorageContext;

use super::dispatch::RequestDispatcher;
use super::response_budget::{
    QueuedResponse, ResponseByteLimiter, reserved_read_response_bytes,
};

pub(super) struct RequestTasks {
    tasks: JoinSet<()>,
}

impl RequestTasks {
    pub(super) fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(super) async fn join_next(&mut self) -> Option<Result<(), JoinError>> {
        self.tasks.join_next().await
    }

    pub(super) async fn spawn_request<I: CacheIndex + 'static>(
        &mut self,
        request: WireRequest,
        context: StorageContext<I>,
        request_limiter: Arc<Semaphore>,
        response_byte_limiter: ResponseByteLimiter,
        max_read_size: u32,
        response_tx: mpsc::Sender<QueuedResponse>,
    ) -> StorageResult<()> {
        let permit = request_limiter.acquire_owned().await.map_err(|error| {
            StorageError::io("request limiter closed", io::Error::other(error))
        })?;
        let reserved_response_bytes =
            reserved_read_response_bytes(&request, max_read_size);
        // Admit synchronously in wire order on the inbound loop, before spawning the dispatch
        // task. See [`RequestDispatcher::admit`] for the full ordering contract: a READ admitted
        // here holds a guard that keeps the target handle alive until `dispatch` completes, so a
        // following CLOSE on the same handle cannot remove it first.
        let dispatcher = RequestDispatcher::admit(&request, &context);
        self.tasks.spawn(async move {
            let _permit = permit;
            let response_bytes = match response_byte_limiter.acquire(reserved_response_bytes).await {
                Ok(response_bytes) => response_bytes,
                Err(error) => {
                    warn!(
                        client_addr = &*context.client_addr,
                        error = %error,
                        "storage connection response byte limiter failed before request dispatch",
                    );
                    return;
                },
            };
            let handler_response = dispatcher.dispatch(request, &context).await;
            let queued_response = QueuedResponse::new(handler_response, response_bytes);
            if let Err(send_error) = response_tx.send(queued_response).await {
                warn!(
                    client_addr = &*context.client_addr,
                    request_id = send_error.0.response.request_id(),
                    "storage connection response channel closed before completed response could be enqueued; \
                     client may observe a missing reply",
                );
            }
        });
        Ok(())
    }

    pub(super) async fn abort_all(&mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }

    pub(super) async fn drain_until(
        &mut self,
        drain_deadline: Instant,
        client_addr: &str,
    ) -> StorageResult<()> {
        match timeout_at(drain_deadline, self.drain()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                warn!(client_addr, error = %error, "storage connection request task failed while draining");
                self.abort_all().await;
                Err(StorageError::from_join_error("request task failed", error))
            }
            Err(_) => {
                warn!(
                    client_addr,
                    "storage connection drain timeout elapsed; aborting request tasks"
                );
                self.abort_all().await;
                Ok(())
            }
        }
    }

    async fn drain(&mut self) -> Result<(), JoinError> {
        while let Some(result) = self.tasks.join_next().await {
            result?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn spawn_background<F>(&mut self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks.spawn(task);
    }
}
