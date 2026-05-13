//! Per-connection state machine.
//!
//! `Connection` owns every subsystem that belongs to one Unix stream (inbound reader, request
//! tasks, response writer, admission limiters) and coordinates their lifecycle. The main loop runs
//! until a [`ConnectionExit`] is produced; shutdown branches on the exit reason and tears the
//! subsystems down in order.

use std::sync::Arc;

use tokio::net::UnixStream;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::cache::CacheIndex;
use crate::config::StorageServerConfig;
use crate::error::{StorageError, StorageResult};
use crate::protocol::WireRequest;
use crate::session::StorageContext;

use super::inbound::{InboundEvent, InboundReader};
use super::request_tasks::RequestTasks;
use super::response_budget::ResponseByteLimiter;
use super::shutdown::{ConnectionExit, ConnectionShutdown};
use super::writer::ResponseWriter;

pub(super) async fn process_connection_with_shutdown<I: CacheIndex + 'static>(
    stream: UnixStream,
    context: StorageContext<I>,
    config: StorageServerConfig,
    shutdown: ConnectionShutdown,
) -> StorageResult<()> {
    config.validate_for_max_read_size(context.service.max_read_size())?;
    Connection::new(stream, context, config, shutdown).run().await
}

struct Connection<I: CacheIndex> {
    inbound: InboundReader,
    context: StorageContext<I>,
    client_addr: Arc<str>,
    max_read_size: u32,
    request_limiter: Arc<Semaphore>,
    response_byte_limiter: ResponseByteLimiter,
    request_tasks: RequestTasks,
    writer: ResponseWriter,
    shutdown: ConnectionShutdown,
}

impl<I: CacheIndex + 'static> Connection<I> {
    fn new(
        stream: UnixStream,
        context: StorageContext<I>,
        config: StorageServerConfig,
        shutdown: ConnectionShutdown,
    ) -> Self {
        let client_addr = context.client_addr.clone();
        debug!(
            client_addr = &*client_addr,
            max_in_flight_requests = config.max_in_flight_requests,
            max_open_handles_per_connection = config.max_open_handles_per_connection,
            max_pending_responses = config.max_pending_responses,
            max_pending_response_bytes = config.max_pending_response_bytes,
            "storage connection processing started",
        );

        let (reader, writer) = stream.into_split();
        let max_read_size = context.service.max_read_size();
        Self {
            inbound: InboundReader::new(reader),
            context,
            client_addr,
            max_read_size,
            request_limiter: Arc::new(Semaphore::new(config.max_in_flight_requests)),
            response_byte_limiter: ResponseByteLimiter::new(config.max_pending_response_bytes),
            request_tasks: RequestTasks::new(),
            writer: ResponseWriter::spawn(writer, config.max_pending_responses, config.response_write_timeout),
            shutdown,
        }
    }

    async fn run(mut self) -> StorageResult<()> {
        let exit = self.poll_until_exit().await;
        let shutdown_result = self.shutdown_after(exit).await;
        let handles_closed = self.context.handles.len();
        let close_result = self.context.service.close_all_handles(&self.context.handles).await;
        let result = shutdown_result.and(close_result);
        info!(
            client_addr = &*self.client_addr,
            handles_closed,
            success = result.is_ok(),
            "storage connection closed",
        );
        result
    }

    /// Drives the main select loop until any subsystem requests exit.
    async fn poll_until_exit(&mut self) -> ConnectionExit {
        loop {
            tokio::select! {
                writer_result = self.writer.wait_finished() => {
                    return ConnectionExit::WriterFinished(writer_result);
                },
                request_result = self.request_tasks.join_next(), if !self.request_tasks.is_empty() => {
                    if let Some(Err(error)) = request_result {
                        return ConnectionExit::RequestTaskFailed(error);
                    }
                },
                inbound = self.inbound.next_event() => {
                    match inbound {
                        Ok(InboundEvent::Request(request)) => {
                            if let Err(error) = self.spawn_request(*request).await {
                                return ConnectionExit::ReaderFailed(error);
                            }
                        },
                        Ok(InboundEvent::Closed) => return ConnectionExit::InboundClosed,
                        Err(error) => return ConnectionExit::ReaderFailed(error),
                    }
                },
            }
        }
    }

    /// Branches on the exit reason and runs the matching shutdown sequence.
    async fn shutdown_after(&mut self, exit: ConnectionExit) -> StorageResult<()> {
        match exit {
            ConnectionExit::InboundClosed => self.shutdown_on_inbound_closed().await,
            ConnectionExit::ReaderFailed(error) => {
                warn!(client_addr = &*self.client_addr, error = %error, "storage connection reader failed");
                self.tear_down().await;
                Err(error)
            },
            ConnectionExit::WriterFinished(result) => {
                debug!(client_addr = &*self.client_addr, "storage connection writer finished");
                self.request_tasks.abort_all().await;
                self.writer.close_sender();
                if let Err(error) = &result {
                    warn!(client_addr = &*self.client_addr, error = %error, "storage connection writer failed");
                }
                result
            },
            ConnectionExit::RequestTaskFailed(error) => {
                warn!(client_addr = &*self.client_addr, error = %error, "storage connection request task failed");
                self.tear_down().await;
                Err(StorageError::from_join_error("request task failed", error))
            },
        }
    }

    /// Peer closed the read half cleanly: delegate to the shutdown policy, which owns the drain
    /// budget rule.
    async fn shutdown_on_inbound_closed(&mut self) -> StorageResult<()> {
        self.shutdown
            .drain_on_inbound_closed(&mut self.request_tasks, &mut self.writer, &self.client_addr)
            .await
    }

    /// Abortive teardown used when the exit reason is itself an error.
    async fn tear_down(&mut self) {
        self.request_tasks.abort_all().await;
        self.writer.close_sender();
        self.writer.abort().await;
    }

    async fn spawn_request(&mut self, request: WireRequest) -> StorageResult<()> {
        self.request_tasks
            .spawn_request(
                request,
                self.context.clone(),
                self.request_limiter.clone(),
                self.response_byte_limiter.clone(),
                self.max_read_size,
                self.writer.sender(),
            )
            .await
    }
}
