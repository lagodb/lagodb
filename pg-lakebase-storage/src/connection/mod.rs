//! Per-connection pipeline: inbound [`crate::transport::read_frame`], bounded concurrency (`Semaphore`), outbound `mpsc` queue plus a dedicated writer task.
//!
//! Read responses reserve [`StorageServerConfig::max_pending_response_bytes`](crate::config::StorageServerConfig::max_pending_response_bytes) until the client consumes bodies or attached FDs.

use tokio::net::UnixStream;

use crate::cache::CacheIndex;
use crate::config::StorageServerConfig;
use crate::error::StorageResult;
use crate::session::StorageContext;

mod dispatch;
mod inbound;
mod pipeline;
mod request_tasks;
mod response_budget;
mod shutdown;
mod writer;

use pipeline::process_connection_with_shutdown;
use shutdown::ConnectionShutdown;

pub async fn process_connection<I: CacheIndex + 'static>(
    stream: UnixStream,
    context: StorageContext<I>,
    config: StorageServerConfig,
) -> StorageResult<()> {
    process_connection_with_shutdown(
        stream,
        context,
        config.normalized(),
        ConnectionShutdown::default(),
    )
    .await
}

pub(crate) async fn process_connection_with_drain_timeout<I: CacheIndex + 'static>(
    stream: UnixStream,
    context: StorageContext<I>,
    config: StorageServerConfig,
) -> StorageResult<()> {
    let config = config.normalized();
    process_connection_with_shutdown(
        stream,
        context,
        config.clone(),
        ConnectionShutdown {
            drain_timeout: config.connection_drain_timeout,
        },
    )
    .await
}

#[cfg(test)]
mod tests;
