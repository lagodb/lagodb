//! Worker URLs and gRPC clients for one run.
//!
//! lagodb maps one URL to one participant process and builds that process's
//! client on top of the DSM stream connecting to it, so no TCP socket, DNS
//! lookup or TLS layer is involved.

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use datafusion::common::{Result, exec_datafusion_err};
use datafusion_distributed::grpc::{BoxCloneSyncChannel, create_worker_client};
use datafusion_distributed::{ChannelResolver, WorkerChannel, WorkerResolver};
use hyper_util::rt::TokioIo;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;
use url::Url;

use super::stream::{DsmStream, StreamContext};

/// URL scheme of one participant process.
const SCHEME: &str = "lagodb";

/// The URL list the planner and the router use for this run.
///
/// Planning sees the requested width, because the task count is decided before
/// PostgreSQL reports how many workers attached; execution sees the attached
/// width.
pub(in crate::datafusion) struct ParallelWorkerResolver {
    urls: Box<[Url]>,
}

impl ParallelWorkerResolver {
    pub(in crate::datafusion) fn planning(worker_cap: u32) -> Self {
        Self::of(1..=worker_cap)
    }

    pub(in crate::datafusion) fn attached(workers: u32) -> Self {
        Self::of(1..=workers)
    }

    fn of(procs: impl Iterator<Item = u32>) -> Self {
        Self {
            urls: procs.map(worker_url).collect(),
        }
    }

    /// The process a generated URL addresses.
    fn proc_of(url: &Url) -> Result<u32> {
        url.host_str()
            .and_then(|host| host.strip_prefix("worker-"))
            .and_then(|proc| proc.parse().ok())
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "lagodb transport received the unknown worker URL '{url}'"
                )
            })
    }
}

fn worker_url(proc: u32) -> Url {
    Url::parse(&format!("{SCHEME}://worker-{proc}/"))
        .expect("a generated worker URL always parses")
}

impl WorkerResolver for ParallelWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>> {
        Ok(self.urls.to_vec())
    }
}

/// gRPC clients of one run, one per participant process.
pub(in crate::datafusion) struct ParallelChannelResolver {
    streams: StreamContext,
    /// This participant's process number; the leader is 0.
    this_proc: u32,
    channels: Mutex<HashMap<u32, BoxCloneSyncChannel>>,
}

impl ParallelChannelResolver {
    pub(in crate::datafusion) fn new(streams: StreamContext, this_proc: u32) -> Self {
        Self {
            streams,
            this_proc,
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// The client of `server`, created on first use.
    ///
    /// Upstream asks for a client per RPC, so the channel is cached: one
    /// connection owns exactly one DSM stream pair.
    fn channel(&self, server: u32) -> Result<BoxCloneSyncChannel> {
        let mut channels =
            self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(channel) = channels.get(&server) {
            return Ok(channel.clone());
        }
        let stream = self.streams.connect(self.this_proc, server).ok_or_else(
            || {
                exec_datafusion_err!(
                    "lagodb transport has no connection from process {} to {server}",
                    self.this_proc
                )
            },
        )?;
        let channel = Endpoint::from_shared(worker_url(server).to_string())
            .map_err(|error| {
                exec_datafusion_err!("invalid lagodb worker URL: {error}")
            })?
            .connect_with_connector_lazy(connector(stream));
        let channel = BoxCloneSyncChannel::new(channel);
        channels.insert(server, channel.clone());
        Ok(channel)
    }
}

#[async_trait]
impl ChannelResolver for ParallelChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<Box<dyn WorkerChannel>> {
        let server = ParallelWorkerResolver::proc_of(url)?;
        Ok(create_worker_client(self.channel(server)?))
    }
}

/// Hands the run's single DSM stream to the channel that owns the connection.
///
/// The endpoint is never reconnected: a failed HTTP/2 connection fails the query
/// instead of silently opening a transport that does not exist.
fn connector(
    stream: DsmStream,
) -> impl tower::Service<
    Uri,
    Response = TokioIo<DsmStream>,
    Error = io::Error,
    Future = impl Send,
> + Clone
+ Send {
    let stream = Arc::new(Mutex::new(Some(stream)));
    service_fn(move |_uri: Uri| {
        let stream = Arc::clone(&stream);
        async move {
            let stream = stream.lock().unwrap_or_else(PoisonError::into_inner).take();
            stream.map(TokioIo::new).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "lagodb transport stream has already been consumed",
                )
            })
        }
    })
}
