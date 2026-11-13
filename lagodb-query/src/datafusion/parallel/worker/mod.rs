//! Worker-local decode, serving and source release.

use std::ffi::CStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion_distributed::{
    ChannelResolver, DistributedExt, Worker, WorkerQueryContext, WorkerSessionBuilder,
};
use lagodb_core::query_contract::{TableScanRoute, TableScanRouteKind};
use prost::Message;

use super::bootstrap::ParallelBootstrap;
use super::catalog::WorkerSourceCatalog;
use super::codec::LagoPhysicalCodec;
use super::host::{ParallelExecutionHost, ParallelWorkerHost};
use super::source_inventory::MappedSourceInventory;
use super::transport::{
    ParallelChannelResolver, ParallelRuntime, RunRole, RunSignal, RunWatch,
    ServerStreams, StreamContext, TransportRegion, WakerSet,
};
use crate::ExecutionProfile;
use crate::datafusion::error::QueryExecutionError;
use crate::datafusion::memory::{
    ParticipantMemoryRecorder, QueryExecutionLimits, TaskMemoryPool,
};
use crate::datafusion::scan_callbacks::WorkerTableScanSource;

/// Drive a restored PG parallel worker through the upstream gRPC service.
///
/// # Safety
/// The host must retain its attached mapping, invoke this on the backend main
/// thread after activation, and report the returned error only after it returns.
pub unsafe fn run_parallel_worker<H: ParallelWorkerHost + 'static>(
    host: Arc<H>,
) -> Result<(), QueryExecutionError> {
    // SAFETY: the caller supplies the active PG mapping; its published length
    // covers the transport prefix and the shared source-inventory tail.
    let transport =
        unsafe { TransportRegion::attach(host.region(), host.region_bytes()) };
    // A worker that enters after the leader ended the run has nothing to serve,
    // and the durable flag means it cannot miss that notice.
    if transport.shutdown().load(Ordering::Acquire) {
        return Ok(());
    }
    let bootstrap =
        ParallelBootstrap::decode(transport.bootstrap()).map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to decode parallel source inventory: {error}"
            ))
        })?;
    // The leader encodes every bootstrap size from a `usize` of this same build
    // and platform, so narrowing back to `usize` is lossless.
    let inventory_len = bootstrap.source_inventory_bytes as usize;
    transport.verify_layout(host.region_bytes(), inventory_len);
    let transport = Arc::new(transport);
    transport.publish_token(host.process_index(), host.receiver_token());
    // SAFETY: activation publishes the complete mapping length only after the
    // leader initialized the transport prefix and the inventory tail.
    let inventory = unsafe {
        MappedSourceInventory::new(host.region(), host.region_bytes(), inventory_len)
    };
    // The bootstrap carries the leader's validated `QueryExecutionLimits`.
    let limits = QueryExecutionLimits::restore(
        bootstrap.work_mem_bytes as usize,
        bootstrap.hash_memory_bytes as usize,
        ExecutionProfile::restore(bootstrap.maximum_batch_rows as usize),
        bootstrap.spill_enabled,
    );
    let sources = WorkerRun::resolve_sources(&host, &bootstrap, &inventory)?;
    let source_host: Arc<dyn ParallelWorkerHost> = host.clone();
    let sources = WorkerSourceCatalog::new(sources, source_host);
    let live = Arc::new(AtomicBool::new(true));
    host.watch_mapping(Arc::clone(&live))
        .map_err(QueryExecutionError::ParallelHost)?;
    let run = WorkerRun {
        host: Arc::clone(&host),
        transport: Arc::clone(&transport),
        live,
        limits,
        collect_metrics: bootstrap.collect_metrics,
        sources: &sources,
    };
    let result = run.serve();
    // Every task plan and stream has dropped before sources are released.
    drop(run);
    let cleanup = sources.close();
    drop(transport);
    let interrupts = host
        .process_interrupts()
        .map_err(QueryExecutionError::ParallelHost);
    // PgTryBuilder turns PostgreSQL ERROR into this Result rather than reporting
    // it, so the first observed error is preserved while cleanup still runs.
    result
        .and(cleanup.map_err(QueryExecutionError::from))
        .and(interrupts)
}

/// One worker's serving state for one run.
struct WorkerRun<'a, H: ParallelWorkerHost> {
    host: Arc<H>,
    transport: Arc<TransportRegion>,
    live: Arc<AtomicBool>,
    limits: QueryExecutionLimits,
    collect_metrics: bool,
    sources: &'a WorkerSourceCatalog,
}

impl<H: ParallelWorkerHost + 'static> WorkerRun<'_, H> {
    /// Decodes every provider payload and binds it to its route.
    fn resolve_sources(
        host: &Arc<H>,
        bootstrap: &ParallelBootstrap,
        inventory: &MappedSourceInventory<'_>,
    ) -> DataFusionResult<Vec<Arc<WorkerTableScanSource>>> {
        let mut sources = Vec::with_capacity(bootstrap.sources.len());
        for source in &bootstrap.sources {
            let kind = TableScanRouteKind::from_code(source.route_kind).ok_or_else(
                || {
                    DataFusionError::Plan(
                        "parallel source has an unknown route kind".to_owned(),
                    )
                },
            )?;
            let name =
                CStr::from_bytes_with_nul(&source.route_name).map_err(|error| {
                    DataFusionError::Plan(format!(
                        "invalid parallel source route name: {error}"
                    ))
                })?;
            let callbacks = host
                .resolve_source(TableScanRoute::new(kind, name))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            sources.push(Arc::new(
                callbacks
                    .decode(inventory.payload(source))
                    .map_err(|error| DataFusionError::External(Box::new(error)))?,
            ));
        }
        Ok(sources)
    }

    /// Serves this run's gRPC traffic until the run ends.
    fn serve(&self) -> Result<(), QueryExecutionError> {
        let host: Arc<dyn ParallelExecutionHost> = self.host.clone();
        let runtime = ParallelRuntime::with_reactor(&host)?;
        let reactor = runtime
            .reactor()
            .expect("a reactor runtime contains its registered reactor");
        let wakers = Arc::new(WakerSet::new(self.transport.workers()));
        let attachment = reactor.attach(Arc::clone(&wakers));
        let streams = StreamContext::new(
            Arc::clone(&self.transport),
            Arc::clone(&host),
            Arc::clone(&wakers),
            Arc::clone(&self.live),
        );
        // Exactly one resolver per run: every task shares it, or each would open
        // its own HTTP/2 connection over the same DSM stream pair.
        let resolver: Arc<dyn ChannelResolver + Send + Sync> = Arc::new(
            ParallelChannelResolver::new(streams.clone(), self.host.process_index()),
        );
        // One peak recorder per worker participant: tasks take turns observing the
        // same reservation.
        let participant = self
            .collect_metrics
            .then(|| Arc::new(ParticipantMemoryRecorder::default()));
        let worker = Worker::from_session_builder(TaskSessionBuilder {
            sources: self.sources.clone(),
            limits: self.limits,
            target_partitions: self.transport.workers() as usize,
            resolver,
            participant,
        });
        let server = tonic::transport::Server::builder()
            .add_service(worker.into_worker_server());
        let incoming = ServerStreams::new(&streams, self.host.process_index());
        let served = runtime.block_on(async {
            let watch =
                RunWatch::new(&self.transport, &self.live, &wakers, RunRole::Worker);
            tokio::select! {
                served = server.serve_with_incoming(incoming) => Served::Done(served),
                signal = watch => Served::Signalled(signal),
            }
        })?;
        drop(attachment);
        drop(runtime);
        match served {
            Served::Done(served) => served
                .map_err(|error| DataFusionError::External(Box::new(error)).into()),
            // The leader ended the run.
            Served::Signalled(RunSignal::Shutdown) => Ok(()),
            Served::Signalled(RunSignal::Abort) => {
                Err(QueryExecutionError::interrupted().into())
            }
        }
    }
}

/// What ended the serve loop.
enum Served {
    Done(Result<(), tonic::transport::Error>),
    Signalled(RunSignal),
}

/// Builds the upstream session of one task.
///
/// Only the task-specific pieces live here: the run-scoped resolver and the
/// worker codec are installed on top of the distributed task config. A worker
/// has no statement-wide session to inherit from, and a codec registration
/// appends rather than replaces.
struct TaskSessionBuilder {
    sources: WorkerSourceCatalog,
    limits: QueryExecutionLimits,
    /// Partitions this worker sized its stage plans for.
    target_partitions: usize,
    /// The run's per-participant gRPC clients, shared by every task.
    resolver: Arc<dyn ChannelResolver + Send + Sync>,
    /// The participant-wide peak recorder, absent when metrics are off.
    participant: Option<Arc<ParticipantMemoryRecorder>>,
}

#[async_trait::async_trait]
impl WorkerSessionBuilder for TaskSessionBuilder {
    async fn build_session_state(
        &self,
        mut ctx: WorkerQueryContext,
    ) -> DataFusionResult<SessionState> {
        let memory = TaskMemoryPool::new(self.limits, self.participant.clone());
        let config = ctx
            .builder
            .config()
            .take()
            .expect("a distributed task always carries its session config");
        let config = self
            .limits
            .apply_session_policy(config, self.target_partitions);
        Ok(ctx
            .builder
            .with_config(config)
            .with_runtime_env(memory.environment())
            .with_distributed_channel_resolver(Arc::clone(&self.resolver))
            .with_distributed_user_codec(LagoPhysicalCodec::worker(
                self.sources.clone(),
            ))
            // The plan arrives after the session exists: the rewrite hook publishes
            // the task's memory limit, the finished hook reports its peaks.
            .with_distributed_worker_plan_rewrite_handler(memory.clone())
            .with_distributed_task_finished_handler(memory)
            .build())
    }
}
