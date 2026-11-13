//! Plan-first launch and the leader's one-run transport owner.

use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::PeakRecordingPool;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::{SendableRecordBatchStream, context::SessionContext};
use futures::StreamExt;
use prost::Message;

use super::bootstrap::ParallelBootstrap;
use super::catalog::{ParallelSourceCatalog, PreparedParallelSource};
use super::host::{ParallelExecutionHost, ParallelWorkers};
use super::lifecycle::ParallelWorkerLaunch;
use super::metrics::ParallelMetrics;
use super::session::ParallelSession;
use super::source_inventory::{ParallelSourceRoute, PreparedSourceInventory};
use super::stages::ParallelStageCatalog;
use super::transport::{
    ParallelRuntime, ReactorAttachment, RunRole, RunSignal, RunWatch, StreamContext,
    TransportLayout, TransportRegion, WakerSet,
};
use crate::datafusion::error::QueryExecutionError;
use crate::datafusion::memory::QueryExecutionLimits;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::physical_plan::CompiledPhysicalPlan;
use crate::datafusion::scan_callbacks::WorkerTableScanCallbacks;
use crate::plan::PlannedTableScan;

/// A finished distributed plan and its provider-owned work inventory. This is
/// prepared once before any worker starts and launches exactly one run before
/// a rescan replaces execute-once operator state.
pub(in crate::datafusion) struct PreparedParallelPlan {
    plan: CompiledPhysicalPlan,
    session: SessionContext,
    memory: Arc<PeakRecordingPool>,
    stages: ParallelStageCatalog,
    sources: Option<Box<[PreparedParallelSource]>>,
}

impl PreparedParallelPlan {
    pub(in crate::datafusion) fn try_new(
        plan: CompiledPhysicalPlan,
        session: SessionContext,
        sources: &ParallelSourceCatalog,
        worker_cap: u32,
        limits: QueryExecutionLimits,
    ) -> Result<Option<Self>, QueryExecutionError> {
        let stages = ParallelStageCatalog::discover(plan.plan())?;
        if stages.launch_width(worker_cap).is_none() {
            return Ok(None);
        }
        let Some(sources) = sources.take()? else {
            return Ok(None);
        };
        let resources = limits.runtime_env_for_plan(plan.plan())?;
        let state = SessionStateBuilder::new_from_existing(session.state())
            .with_runtime_env(resources.environment)
            .build();
        Ok(Some(Self {
            plan,
            session: SessionContext::new_with_state(state),
            memory: resources.memory,
            stages,
            sources: Some(sources),
        }))
    }

    pub(in crate::datafusion) fn plan(&self) -> &CompiledPhysicalPlan {
        &self.plan
    }

    pub(in crate::datafusion) fn peak_reserved(&self) -> usize {
        self.memory.peak_reserved()
    }
}

/// Backend-local callbacks and process services selected at BeginCustomScan.
pub struct ParallelQueryOptions {
    pub(in crate::datafusion) host: Arc<dyn ParallelExecutionHost>,
    pub(in crate::datafusion) callbacks: Box<[WorkerTableScanCallbacks]>,
    routes: Box<[ParallelSourceRoute]>,
    queue_bytes: usize,
}

impl ParallelQueryOptions {
    pub fn new(
        host: Arc<dyn ParallelExecutionHost>,
        scans: &[PlannedTableScan<'_>],
        callbacks: Vec<WorkerTableScanCallbacks>,
        queue_bytes: usize,
    ) -> Self {
        Self {
            host,
            callbacks: callbacks.into_boxed_slice(),
            routes: scans
                .iter()
                .map(|scan| ParallelSourceRoute {
                    kind: scan.route().kind().code(),
                    name: scan.route().name().to_owned(),
                })
                .collect(),
            queue_bytes,
        }
    }
}

/// One run's DSM prefix, reactor attachment and mapping liveness.
///
/// Field order is the release order the run needs: the mapping is still usable
/// while the transport ends the run, the reactor attachment goes away before
/// anything stops touching the mapping, and the mapping itself is released by
/// PostgreSQL after the worker group finishes.
pub(super) struct LeaderTransport {
    region: Arc<TransportRegion>,
    _attachment: ReactorAttachment,
    live: Arc<AtomicBool>,
    wakers: Arc<WakerSet>,
    host: Arc<dyn ParallelExecutionHost>,
}

impl LeaderTransport {
    pub(super) fn region(&self) -> &TransportRegion {
        &self.region
    }

    pub(super) fn wakers(&self) -> &WakerSet {
        &self.wakers
    }

    pub(super) fn live(&self) -> &AtomicBool {
        &self.live
    }

    /// Reports the end of the run to every worker while the mapping is
    /// attached.
    ///
    /// The shutdown flag is durable, so a worker that wakes up late still sees
    /// it; the wakeups are what get a parked worker to look.
    fn end_run(&self) {
        self.region.close();
        for worker in 1..=self.region.workers() {
            self.host.wake(self.region.receiver_token(worker));
        }
    }
}

impl Drop for LeaderTransport {
    fn drop(&mut self) {
        // A run whose mapping is already detached must not write to it.
        if self.live.load(Ordering::Acquire) {
            self.end_run();
        }
    }
}

/// A fresh distributed plan, transport and worker group for exactly one
/// execution. Rescan constructs another run rather than reusing execute-once
/// operator state.
pub(in crate::datafusion) struct ParallelRun {
    plan: CompiledPhysicalPlan,
    session: SessionContext,
    lifecycle: ParallelRunLifecycle,
    host: Arc<dyn ParallelExecutionHost>,
    metrics: Option<ParallelMetrics>,
    display_plan: Option<CompiledPhysicalPlan>,
    worker_count: u32,
}

/// Enforces a single-field parallel lifecycle: transport and worker ownership
/// either coexist or have both been released.
enum ParallelRunLifecycle {
    Running {
        transport: LeaderTransport,
        workers: Box<dyn ParallelWorkers>,
    },
    Finished,
}

impl ParallelRun {
    pub(in crate::datafusion) fn launch(
        prepared: &mut PreparedParallelPlan,
        options: &ParallelQueryOptions,
        limits: QueryExecutionLimits,
        runtime: &ParallelRuntime,
        execution_metrics: Option<&Arc<ExecutionMetrics>>,
    ) -> Result<Option<Self>, QueryExecutionError> {
        let plan = prepared.plan.clone();
        let session = prepared.session.clone();
        let Some(width) = prepared.stages.launch_width(options.host.worker_cap())
        else {
            return Ok(None);
        };
        // Both checks precede `host.launch`: a leader that cannot be woken would
        // park until PostgreSQL cancels it, and starting workers first would
        // leave them orphaned behind that wait.
        let Some(reactor) = runtime.reactor() else {
            return Ok(None);
        };
        if !options.host.wakeup_available() {
            return Ok(None);
        }
        let sources = prepared.sources.take().ok_or_else(|| {
            QueryExecutionError::DataFusion(DataFusionError::Internal(
                "parallel source inventory was already consumed".to_owned(),
            ))
        })?;
        let source_inventory =
            PreparedSourceInventory::try_new(sources, &options.routes)?;
        let bootstrap = ParallelBootstrap {
            sources: source_inventory.descriptors().to_vec(),
            work_mem_bytes: limits.work_mem_bytes() as u64,
            maximum_batch_rows: limits.maximum_batch_rows() as u32,
            collect_metrics: execution_metrics.is_some(),
            source_inventory_bytes: source_inventory.byte_len() as u64,
            hash_memory_bytes: limits.hash_memory_bytes() as u64,
            spill_enabled: limits.spill_enabled(),
        }
        .encode_to_vec();
        let layout =
            TransportLayout::new(width.get(), options.queue_bytes, bootstrap.len());
        let allocation_prefix = layout.allocation_prefix(width.get());
        let allocation = allocation_prefix
            .checked_add(source_inventory.byte_len())
            .ok_or_else(|| {
            QueryExecutionError::DataFusion(DataFusionError::Plan(
                "parallel DSM allocation size overflowed usize".to_owned(),
            ))
        })?;
        let Some(workers) = options
            .host
            .launch(width.get(), allocation)
            .map_err(QueryExecutionError::ParallelHost)?
        else {
            return Ok(None);
        };
        let mut launch = ParallelWorkerLaunch::new(workers);
        let attached = launch.workers().attached_workers();
        // SAFETY: the PG host allocated `allocation` bytes for `width` and leaves
        // workers behind the activation gate. PostgreSQL attaches at most the
        // launched width, so the attached prefix fits, and `layout` was built
        // from `bootstrap` above.
        let region = Arc::new(unsafe {
            TransportRegion::initialize(
                launch.workers().region(),
                attached,
                layout,
                &bootstrap,
            )
        });
        region.publish_token(0, options.host.receiver_token());
        // The transport owns the DSM prefix. Provider payloads occupy one
        // immutable flat tail shared by every participant; the compact
        // bootstrap above contains only route and offset/length descriptors.
        // SAFETY: the allocation reserves this prefix plus exactly the source
        // inventory length, and workers remain behind the activation gate.
        unsafe {
            source_inventory.write_to(
                launch
                    .workers()
                    .region()
                    .as_ptr()
                    .cast::<u8>()
                    .add(allocation_prefix),
            );
        }
        let live = Arc::new(AtomicBool::new(true));
        launch
            .workers()
            .watch_mapping(Arc::clone(&live))
            .map_err(QueryExecutionError::ParallelHost)?;
        let wakers = Arc::new(WakerSet::new(attached));
        let attachment = reactor.attach(Arc::clone(&wakers));
        let streams = StreamContext::new(
            Arc::clone(&region),
            Arc::clone(&options.host),
            Arc::clone(&wakers),
            Arc::clone(&live),
        );
        let session = ParallelSession::attach(session, streams, 0);
        let metrics = ParallelMetrics::new(plan.plan(), execution_metrics);
        launch.workers_mut().activate(allocation);
        if let Some(execution_metrics) = execution_metrics {
            source_inventory.record_task_plans(execution_metrics);
        }
        launch.install_transport(LeaderTransport {
            region,
            _attachment: attachment,
            live,
            wakers,
            host: Arc::clone(&options.host),
        });
        let (transport, workers) = launch.complete();
        Ok(Some(Self {
            plan,
            session,
            lifecycle: ParallelRunLifecycle::Running { transport, workers },
            host: Arc::clone(&options.host),
            metrics,
            display_plan: None,
            worker_count: attached,
        }))
    }

    pub(in crate::datafusion) fn execute(
        &self,
        runtime: &ParallelRuntime,
    ) -> Result<SendableRecordBatchStream, QueryExecutionError> {
        let _entered = runtime.runtime().enter();
        self.plan
            .execute(self.session.task_ctx())
            .map_err(QueryExecutionError::from)
    }

    pub(in crate::datafusion) fn next_batch(
        &self,
        runtime: &ParallelRuntime,
        stream: &mut SendableRecordBatchStream,
    ) -> Result<Option<RecordBatch>, QueryExecutionError> {
        let transport = self.transport();
        let outcome = runtime.block_on(async {
            let watch = RunWatch::new(
                transport.region(),
                transport.live(),
                transport.wakers(),
                RunRole::Leader,
            );
            tokio::select! {
                batch = stream.next() => Wait::Ready(batch),
                signal = watch => Wait::Signalled(signal),
            }
        })?;
        match outcome {
            Wait::Ready(batch) => {
                batch.transpose().map_err(QueryExecutionError::from)
            }
            Wait::Signalled(RunSignal::Abort | RunSignal::Shutdown) => {
                Err(QueryExecutionError::interrupted().into())
            }
        }
    }

    pub(in crate::datafusion) fn plan(&self) -> &CompiledPhysicalPlan {
        self.display_plan.as_ref().unwrap_or(&self.plan)
    }

    pub(in crate::datafusion) fn worker_count(&self) -> u32 {
        self.worker_count
    }

    fn transport(&self) -> &LeaderTransport {
        match &self.lifecycle {
            ParallelRunLifecycle::Running { transport, .. } => transport,
            ParallelRunLifecycle::Finished => {
                unreachable!("the executor reads batches before it finishes the run")
            }
        }
    }

    /// Waits until every worker reported its task metrics.
    ///
    /// `wait_for_metrics` has no failure exit: a failed worker never reports, so
    /// the wait is always raced against the run watch.
    fn collect_metrics(
        &self,
        runtime: &ParallelRuntime,
    ) -> Result<(), QueryExecutionError> {
        let Some(metrics) = &self.metrics else {
            return Ok(());
        };
        let transport = match &self.lifecycle {
            ParallelRunLifecycle::Running { transport, .. } => transport,
            ParallelRunLifecycle::Finished => {
                unreachable!("metrics are collected while the run is still running")
            }
        };
        let outcome = runtime.block_on(async {
            let watch = RunWatch::new(
                transport.region(),
                transport.live(),
                transport.wakers(),
                RunRole::Leader,
            );
            tokio::select! {
                reports = metrics.wait() => Wait::Ready(reports),
                signal = watch => Wait::Signalled(signal),
            }
        })?;
        match outcome {
            Wait::Ready(reports) => {
                if let Some(reports) = reports {
                    metrics.record(reports)?;
                }
                Ok(())
            }
            Wait::Signalled(RunSignal::Abort | RunSignal::Shutdown) => {
                Err(QueryExecutionError::interrupted().into())
            }
        }
    }

    /// Rewrites the plan the executor displays, with the metrics the workers
    /// reported.
    ///
    /// Runs while the reactor is still attached: it is the last `block_on` of
    /// this run.
    fn build_display_plan(
        &mut self,
        runtime: &ParallelRuntime,
    ) -> Result<(), QueryExecutionError> {
        let Some(metrics) = &self.metrics else {
            return Ok(());
        };
        let plan = runtime
            .block_on(metrics.plan_with_metrics(Arc::clone(self.plan.plan())))??;
        self.display_plan = Some(CompiledPhysicalPlan::try_new(plan)?);
        Ok(())
    }

    /// The output stream has already been dropped, so the coordinator channels
    /// have closed and every worker reports what it ran. Release order:
    /// metrics and the display plan (the last two readers of the transport),
    /// interrupts, then the lifecycle transition that ends the run and detaches
    /// the reactor, after which PostgreSQL joins the workers.
    pub(in crate::datafusion) fn finish(
        &mut self,
        runtime: &ParallelRuntime,
    ) -> Result<(), QueryExecutionError> {
        if matches!(self.lifecycle, ParallelRunLifecycle::Finished) {
            return Ok(());
        }
        let metrics = self.collect_metrics(runtime);
        let display = if metrics.is_ok() {
            self.build_display_plan(runtime)
        } else {
            Ok(())
        };
        let interrupts = self
            .host
            .process_interrupts()
            .map_err(QueryExecutionError::ParallelHost);
        let ParallelRunLifecycle::Running {
            transport,
            mut workers,
        } = mem::replace(&mut self.lifecycle, ParallelRunLifecycle::Finished)
        else {
            unreachable!("finish runs once for a running run")
        };
        drop(transport);
        let finish = workers.finish().map_err(QueryExecutionError::ParallelHost);
        // Interrupt and finish errors are captured Results at this layer. Keep
        // the metrics-phase error that occurred first while still running both
        // teardown operations above.
        metrics.and(display).and(interrupts).and(finish)
    }
}

impl Drop for ParallelRun {
    fn drop(&mut self) {
        let lifecycle =
            mem::replace(&mut self.lifecycle, ParallelRunLifecycle::Finished);
        let ParallelRunLifecycle::Running { transport, workers } = lifecycle else {
            return;
        };
        // Leave an unfinished ParallelContext to PostgreSQL's transaction-abort
        // cleanup. ResourceOwner callbacks run after locks
        // and must not call DestroyParallelContext, whose PG implementation
        // waits uninterruptibly for worker exit.
        drop(transport);
        workers.abandon();
    }
}

/// One outcome of a `select` against the run watch.
enum Wait<T> {
    Ready(T),
    Signalled(RunSignal),
}
