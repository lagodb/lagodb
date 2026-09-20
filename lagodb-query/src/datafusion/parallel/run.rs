//! Plan-first launch and the leader's one-run transport owner.

use std::mem;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::PeakRecordingPool;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::{SendableRecordBatchStream, context::SessionContext};
use datafusion_distributed::shm::{
    CooperativeDrainSet, LeaderSession, dsm_region_bytes, leader_setup,
};
use futures::StreamExt;
use prost::Message;
use tokio::runtime::Runtime;

use super::bootstrap::ParallelBootstrap;
use super::catalog::{ParallelSourceCatalog, PreparedParallelSource};
use super::host::{ParallelExecutionHost, ParallelInterruptGuard, ParallelWorkers};
use super::lifecycle::ParallelWorkerLaunch;
use super::metrics::ParallelMetrics;
use super::session::{ParallelSession, TransportHost};
use super::source_inventory::{ParallelSourceRoute, PreparedSourceInventory};
use super::stages::ParallelStageCatalog;
use crate::datafusion::metrics::ExecutionMetrics;
use crate::datafusion::physical_plan::CompiledPhysicalPlan;
use crate::datafusion::{
    QueryExecutionError, QueryExecutionLimits, WorkerTableScanCallbacks,
};
use crate::plan::PlannedTableScan;

const METRICS_GRACE_PASSES: usize = 100;
const METRICS_GRACE_INTERVAL: Duration = Duration::from_millis(1);

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
    debug_deadlock_detector: bool,
}

impl ParallelQueryOptions {
    pub fn new(
        host: Arc<dyn ParallelExecutionHost>,
        scans: &[PlannedTableScan<'_>],
        callbacks: Vec<WorkerTableScanCallbacks>,
        queue_bytes: usize,
        debug_deadlock_detector: bool,
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
            debug_deadlock_detector,
        }
    }
}

/// A fresh distributed plan, mesh and worker group for exactly one execution.
/// Rescan constructs another run rather than reusing execute-once operator state.
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
        transport: LeaderSession,
        workers: Box<dyn ParallelWorkers>,
    },
    Finished,
}

impl ParallelRun {
    pub(in crate::datafusion) fn launch(
        prepared: &mut PreparedParallelPlan,
        options: &ParallelQueryOptions,
        limits: QueryExecutionLimits,
        execution_metrics: Option<&Arc<ExecutionMetrics>>,
    ) -> Result<Option<Self>, QueryExecutionError> {
        let plan = prepared.plan.clone();
        let session = prepared.session.clone();
        let stages = &prepared.stages;
        let Some(width) = stages.launch_width(options.host.worker_cap()) else {
            return Ok(None);
        };
        let sources = prepared.sources.take().ok_or_else(|| {
            QueryExecutionError::DataFusion(DataFusionError::Internal(
                "parallel source inventory was already consumed".to_owned(),
            ))
        })?;
        let source_inventory =
            PreparedSourceInventory::try_new(sources, &options.routes)?;
        let bytes = ParallelBootstrap {
            stages: stages.stages().to_vec(),
            sources: source_inventory.descriptors().to_vec(),
            work_mem_bytes: limits.work_mem_bytes() as u64,
            maximum_batch_rows: limits.maximum_batch_rows() as u32,
            collect_metrics: execution_metrics.is_some(),
            source_inventory_bytes: source_inventory.encoded_byte_len()?,
            debug_deadlock_detector: options.debug_deadlock_detector,
            hash_memory_bytes: limits.hash_memory_bytes() as u64,
        }
        .encode_to_vec();
        let transport_allocation =
            dsm_region_bytes(width.get() + 1, options.queue_bytes, bytes.len())?;
        let allocation = transport_allocation
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
        let actual_transport_bytes = dsm_region_bytes(
            launch.workers().attached_workers() + 1,
            options.queue_bytes,
            bytes.len(),
        )?;
        let adapter = Arc::new(TransportHost(Arc::clone(&options.host)));
        // Transport governance intentionally follows the currently selected
        // LagoDB fork protocol. The fork still
        // has two known protocol debts: inbound demux can accumulate an
        // unbounded VecDeque<RecordBatch> outside the DataFusion memory pool;
        // and coordinator SetPlan build/send failures are logged instead of
        // returned. Keep the issues at this protocol boundary until they are
        // governed in the fork, rather than adding incompatible wrapper-side
        // checks here. A run owns a fresh DSM, mesh and worker group, so frames
        // cannot cross an execution boundary and need no extra run-id field.
        // SAFETY: the PG host allocated `allocation` bytes and leaves workers
        // behind the activation gate. The actual attached width needs no more
        // space than the requested width; all handles stay within this owner.
        let transport = unsafe {
            leader_setup(
                launch.workers().region().as_ptr(),
                launch.workers().attached_workers() + 1,
                options.queue_bytes,
                &bytes,
                adapter.clone(),
                options.host.receiver_token(),
                adapter,
                true,
            )
        }?;
        // The transport owns the DSM prefix. Provider payloads occupy one
        // immutable flat tail shared by every participant; the compact
        // bootstrap above contains only route and offset/length descriptors.
        // The requested-width prefix bounds the actual short-launch prefix,
        // so the two regions cannot overlap.
        debug_assert!(actual_transport_bytes <= transport_allocation);
        // SAFETY: `allocation` reserves this prefix plus exactly the source
        // inventory length, and workers remain behind the activation gate.
        let destination = unsafe {
            launch
                .workers()
                .region()
                .as_ptr()
                .cast::<u8>()
                .add(transport_allocation)
        };
        // SAFETY: the destination is the non-overlapping inventory tail sized
        // from `source_inventory.byte_len()` above.
        unsafe { source_inventory.write_to(destination) };
        launch.install_transport(transport);
        launch
            .workers()
            .watch_mapping(launch.transport().mesh.detached_flag())
            .map_err(QueryExecutionError::ParallelHost)?;
        let session =
            ParallelSession::attach(session, Arc::clone(&launch.transport().mesh));
        let metrics = ParallelMetrics::new(
            plan.plan(),
            &launch.transport().mesh,
            execution_metrics,
        )?;
        launch.workers_mut().activate(allocation);
        if let Some(execution_metrics) = execution_metrics {
            source_inventory.record_task_plans(execution_metrics);
        }
        let worker_count = launch.workers().attached_workers();
        let (transport, workers) = launch.complete();
        Ok(Some(Self {
            plan,
            session,
            lifecycle: ParallelRunLifecycle::Running { transport, workers },
            host: Arc::clone(&options.host),
            metrics,
            display_plan: None,
            worker_count,
        }))
    }

    pub(in crate::datafusion) fn execute(
        &self,
        runtime: &Runtime,
    ) -> Result<SendableRecordBatchStream, QueryExecutionError> {
        let _entered = runtime.enter();
        self.plan
            .execute(self.session.task_ctx())
            .map_err(QueryExecutionError::from)
    }

    pub(in crate::datafusion) fn next_batch(
        &self,
        runtime: &Runtime,
        stream: &mut SendableRecordBatchStream,
    ) -> Result<Option<RecordBatch>, QueryExecutionError> {
        let result = {
            let _held = ParallelInterruptGuard::new(self.host.as_ref());
            runtime.block_on(async { stream.next().await })
        };
        // `ParallelMessagePending` deliberately does not interrupt this Tokio wait: it also
        // represents notices and normal worker exits, and PostgreSQL must not process it while a
        // runtime and its Rust-owned plans are live on the stack. Fragment failures do not rely on
        // that flag to unblock the data path. `WorkerFragment::execute` converts a typed pgrx panic
        // to DataFusionError inside the task-request scope, then run_execute_task_loop publishes a
        // DSD TaskError to the requester. The TaskError terminates this stream first; once the
        // runtime is idle, the PostgreSQL error queue remains the authoritative reporting path.
        self.host
            .process_interrupts()
            .map_err(QueryExecutionError::ParallelHost)?;
        result.transpose().map_err(QueryExecutionError::from)
    }

    pub(in crate::datafusion) fn plan(&self) -> &CompiledPhysicalPlan {
        self.display_plan.as_ref().unwrap_or(&self.plan)
    }

    pub(in crate::datafusion) fn worker_count(&self) -> u32 {
        self.worker_count
    }

    fn drain_metrics_before_finish(
        &mut self,
        runtime: &Runtime,
        transport: &LeaderSession,
    ) -> Result<(), QueryExecutionError> {
        let host = self.host.as_ref();
        let Some(metrics) = &mut self.metrics else {
            // With metrics disabled, workers publish no teardown telemetry.
            // Normal streams have reached EOF; dropped streams sent bounded
            // Cancel frames from their guards, so PG can join workers directly.
            return Ok(());
        };

        // Metrics and their transport drain are best-effort. Keep driving the
        // current-thread runtime for a short bounded grace window while freeing
        // ring slots, but never promote telemetry failure into a query error.
        for _ in 0..METRICS_GRACE_PASSES {
            let complete = {
                let _held = ParallelInterruptGuard::new(host);
                runtime.block_on(async {
                    let _ = transport.mesh.try_drain_pass();
                    let _ = metrics.drain();
                    let complete = metrics.complete();
                    if !complete {
                        tokio::time::sleep(METRICS_GRACE_INTERVAL).await;
                    }
                    complete
                })
            };
            host.process_interrupts()
                .map_err(QueryExecutionError::ParallelHost)?;
            if complete {
                break;
            }
        }

        // Capture frames published during the last grace interval before the
        // transport is dropped. Incomplete metrics remain best-effort.
        let _ = transport.mesh.try_drain_pass();
        let _ = metrics.drain();
        Ok(())
    }

    /// The output stream has already been dropped, so per-stream transport
    /// cancellation has reached producers that did not run to EOF. Drain
    /// best-effort metrics while the mesh is mapped, then let PostgreSQL join
    /// every worker.
    pub(in crate::datafusion) fn finish(
        &mut self,
        runtime: &Runtime,
    ) -> Result<(), QueryExecutionError> {
        let lifecycle =
            mem::replace(&mut self.lifecycle, ParallelRunLifecycle::Finished);
        let ParallelRunLifecycle::Running {
            transport,
            mut workers,
        } = lifecycle
        else {
            return Ok(());
        };
        let metrics = self.drain_metrics_before_finish(runtime, &transport);
        let interrupts = self
            .host
            .process_interrupts()
            .map_err(QueryExecutionError::ParallelHost);
        drop(transport);
        let finish = workers.finish().map_err(QueryExecutionError::ParallelHost);
        // Interrupt and finish errors are captured Results at this layer. Keep
        // the metrics-phase error that occurred first while still running both
        // teardown operations above.
        let result = metrics.and(interrupts).and(finish);
        if let Some(metrics) = &self.metrics {
            metrics.record_report_counts();
        }
        if result.is_ok()
            && let Some(metrics) = &self.metrics
            && metrics.complete()
        {
            let plan = runtime
                .block_on(metrics.plan_with_metrics(Arc::clone(self.plan.plan())))?;
            self.display_plan = Some(CompiledPhysicalPlan::try_new(plan)?);
        }
        result
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
