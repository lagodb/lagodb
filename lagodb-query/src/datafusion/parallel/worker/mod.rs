//! Worker-local decode, request execution and source release.

mod fragment;

use std::ffi::CStr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{TaskContext, context::SessionContext};
use datafusion_distributed::DistributedTaskContext;
use datafusion_distributed::shm::{
    CooperativeDrainSet, MppMesh as ParallelMesh, MppSender as ParallelSender,
    region_total, worker_setup,
};
use datafusion_proto::physical_plan::{
    DeduplicatingProtoConverter, PhysicalPlanNodeExt,
};
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::future::try_join_all;
use lagodb_core::query_contract::{TableScanRoute, TableScanRouteKind};
use prost::Message;
use tokio::runtime::Builder;

use super::bootstrap::ParallelBootstrap;
use super::catalog::WorkerSourceCatalog;
use super::codec::LagoPhysicalCodec;
use super::host::{ParallelInterruptGuard, ParallelWorkerHost};
use super::metrics::WorkerMemoryMetrics;
use super::session::{ParallelSession, TransportHost};
use super::source_inventory::MappedSourceInventory;
use super::stages::{ParallelStageCatalog, ParallelStageRouting};
use crate::ExecutionProfile;
use crate::datafusion::error::QueryExecutionError;
use crate::datafusion::memory::ParticipantMemoryRecorder;
use crate::datafusion::memory::QueryExecutionLimits;

use fragment::{WorkerFragment, WorkerPartitionSink};

const DEBUG_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Drive a restored PG parallel worker using its leader-initialized mapping.
///
/// # Safety
/// The host must retain its attached mapping, invoke this on the backend main
/// thread after activation, and report the returned error only after it returns.
pub unsafe fn run_parallel_worker<H: ParallelWorkerHost + 'static>(
    host: Arc<H>,
) -> Result<(), QueryExecutionError> {
    let adapter = Arc::new(TransportHost(Arc::clone(&host)));
    // SAFETY: the caller supplies the active PG mapping. The transport header
    // owns only the mapping prefix; the published mapping length also includes
    // the shared source-inventory tail.
    let transport_bytes =
        unsafe { region_total(host.region().as_ptr().cast_const()) };
    // SAFETY: `transport_bytes` was read from this initialized mapping's
    // transport header; the host keeps the mapping attached for this call.
    // Keep this as the first fallible engine operation after activation.  The
    // errors before sender attachment only validate the leader-published DSM
    // layout and PostgreSQL's dense worker index; bootstrap decoding starts
    // only after `WorkerSession` has registered this worker's senders.
    let transport = unsafe {
        worker_setup(
            host.region().as_ptr(),
            transport_bytes,
            host.process_index(),
            adapter.clone(),
            host.receiver_token(),
            adapter,
        )
    }?;
    host.watch_mapping(transport.mesh.detached_flag())
        .map_err(QueryExecutionError::ParallelHost)?;
    let bootstrap = ParallelBootstrap::decode(transport.plan_bytes.as_slice())
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "failed to decode parallel source inventory: {error}"
            ))
        })?;
    let inventory_len =
        usize::try_from(bootstrap.source_inventory_bytes).map_err(|_| {
            DataFusionError::Plan(
                "parallel source inventory exceeds this platform".to_owned(),
            )
        })?;
    // SAFETY: activation publishes the complete mapping length only after the
    // leader has initialized both the transport prefix and inventory tail.
    let source_inventory = unsafe {
        MappedSourceInventory::try_new(
            host.region(),
            host.region_bytes(),
            transport_bytes,
            inventory_len,
        )
    }?;
    let profile = ExecutionProfile::try_new(bootstrap.maximum_batch_rows as usize)
        .map_err(|error| DataFusionError::Plan(error.to_string()))?;
    let work_mem_bytes = usize::try_from(bootstrap.work_mem_bytes).map_err(|_| {
        DataFusionError::Plan(
            "parallel participant memory budget exceeds this platform".to_owned(),
        )
    })?;
    let hash_memory_bytes =
        usize::try_from(bootstrap.hash_memory_bytes).map_err(|_| {
            DataFusionError::Plan(
                "parallel hash memory budget exceeds this platform".to_owned(),
            )
        })?;
    let limits =
        QueryExecutionLimits::try_new(work_mem_bytes, hash_memory_bytes, profile)?;
    // The shared session is used for plan decoding. Each executable fragment
    // installs its own plan-sized RuntimeEnv below.
    let memory = limits.planning_runtime_env()?;
    let session = ParallelSession::build(
        memory.environment,
        limits.maximum_batch_rows(),
        transport.mesh.n_workers() as usize,
        bootstrap.collect_metrics,
        false,
    )?;
    let session = ParallelSession::attach(session, Arc::clone(&transport.mesh));
    let runtime = Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(QueryExecutionError::Runtime)?;
    let mut sources = Vec::with_capacity(bootstrap.sources.len());
    let source_result: Result<()> = (|| {
        for source in bootstrap.sources {
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
                    .decode(source_inventory.payload(&source)?)
                    .map_err(|error| DataFusionError::External(Box::new(error)))?,
            ));
        }
        Ok(())
    })();
    let source_host: Arc<dyn ParallelWorkerHost> = host.clone();
    let sources = WorkerSourceCatalog::new(sources, source_host);
    let result = match source_result {
        Err(error) => Err(error),
        Ok(()) => {
            let stages = ParallelStageCatalog::from_stages(bootstrap.stages);
            let outbound = Arc::new(transport.outbound_senders().to_vec());
            let _held = ParallelInterruptGuard::new(host.as_ref());
            runtime.block_on(
                WorkerExecution {
                    stages: &stages,
                    session: &session,
                    sources: &sources,
                    mesh: Arc::clone(&transport.mesh),
                    outbound,
                    limits,
                    collect_metrics: bootstrap.collect_metrics,
                    debug_deadlock_detector: bootstrap.debug_deadlock_detector,
                }
                .run(),
            )
        }
    };
    // All fragment futures/plans have dropped before sources are released.
    drop(session);
    drop(runtime);
    let cleanup = sources.close();
    drop(transport);
    let interrupts = host
        .process_interrupts()
        .map_err(QueryExecutionError::ParallelHost);
    // PgTryBuilder turns PostgreSQL ERROR into this Result; it does not report
    // immediately. Preserve the already-observed execution/cleanup error when
    // the final interrupt service also returns an error.
    result
        .map_err(QueryExecutionError::from)
        .and(cleanup.map_err(QueryExecutionError::from))
        .and(interrupts)
}

struct WorkerExecution<'a> {
    stages: &'a ParallelStageCatalog,
    session: &'a SessionContext,
    sources: &'a WorkerSourceCatalog,
    mesh: Arc<ParallelMesh>,
    outbound: Arc<Vec<Option<ParallelSender>>>,
    limits: QueryExecutionLimits,
    collect_metrics: bool,
    debug_deadlock_detector: bool,
}

impl WorkerExecution<'_> {
    async fn run(self) -> Result<()> {
        let width = NonZeroU32::new(self.mesh.n_workers())
            .expect("worker mesh includes producer processes");
        let codec = LagoPhysicalCodec::worker(self.sources.clone()).combined();
        let participant_memory = self
            .collect_metrics
            .then(|| Arc::new(ParticipantMemoryRecorder::default()));
        let mut fragments = Vec::new();
        for assignment in self.stages.assignments(self.mesh.this_proc, width) {
            let frame = {
                let receive = self
                    .mesh
                    .take_set_plan(assignment.stage_id, assignment.task_id);
                tokio::pin!(receive);
                loop {
                    tokio::select! {
                        frame = &mut receive => break frame?,
                        _ = tokio::time::sleep(Duration::from_millis(1)) => {
                            self.mesh.try_drain_pass()?;
                            self.mesh.check_interrupt()?;
                        }
                    }
                }
            };
            let (request, _) = frame.into_parts()?;
            let config =
                self.session
                    .state()
                    .config()
                    .clone()
                    .with_extension(Arc::new(DistributedTaskContext {
                        task_index: assignment.task_id as usize,
                        task_count: assignment.task_count as usize,
                    }));
            let decode_context = Arc::new(
                TaskContext::from(self.session).with_session_config(config.clone()),
            );
            let proto = PhysicalPlanNode::decode(request.plan_proto.as_slice())
                .map_err(|error| {
                    DataFusionError::Plan(format!(
                        "failed to decode parallel stage plan: {error}"
                    ))
                })?;
            let plan = proto.try_into_physical_plan_with_converter(
                &decode_context,
                &codec,
                &DeduplicatingProtoConverter::default(),
            )?;
            let resources = match &participant_memory {
                Some(participant) => self
                    .limits
                    .runtime_env_for_fragment(&plan, Arc::clone(participant))?,
                None => self.limits.runtime_env_for_plan(&plan)?,
            };
            let memory_metrics = participant_memory.as_ref().map(|participant| {
                WorkerMemoryMetrics::new(
                    Arc::clone(&resources.memory),
                    Arc::clone(participant),
                )
            });
            let context = Arc::new(
                TaskContext::from(self.session)
                    .with_session_config(config)
                    .with_runtime(resources.environment),
            );
            let stage = self
                .stages
                .stages()
                .iter()
                .find(|stage| stage.stage_id == assignment.stage_id)
                .expect("assignment belongs to the stage inventory")
                .clone();
            let routing =
                ParallelStageRouting::try_from(stage.routing).map_err(|_| {
                    DataFusionError::Plan(format!(
                        "parallel stage {} has unknown routing {}",
                        stage.stage_id, stage.routing,
                    ))
                })?;
            let sink = Arc::new(WorkerPartitionSink::new(
                stage,
                routing,
                Arc::clone(&self.mesh),
                Arc::clone(&self.outbound),
            ));
            fragments.push(WorkerFragment::new(
                assignment,
                plan,
                context,
                sink,
                memory_metrics,
            ));
        }
        // Network-boundary execution constructs every upstream stream eagerly.
        // Dropping a consumer before EOF sends a stream-level Cancel to its
        // producer; fragment request loops therefore finish without a separate
        // worker-wide cancellation monitor.
        let execution = async {
            try_join_all(fragments.iter().map(WorkerFragment::execute))
                .await
                .map(|_| ())
        };
        let result = if self.debug_deadlock_detector {
            match tokio::time::timeout(DEBUG_DEADLOCK_TIMEOUT, execution).await {
                Ok(result) => result,
                Err(_) => Err(DataFusionError::Internal(
                    "parallel worker fragment execution exceeded 30 seconds; deadlock detector triggered"
                        .to_owned(),
                )),
            }
        } else {
            execution.await
        };
        let mut metrics_error = None;
        if self.collect_metrics {
            for fragment in &fragments {
                let sent = fragment.send_metrics().await;
                if metrics_error.is_none() {
                    metrics_error = sent.err();
                }
            }
        }
        result.and(match metrics_error {
            Some(error) => Err(error),
            None => Ok(()),
        })
    }
}
