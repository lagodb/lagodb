//! Execution and output routing for one worker-local fragment.

use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_distributed::shm::{
    CooperativeDrainSet, MppDataStreamKey as ParallelDataStreamKey,
    MppFrameHeader as ParallelFrameHeader, MppMesh as ParallelMesh,
    MppPartitionSink as ParallelPartitionSink, MppSender as ParallelSender,
    PartitionSink, collect_task_metrics, proc_for_task, run_execute_task_loop,
    run_worker_fragment,
};
use futures::FutureExt;
use pgrx::pg_sys::panic::{CaughtError, ErrorReportWithLevel};
use tokio_util::sync::CancellationToken;

use super::super::metrics::WorkerMemoryMetrics;
use super::super::stages::{
    ParallelStage, ParallelStageRouting, ParallelTaskAssignment,
};

pub(super) struct WorkerFragment {
    assignment: ParallelTaskAssignment,
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    sink: Arc<WorkerPartitionSink>,
    memory_metrics: Option<WorkerMemoryMetrics>,
}

impl WorkerFragment {
    pub(super) fn new(
        assignment: ParallelTaskAssignment,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        sink: Arc<WorkerPartitionSink>,
        memory_metrics: Option<WorkerMemoryMetrics>,
    ) -> Self {
        Self {
            assignment,
            plan,
            context,
            sink,
            memory_metrics,
        }
    }

    pub(super) async fn execute(&self) -> Result<()> {
        let assignment = self.assignment;
        let output_partitions = self.plan.output_partitioning().partition_count();
        run_execute_task_loop(
            &self.sink.mesh,
            assignment.stage_id,
            assignment.task_id,
            output_partitions,
            CancellationToken::new(),
            |_request, _headers, range| {
                let plan = Arc::clone(&self.plan);
                let context = Arc::clone(&self.context);
                let sink = Arc::clone(&self.sink);
                async move {
                    // Static planning inserts producer heads before dispatch;
                    // the specialized plan already contains broadcast/hash heads.
                    let sinks = range
                        .clone()
                        .map(|partition| sink.open(assignment.task_id, partition))
                        .collect::<Result<Vec<_>>>()?;
                    let execution = std::panic::AssertUnwindSafe(
                        run_worker_fragment(plan, sinks, context, range),
                    )
                    .catch_unwind()
                    .await;
                    match execution {
                        Ok(result) => result,
                        Err(payload) => {
                            // PostgreSQL-facing provider callbacks already use
                            // `CallbackErrorReport::capture_result` as their C ABI
                            // PgTryBuilder boundary. Their normal failure path reaches
                            // this future as `Ok(Err(DataFusionError::External(_)))`;
                            // do not add another PgTryBuilder here.
                            //
                            // This Rust task-request boundary only preserves the
                            // diagnostic if a typed pgrx panic escapes its owning
                            // boundary: `run_execute_task_loop` must publish TaskError
                            // before the outer PG entrypoint reports it. Its generic
                            // panic conversion only preserves `&str`/`String` and
                            // would otherwise emit "Box<dyn Any>".
                            let message = if let Some(report) =
                                payload.downcast_ref::<ErrorReportWithLevel>()
                            {
                                Some(report.message().to_owned())
                            } else {
                                payload.downcast_ref::<CaughtError>().map(|caught| {
                                    match caught {
                                        CaughtError::RustPanic {
                                            ereport, ..
                                        } => ereport.message().to_owned(),
                                        CaughtError::PostgresError(report)
                                        | CaughtError::ErrorReport(report) => {
                                            report.message().to_owned()
                                        }
                                    }
                                })
                            };
                            match message {
                                Some(message) => {
                                    Err(DataFusionError::Execution(message))
                                }
                                None => std::panic::resume_unwind(payload),
                            }
                        }
                    }
                }
            },
        )
        .await
    }

    pub(super) async fn send_metrics(&self) -> Result<()> {
        if let Some(base) = self.sink.outbound[0].as_ref() {
            let assignment = self.assignment;
            let sender = base.clone_with_header(ParallelFrameHeader::task_metrics(
                assignment.stage_id,
                assignment.task_id,
                self.sink.mesh.this_proc,
            ));
            let mut metrics = collect_task_metrics(
                &self.plan,
                assignment.task_id as usize,
                assignment.task_count as usize,
            );
            if let Some(memory) = &self.memory_metrics {
                memory.attach_to(&mut metrics);
            }
            sender.send_task_metrics_best_effort(&metrics).await?;
        }
        Ok(())
    }
}

pub(super) struct WorkerPartitionSink {
    stage: ParallelStage,
    routing: ParallelStageRouting,
    mesh: Arc<ParallelMesh>,
    outbound: Arc<Vec<Option<ParallelSender>>>,
}

impl WorkerPartitionSink {
    pub(super) fn new(
        stage: ParallelStage,
        routing: ParallelStageRouting,
        mesh: Arc<ParallelMesh>,
        outbound: Arc<Vec<Option<ParallelSender>>>,
    ) -> Self {
        Self {
            stage,
            routing,
            mesh,
            outbound,
        }
    }

    fn open(&self, task: u32, partition: usize) -> Result<Box<dyn PartitionSink>> {
        let destination = match self.routing {
            ParallelStageRouting::Leader => 0,
            ParallelStageRouting::NestedCoalesce => {
                proc_for_task(self.mesh.n_workers(), 0)
            }
            ParallelStageRouting::NestedPartitioned => {
                let consumer = self
                    .stage
                    .consumer_tasks
                    .get(partition)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "parallel stage {} has no consumer for partition {partition}",
                            self.stage.stage_id
                        ))
                    })?;
                proc_for_task(self.mesh.n_workers(), *consumer)
            }
        };
        let partition = u32::try_from(partition).map_err(|_| {
            DataFusionError::Plan(
                "parallel partition exceeds the transport address space".to_owned(),
            )
        })?;
        let key = ParallelDataStreamKey::new(self.stage.stage_id, task, partition);
        if destination == self.mesh.this_proc {
            return Ok(self.mesh.open_local_partition_sink(key));
        }
        let base = self.outbound[destination as usize]
            .as_ref()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "parallel worker has no sender for process {destination}"
                ))
            })?;
        let sender = base
            .clone_with_header(ParallelFrameHeader::batch(key, self.mesh.this_proc))
            .with_cooperative_drain(
                Arc::clone(&self.mesh) as Arc<dyn CooperativeDrainSet>
            );
        Ok(Box::new(ParallelPartitionSink::new(sender)))
    }
}
