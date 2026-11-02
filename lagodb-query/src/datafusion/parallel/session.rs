//! Shared session configuration and the two transport adapter seams.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_distributed::shm::{
    InProcessWorkerResolver, Interrupt, MppMesh as ParallelMesh, ShmChannelResolver,
    Wakeup,
};
use datafusion_distributed::{
    DistributedConfig, DistributedExt, SessionStateBuilderExt,
};

use super::codec::{LagoPhysicalCodec, LagoStagePlanDispatch};
use super::host::ParallelExecutionHost;
use super::scan_exec::ParallelTableScanExec;
use crate::datafusion::error::QueryExecutionError;
use crate::datafusion::join_planning::QuerySessionRules;
use crate::datafusion::memory::QueryExecutionLimits;
use crate::plan::QueryFragment;

pub(in crate::datafusion) struct ParallelSession;

impl ParallelSession {
    /// `fragment` is the query being planned; a worker only decodes finished
    /// stage plans and passes `None`.
    pub(in crate::datafusion) fn build(
        environment: Arc<RuntimeEnv>,
        limits: QueryExecutionLimits,
        worker_cap: usize,
        collect_metrics: bool,
        fragment: Option<&QueryFragment>,
    ) -> Result<SessionContext> {
        let mut config = limits.session_config(worker_cap);
        // Zero thresholds keep JoinSelection from promoting a Partitioned hash
        // join to CollectLeft. PostgreSQL's estimates reach it through
        // `QuerySessionRules` to pick build sides, and a broadcast sized from
        // them would have no way back from an estimate that proves too small.
        //
        // Join dynamic filters stay off here. Parallel scans accept none, and a
        // Partitioned filter waits for a report from every partition, which
        // `AdaptiveHashJoinExec` cannot give once a partition leaves the hash
        // join.
        let optimizer = &mut config.options_mut().optimizer;
        optimizer.hash_join_single_partition_threshold = 0;
        optimizer.hash_join_single_partition_threshold_rows = 0;
        optimizer.enable_join_dynamic_filter_pushdown = false;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(environment)
            .with_default_features()
            .with_distributed_option_extension(DistributedConfig::default())
            .with_distributed_worker_resolver(InProcessWorkerResolver::new(
                worker_cap,
            ))
            .with_distributed_desired_task_count_handler(
                ParallelTableScanExec::desired_task_count,
            )
            .with_distributed_desired_task_count_handler(worker_cap)
            .with_distributed_scale_up_leaf_node_handler(
                ParallelTableScanExec::scale_up,
            )
            .with_distributed_user_codec(LagoPhysicalCodec::leader())
            .with_distributed_dispatch_plan_source(LagoStagePlanDispatch)
            .with_distributed_metrics_collection(collect_metrics)?
            .with_distributed_dynamic_filter_collection(false)?
            .with_distributed_broadcast_joins(true)?
            .with_distributed_planner()
            .build();
        Ok(SessionContext::new_with_state(match fragment {
            Some(fragment) => QuerySessionRules::new(fragment, limits).apply(state),
            None => state,
        }))
    }

    pub(in crate::datafusion) fn attach(
        session: SessionContext,
        mesh: Arc<ParallelMesh>,
    ) -> SessionContext {
        let state = session
            .state()
            .with_distributed_channel_resolver(ShmChannelResolver::new(mesh));
        SessionContext::new_with_state(state)
    }
}

pub(super) struct TransportHost<H: ParallelExecutionHost + ?Sized>(pub Arc<H>);

impl<H: ParallelExecutionHost + ?Sized> Wakeup for TransportHost<H> {
    fn wake(&self, token: u64) {
        self.0.wake(token);
    }
}

impl<H: ParallelExecutionHost + ?Sized> Interrupt for TransportHost<H> {
    fn check(&self) -> Result<()> {
        // Only cancel/die are execution interrupts. Task-scoped parallel worker failures use the
        // transport's TaskError protocol to make the active stream return;
        // `ParallelMessagePending` is then serviced at the idle-runtime boundary in
        // `ParallelRun::next_batch`.
        if self.0.interrupt_pending() {
            return Err(QueryExecutionError::interrupted());
        }
        Ok(())
    }
}
