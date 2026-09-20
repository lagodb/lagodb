//! Shared session configuration and the two transport adapter seams.

use std::sync::Arc;

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_distributed::shm::{
    InProcessWorkerResolver, Interrupt, MppMesh as ParallelMesh, ShmChannelResolver,
    Wakeup,
};
use datafusion_distributed::{
    DistributedConfig, DistributedExt, SessionStateBuilderExt,
};

use super::codec::LagoPhysicalCodec;
use super::host::ParallelExecutionHost;
use super::{LagoStagePlanDispatch, ParallelTableScanExec};

pub(in crate::datafusion) struct ParallelSession;

impl ParallelSession {
    pub(in crate::datafusion) fn build(
        environment: Arc<RuntimeEnv>,
        maximum_batch_rows: usize,
        worker_cap: usize,
        collect_metrics: bool,
        has_having: bool,
    ) -> Result<SessionContext> {
        let config = SessionConfig::new()
            .with_target_partitions(worker_cap)
            .with_batch_size(maximum_batch_rows);
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
        let state = if has_having {
            let rules = state
                .optimizers()
                .iter()
                .filter(|rule| rule.name() != "push_down_filter")
                .cloned()
                .collect();
            SessionStateBuilder::new_from_existing(state)
                .with_optimizer_rules(rules)
                .build()
        } else {
            state
        };
        Ok(SessionContext::new_with_state(state))
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
            return Err(DataFusionError::Execution(
                "parallel query interrupted".to_owned(),
            ));
        }
        Ok(())
    }
}
