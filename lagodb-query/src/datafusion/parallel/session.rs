//! Session configuration for leader planning and per-task worker execution.
//!
//! The leader plans on one session per statement; a worker builds one per task.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_distributed::{
    DistributedConfig, DistributedExt, SessionStateBuilderExt,
};

use super::codec::LagoPhysicalCodec;
use super::scan_exec::ParallelTableScanExec;
use super::transport::{
    ParallelChannelResolver, ParallelWorkerResolver, StreamContext,
};
use crate::datafusion::join_planning::QuerySessionRules;
use crate::datafusion::memory::QueryExecutionLimits;
use crate::plan::QueryFragment;

pub(in crate::datafusion) struct ParallelSession;

impl ParallelSession {
    /// The leader's session for one statement: distributed options, planning
    /// resolvers and the leader's codec.
    pub(in crate::datafusion) fn build(
        environment: Arc<RuntimeEnv>,
        limits: QueryExecutionLimits,
        worker_cap: usize,
        collect_metrics: bool,
        fragment: &QueryFragment,
    ) -> Result<SessionContext> {
        let mut config = limits.session_config(worker_cap);
        // Zero thresholds keep JoinSelection from promoting a Partitioned hash
        // join to CollectLeft. Dynamic filters stay off: parallel scans accept
        // none, and a Partitioned filter waits for reports that
        // `AdaptiveHashJoinExec` cannot give once a partition leaves the join.
        let optimizer = &mut config.options_mut().optimizer;
        optimizer.hash_join_single_partition_threshold = 0;
        optimizer.hash_join_single_partition_threshold_rows = 0;
        optimizer.enable_join_dynamic_filter_pushdown = false;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(environment)
            .with_default_features()
            .with_distributed_option_extension(DistributedConfig::default())
            .with_distributed_worker_resolver(ParallelWorkerResolver::planning(
                worker_cap as u32,
            ))
            .with_distributed_desired_task_count_handler(
                ParallelTableScanExec::desired_task_count,
            )
            .with_distributed_desired_task_count_handler(worker_cap)
            .with_distributed_scale_up_leaf_node_handler(
                ParallelTableScanExec::scale_up,
            )
            .with_distributed_user_codec(LagoPhysicalCodec::leader())
            .with_distributed_metrics_collection(collect_metrics)?
            // Scan counters carry ScanId as a metric label, which DSD's
            // worker-side aggregate_by_name drops, so merge per-partition
            // metrics on the leader instead.
            .with_distributed_aggregate_metrics_on_workers_before_collection(false)?
            .with_distributed_dynamic_filter_collection(false)?
            .with_distributed_broadcast_joins(true)?
            // The rings live in this machine's memory, so compression would only
            // burn CPU on both ends.
            .with_distributed_compression(None)?
            // A parallel scan accepts no dynamic filter.
            .with_distributed_remote_dynamic_filters(false)?
            .with_distributed_planner()
            .build();
        Ok(SessionContext::new_with_state(
            QuerySessionRules::new(fragment, limits).apply(state),
        ))
    }

    /// Installs the transport's resolvers for one run, so the router addresses
    /// only the workers PostgreSQL attached.
    pub(in crate::datafusion) fn attach(
        session: SessionContext,
        streams: StreamContext,
        this_proc: u32,
    ) -> SessionContext {
        let state = session
            .state()
            .with_distributed_worker_resolver(ParallelWorkerResolver::attached(
                streams.region().workers(),
            ))
            .with_distributed_channel_resolver(ParallelChannelResolver::new(
                streams, this_proc,
            ));
        SessionContext::new_with_state(state)
    }
}
