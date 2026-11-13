//! Query participant memory and spill policy.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool,
    MemoryReservation, PeakRecordingPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_distributed::BroadcastExec;

use crate::ExecutionProfile;

use super::error::QueryExecutionError;
use super::join_planning::AdaptiveHashJoinExec;
use super::spill::PgTempFileFactory;

mod worker;

pub(in crate::datafusion) use worker::{ParticipantMemoryRecorder, TaskMemoryPool};

/// DataFusion's default merge reservation, kept as the ceiling for larger
/// `work_mem`.
const MAXIMUM_SORT_MERGE_RESERVATION: usize = 10 * 1024 * 1024;

/// Shared by the `ExternalSorter[p]` and `ExternalSorterMerge[p]` consumers.
const SORT_CONSUMER_PREFIX: &str = "ExternalSorter";

pub(super) struct RuntimeResources {
    pub(super) environment: Arc<RuntimeEnv>,
    pub(super) memory: Arc<PeakRecordingPool>,
}

/// `FairSpillPool` that books a Sort's refusal up to its merge reservation.
///
/// `ExternalSorter` reserves twice a batch it already holds and the pool keeps
/// only accounts, so a refusal frees nothing and would end the query. Only Sort
/// gets the floor: other consumers also start empty, so size alone cannot
/// bound them.
#[derive(Debug)]
struct SortFloorPool {
    inner: FairSpillPool,
    sort_floor_bytes: usize,
}

impl SortFloorPool {
    fn admits_refused(&self, reservation: &MemoryReservation) -> bool {
        reservation.size() <= self.sort_floor_bytes
            && reservation
                .consumer()
                .name()
                .starts_with(SORT_CONSUMER_PREFIX)
    }
}

impl Display for SortFloorPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, formatter)
    }
}

impl MemoryPool for SortFloorPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, returned: usize) {
        self.inner.shrink(reservation, returned);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        match self.inner.try_grow(reservation, additional) {
            Err(_) if self.admits_refused(reservation) => {
                self.inner.grow(reservation, additional);
                Ok(())
            }
            outcome => outcome,
        }
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// PostgreSQL operation budgets and scan batch shape for one query execution.
///
/// Every executable plan or worker fragment gets its own pool. With spilling
/// enabled it is a `FairSpillPool`: the plan-wide total is fixed, so an
/// operator's share is not a hard per-operator limit. Without spilling the pool
/// is a `GreedyMemoryPool` limited to `usize::MAX`, because DataFusion picks a
/// different aggregate stream for a pool reporting `MemoryLimit::Infinite`.
#[derive(Debug, Clone, Copy)]
pub struct QueryExecutionLimits {
    work_mem_bytes: usize,
    hash_memory_bytes: usize,
    execution: ExecutionProfile,
    spill_enabled: bool,
}

impl QueryExecutionLimits {
    pub fn try_new(
        work_mem_bytes: usize,
        hash_memory_bytes: usize,
        execution: ExecutionProfile,
        spill_enabled: bool,
    ) -> Result<Self, QueryExecutionError> {
        if work_mem_bytes == 0 || hash_memory_bytes == 0 {
            return Err(QueryExecutionError::InvalidLimits);
        }
        Ok(Self::restore(
            work_mem_bytes,
            hash_memory_bytes,
            execution,
            spill_enabled,
        ))
    }

    /// Rebuild limits from a validated instance, such as the leader limits
    /// carried to a parallel worker.
    pub(in crate::datafusion) const fn restore(
        work_mem_bytes: usize,
        hash_memory_bytes: usize,
        execution: ExecutionProfile,
        spill_enabled: bool,
    ) -> Self {
        Self {
            work_mem_bytes,
            hash_memory_bytes,
            execution,
            spill_enabled,
        }
    }

    #[inline]
    pub(super) const fn maximum_batch_rows(self) -> usize {
        self.execution.maximum_batch_rows().get()
    }

    pub(super) const fn work_mem_bytes(self) -> usize {
        self.work_mem_bytes
    }

    pub(super) const fn hash_memory_bytes(self) -> usize {
        self.hash_memory_bytes
    }

    pub(super) const fn spill_enabled(self) -> bool {
        self.spill_enabled
    }

    fn sort_merge_reservation_bytes(self) -> usize {
        (self.work_mem_bytes / 4).min(MAXIMUM_SORT_MERGE_RESERVATION)
    }

    /// Session settings shared by leader planning and worker tasks; Sort reads
    /// its merge reservation from the session.
    pub(super) fn session_config(self, target_partitions: usize) -> SessionConfig {
        self.apply_session_policy(SessionConfig::new(), target_partitions)
    }

    /// Applies PostgreSQL's execution policy to a config that already carries
    /// the session's own options.
    pub(super) fn apply_session_policy(
        self,
        config: SessionConfig,
        target_partitions: usize,
    ) -> SessionConfig {
        let mut config = config
            .with_target_partitions(target_partitions)
            .with_batch_size(self.maximum_batch_rows());
        config.options_mut().optimizer.use_statistics_registry = true;
        if self.spill_enabled {
            config = config.with_sort_spill_reservation_bytes(
                self.sort_merge_reservation_bytes(),
            );
        }
        config
    }

    /// Bounded environment used only while constructing a physical plan.
    pub(super) fn planning_runtime_env(self) -> DataFusionResult<RuntimeResources> {
        self.runtime_env(self.work_mem_bytes)
    }

    /// Environment of one executable leader plan; a worker computes the same
    /// budget when its decoded plan reaches the rewrite hook.
    pub(super) fn runtime_env_for_plan(
        self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<RuntimeResources> {
        let bytes = self.pool_bytes(plan)?;
        self.runtime_env(bytes)
    }

    fn pool_bytes(self, plan: &Arc<dyn ExecutionPlan>) -> DataFusionResult<usize> {
        if self.spill_enabled {
            self.plan_memory_bytes(plan)
        } else {
            Ok(usize::MAX)
        }
    }

    /// Spilling needs a real total: the pool share is what tells an operator
    /// when to spill.
    fn plan_memory_bytes(
        self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<usize> {
        let mut bytes = 0usize;
        plan.apply(|node| {
            bytes = bytes
                .checked_add(self.node_memory_bytes(node)?)
                .ok_or_else(Self::budget_overflow)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(bytes.max(self.work_mem_bytes))
    }

    /// Budget for one node of one task: `hash_mem` for hash tables, `work_mem`
    /// for the rest. A task's partitions share a backend, so these
    /// per-partition budgets are wider than PostgreSQL's per-process allowance.
    fn node_memory_bytes(
        self,
        node: &Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<usize> {
        let partitions = node.output_partitioning().partition_count();
        let (per_partition, partitions) = if node.is::<HashJoinExec>() {
            // CollectLeft builds one table every partition probes; other joins
            // run under `AdaptiveHashJoinExec`.
            (self.hash_memory_bytes, 1)
        } else if node.is::<AdaptiveHashJoinExec>() {
            // A partition that falls back sorts both inputs, and the plan does
            // not show those Sorts.
            (self.work_mem_bytes, 2 * partitions)
        } else if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            if aggregate.group_expr().is_true_no_grouping() {
                (self.work_mem_bytes, partitions)
            } else {
                (self.hash_memory_bytes, partitions)
            }
        } else if node.is::<NestedLoopJoinExec>() {
            (self.work_mem_bytes, 1)
        } else if node.is::<BroadcastExec>() {
            // One cached build feeds every consumer task.
            (self.hash_memory_bytes, 1)
        } else if node.is::<SortExec>()
            || node.is::<SortMergeJoinExec>()
            || node.is::<RepartitionExec>()
        {
            (self.work_mem_bytes, partitions)
        } else {
            return Ok(0);
        };
        per_partition
            .checked_mul(partitions)
            .ok_or_else(Self::budget_overflow)
    }

    fn runtime_env(self, bytes: usize) -> DataFusionResult<RuntimeResources> {
        let memory = Arc::new(PeakRecordingPool::new(self.memory_pool(bytes)));
        let environment = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&memory) as Arc<dyn MemoryPool>)
            .with_disk_manager_builder(self.disk_manager())
            .build()?;
        Ok(RuntimeResources {
            environment: Arc::new(environment),
            memory,
        })
    }

    /// Memory admission policy for `bytes`, shared by leader plans and worker
    /// tasks.
    fn memory_pool(self, bytes: usize) -> Arc<dyn MemoryPool> {
        if self.spill_enabled {
            // TODO(datafusion-aggregate-spill): a grouped aggregate writes each
            // spill run in batches of up to `batch_size` rows whatever their
            // byte size, and the merge seats each run with twice its widest
            // batch, so a run with fewer groups than one batch fails with
            // ResourcesExhausted where PostgreSQL's soft `work_mem` would
            // continue. See apache/datafusion#25851 and #26987; recheck on every
            // DataFusion upgrade.
            Arc::new(SortFloorPool {
                inner: FairSpillPool::new(bytes),
                sort_floor_bytes: self.sort_merge_reservation_bytes(),
            })
        } else {
            Arc::new(GreedyMemoryPool::new(bytes))
        }
    }

    /// Disk policy shared by leader and worker runtime environments.
    fn disk_manager(self) -> DiskManagerBuilder {
        if self.spill_enabled {
            // TODO(datafusion-nlj-fallback): a temp-file disk manager also
            // enables NestedLoopJoin's fallback, which reruns its left child
            // without reset and loses rows under a finished CollectLeft build.
            // apache/datafusion#24676 is fixed on main but not in 55.0.0;
            // recheck on every DataFusion upgrade.
            PgTempFileFactory::disk_manager(self.work_mem_bytes)
        } else {
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled)
        }
    }

    fn budget_overflow() -> DataFusionError {
        DataFusionError::Plan(
            "physical-plan memory budget exceeds the host address space".to_owned(),
        )
    }
}
