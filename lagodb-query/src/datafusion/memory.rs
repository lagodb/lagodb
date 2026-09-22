//! Query participant memory and spill policy.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
    PeakRecordingPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::ExecutionProfile;

use super::error::QueryExecutionError;

pub(super) struct RuntimeResources {
    pub(super) environment: Arc<RuntimeEnv>,
    pub(super) memory: Arc<PeakRecordingPool>,
}

/// Worker-local high-water mark across independently bounded fragment pools.
///
/// The recorder observes reservation deltas only when execution metrics are
/// enabled. It never participates in admission, so one fragment cannot consume
/// another fragment's PostgreSQL operation budget.
#[derive(Debug, Default)]
pub(super) struct ParticipantMemoryRecorder {
    reserved: AtomicUsize,
    peak: AtomicUsize,
}

impl ParticipantMemoryRecorder {
    pub(super) fn peak_reserved(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    fn grow(&self, additional: usize) {
        let reserved =
            self.reserved.fetch_add(additional, Ordering::Relaxed) + additional;
        self.peak.fetch_max(reserved, Ordering::Relaxed);
    }

    fn shrink(&self, returned: usize) {
        self.reserved.fetch_sub(returned, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ParticipantRecordingPool {
    fragment: Arc<PeakRecordingPool>,
    participant: Arc<ParticipantMemoryRecorder>,
}

impl Display for ParticipantRecordingPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.fragment, formatter)
    }
}

impl MemoryPool for ParticipantRecordingPool {
    fn name(&self) -> &str {
        self.fragment.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.fragment.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.fragment.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.fragment.grow(reservation, additional);
        self.participant.grow(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, returned: usize) {
        self.fragment.shrink(reservation, returned);
        self.participant.shrink(returned);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<(), DataFusionError> {
        self.fragment.try_grow(reservation, additional)?;
        self.participant.grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.fragment.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.fragment.memory_limit()
    }
}

/// PostgreSQL operation budgets and scan batch shape for one query execution.
///
/// Every executable plan or worker fragment receives its own pool. The pool
/// rejects growth that reserves before allocation. DataFusion 55's
/// native DISTINCT accumulators update their state before resizing their
/// reservation, so this type does not claim a reservation-first hard limit for
/// that state.
#[derive(Debug, Clone, Copy)]
pub struct QueryExecutionLimits {
    work_mem_bytes: usize,
    hash_memory_bytes: usize,
    execution: ExecutionProfile,
}

impl QueryExecutionLimits {
    pub fn try_new(
        work_mem_bytes: usize,
        hash_memory_bytes: usize,
        execution: ExecutionProfile,
    ) -> Result<Self, QueryExecutionError> {
        if work_mem_bytes == 0 || hash_memory_bytes == 0 {
            return Err(QueryExecutionError::InvalidLimits);
        }
        Ok(Self {
            work_mem_bytes,
            hash_memory_bytes,
            execution,
        })
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

    /// Build the bounded environment used only while constructing a physical
    /// plan. Execution replaces it with a plan-sized environment.
    pub(super) fn planning_runtime_env(self) -> DataFusionResult<RuntimeResources> {
        self.runtime_env(self.work_mem_bytes, None)
    }

    /// Size one executable plan using PostgreSQL's per-operation memory model.
    /// Each worker fragment calls this independently, so concurrently assigned
    /// fragments cannot consume one another's reservation budget.
    pub(super) fn runtime_env_for_plan(
        self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<RuntimeResources> {
        let bytes = self.plan_memory_bytes(plan)?;
        self.runtime_env(bytes, None)
    }

    /// Build one independently bounded worker-fragment environment while
    /// observing the aggregate reservation of this worker participant.
    pub(super) fn runtime_env_for_fragment(
        self,
        plan: &Arc<dyn ExecutionPlan>,
        participant: Arc<ParticipantMemoryRecorder>,
    ) -> DataFusionResult<RuntimeResources> {
        let bytes = self.plan_memory_bytes(plan)?;
        self.runtime_env(bytes, Some(participant))
    }

    fn plan_memory_bytes(
        self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<usize> {
        let mut bytes = 0usize;
        plan.apply(|node| {
            let per_partition = if node.is::<HashJoinExec>() {
                self.hash_memory_bytes
            } else if node.is::<SortExec>() || node.is::<SortMergeJoinExec>() {
                self.work_mem_bytes
            } else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let node_bytes = per_partition
                .checked_mul(node.output_partitioning().partition_count())
                .ok_or_else(Self::budget_overflow)?;
            bytes = bytes
                .checked_add(node_bytes)
                .ok_or_else(Self::budget_overflow)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(bytes.max(self.work_mem_bytes))
    }

    fn runtime_env(
        self,
        bytes: usize,
        participant: Option<Arc<ParticipantMemoryRecorder>>,
    ) -> DataFusionResult<RuntimeResources> {
        let disk = DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled);
        let memory = Arc::new(PeakRecordingPool::new(Arc::new(
            GreedyMemoryPool::new(bytes),
        )));
        let memory_pool: Arc<dyn MemoryPool> = match participant {
            Some(participant) => Arc::new(ParticipantRecordingPool {
                fragment: Arc::clone(&memory),
                participant,
            }),
            None => Arc::clone(&memory) as Arc<dyn MemoryPool>,
        };
        let environment = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .with_disk_manager_builder(disk)
            .build()?;
        Ok(RuntimeResources {
            environment: Arc::new(environment),
            memory,
        })
    }

    fn budget_overflow() -> DataFusionError {
        DataFusionError::Plan(
            "physical-plan memory budget exceeds the host address space".to_owned(),
        )
    }
}
