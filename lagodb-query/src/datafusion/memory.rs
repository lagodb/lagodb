//! Query participant memory and spill policy.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// DataFusion's own default, kept as the ceiling for larger `work_mem`.
const MAXIMUM_SORT_MERGE_RESERVATION: usize = 10 * 1024 * 1024;

/// Shared by the `ExternalSorter[p]` and `ExternalSorterMerge[p]` consumers.
const SORT_CONSUMER_PREFIX: &str = "ExternalSorter";

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

/// `FairSpillPool` that does not refuse a Sort holding no more than its merge
/// reservation.
///
/// `ExternalSorter` reserves twice the size of a batch it already holds, and
/// the pool only keeps accounts, so a refusal frees nothing. With nothing
/// buffered to spill, that refusal ends the query, while PostgreSQL's
/// `work_mem` is soft: `tuplesort_puttuple_common` stores the tuple and checks
/// `LACKMEM` after. Up to this floor a refused Sort is booked anyway. Above it
/// the refusal stands and Sort spills as usual, so a batch wider than its share
/// becomes a run of its own, and a merge that cannot seat two runs re-spills
/// them with smaller batches.
///
/// Only Sort gets the floor. Other consumers start empty too, `BroadcastExec`
/// once per batch, so a rule on size alone would leave them unbounded.
///
/// Recheck three `ExternalSorter` behaviors on a DataFusion upgrade: its
/// consumers keep these names, a refusal with batches buffered still spills, and
/// a merge that cannot seat two runs still ends in `SplitThenRetry`.
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
/// Every executable plan or worker fragment receives its own pool. The pool
/// rejects growth that reserves before allocation. DataFusion 55's
/// native DISTINCT accumulators update their state before resizing their
/// reservation, so this type does not claim a reservation-first hard limit for
/// that state.
///
/// With spilling enabled the pool is `FairSpillPool`: the plan-wide total is
/// fixed, and operators that can spill share what the others leave. An
/// individual operator's share is therefore not a hard per-operator limit. A
/// Sort can also pass the total by about its merge reservation and one batch,
/// see `SortFloorPool`.
///
/// With spilling disabled an operator has nowhere to put what does not fit, so
/// a `work_mem` budget would turn PostgreSQL's soft allowance into a query
/// failure. The pool has no limit instead. It stays a `GreedyMemoryPool` because
/// DataFusion picks a different aggregate stream for a pool that reports
/// `MemoryLimit::Infinite`.
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

    /// Rebuild limits from the parts of a validated instance, such as the
    /// leader limits carried to a parallel worker by its bootstrap.
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

    /// Session settings shared by leader planning and worker decoding. Sort
    /// reads its merge reservation from the session, so a worker needs the same
    /// value as the leader.
    pub(super) fn session_config(self, target_partitions: usize) -> SessionConfig {
        let mut config = SessionConfig::new()
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

    /// Build the bounded environment used only while constructing a physical
    /// plan. Execution replaces it with a plan-sized environment.
    pub(super) fn planning_runtime_env(self) -> DataFusionResult<RuntimeResources> {
        self.runtime_env(self.work_mem_bytes, None)
    }

    /// Build the pool of one executable plan. Each worker fragment calls this
    /// independently, so concurrently assigned fragments cannot consume one
    /// another's reservation budget.
    pub(super) fn runtime_env_for_plan(
        self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<RuntimeResources> {
        let bytes = self.pool_bytes(plan)?;
        self.runtime_env(bytes, None)
    }

    /// Build one independently bounded worker-fragment environment while
    /// observing the aggregate reservation of this worker participant.
    pub(super) fn runtime_env_for_fragment(
        self,
        plan: &Arc<dyn ExecutionPlan>,
        participant: Arc<ParticipantMemoryRecorder>,
    ) -> DataFusionResult<RuntimeResources> {
        let bytes = self.pool_bytes(plan)?;
        self.runtime_env(bytes, Some(participant))
    }

    fn pool_bytes(self, plan: &Arc<dyn ExecutionPlan>) -> DataFusionResult<usize> {
        if self.spill_enabled {
            self.plan_memory_bytes(plan)
        } else {
            Ok(usize::MAX)
        }
    }

    /// Spilling needs a real total, because the pool share is what tells an
    /// operator when to spill. PostgreSQL's per-operation memory model gives it.
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

    /// Budget for one node of one task, granting `hash_mem` to hash tables and
    /// `work_mem` to the rest. PostgreSQL grants one allowance per process, but
    /// a task's partitions share a backend, so a per-partition budget here is
    /// wider. Only an adaptive hash join divides its `hash_mem` among them.
    /// Partial and final aggregates are separate nodes and count separately.
    /// Repartition holds no PostgreSQL-style operation memory, but each output
    /// registers a spillable consumer that takes a share of the pool, so it
    /// brings its own allowance rather than shrinking the other operators.
    fn node_memory_bytes(
        self,
        node: &Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<usize> {
        let partitions = node.output_partitioning().partition_count();
        let (per_partition, partitions) = if node.is::<HashJoinExec>() {
            // CollectLeft builds one table that every partition probes. Any
            // other join runs under `AdaptiveHashJoinExec`, which holds each
            // partition to its share of one `hash_mem`. Null-aware joins, the
            // only ones it skips, are always CollectLeft.
            (self.hash_memory_bytes, 1)
        } else if node.is::<AdaptiveHashJoinExec>() {
            // A partition that falls back sorts both inputs. The plan does
            // not show those Sorts, so their usual budgets are granted here.
            // Sharing the join's `hash_mem` instead would shrink each Sort's
            // runs as partitions grow.
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
            // One cached build feeds every consumer task, so its virtual
            // output partitions do not multiply the memory it holds.
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

    fn runtime_env(
        self,
        bytes: usize,
        participant: Option<Arc<ParticipantMemoryRecorder>>,
    ) -> DataFusionResult<RuntimeResources> {
        let (pool, disk): (Arc<dyn MemoryPool>, DiskManagerBuilder) = if self
            .spill_enabled
        {
            // TODO(datafusion-nlj-fallback): a temp-file disk manager also
            // enables NestedLoopJoin's fallback, which on ResourcesExhausted
            // executes its left child again with no reset. In serial, a
            // CollectLeft HashJoin under that child keeps its finished build and
            // skips final emission, so LEFT SEMI/ANTI/OUTER rows go missing with
            // no error. In parallel the rerun ends in an internal ERROR. This is
            // apache/datafusion#24676, fixed on main by #24677 but not in 55.2.0;
            // see also #25701 and datafusion-distributed#674. Recheck them on
            // every DataFusion upgrade and remove this once the fix is pinned.
            //
            // TODO(datafusion-aggregate-spill): a grouped aggregate writes each
            // spill run in batches of up to `batch_size` rows whatever their
            // size in bytes, and reading the runs back seats each one with twice
            // its widest batch. A run that holds fewer groups than one batch is a
            // single batch about the size of the aggregate's share, so the merge
            // is refused and the query fails with ResourcesExhausted where
            // PostgreSQL's soft work_mem would continue. This is
            // apache/datafusion#25851, open, with byte-bounded spill batches
            // proposed in #26987. One group whose state outgrows the share fails
            // the same way, in the merge or in the replay, which cannot spill;
            // #25851 does not cover that case. Recheck both on every DataFusion
            // upgrade and remove this once they are resolved.
            (
                Arc::new(SortFloorPool {
                    inner: FairSpillPool::new(bytes),
                    sort_floor_bytes: self.sort_merge_reservation_bytes(),
                }),
                PgTempFileFactory::disk_manager(self.work_mem_bytes),
            )
        } else {
            (
                Arc::new(GreedyMemoryPool::new(bytes)),
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
        };
        let memory = Arc::new(PeakRecordingPool::new(pool));
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
