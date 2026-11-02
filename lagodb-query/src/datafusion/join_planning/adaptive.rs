//! Hash join that chooses its algorithm from the build input it actually reads.

use std::fmt::{self, Formatter};
use std::sync::{Arc, Mutex};

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter_pushdown::{
    FilterDescription, FilterPushdownPhase,
};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    PlanProperties, ReplaceChildrenOptions,
};
use futures::{StreamExt, TryStreamExt, stream};

use super::sort_merge::sort_merge_join;
use super::staged_build::{StagedBuild, StagedInputExec};

/// Per-partition plans that replaced the hash join, kept for their metrics.
type FallbackPlans = Arc<Mutex<Vec<Arc<dyn ExecutionPlan>>>>;

/// Runs one fixed `HashJoinExec`, deciding per partition whether it can hash.
///
/// DataFusion's hash join builds in memory and cannot spill. PostgreSQL decides
/// batching at run time for the same reason an estimate cannot: the build size
/// is only known once read. This node reads the build partition first, under
/// its even share of the task's `hash_mem`. When it fits, the hash join runs on
/// those batches. When it does not, both inputs go through spillable sorts into
/// a sort-merge join. The probe side is still unread at that point, so each
/// input is read exactly once and the plan never changes.
///
/// The join stays a child so that planners keep recognizing it, and so that
/// the protobuf codec carries it. This node never executes that child's own
/// inputs twice: it opens them itself and hands over the opened streams. Both
/// open in `execute`, as `HashJoinExec` opens them, because this stream may
/// never be polled and a worker finishes only once every upstream stream exists.
///
/// Only a join whose partitions are independent can take this path. A
/// CollectLeft join with several probe partitions shares one build and stays a
/// plain hash join.
///
/// Recheck three `HashJoinExec` behaviors on every DataFusion upgrade, because
/// a change to them need not fail loudly here: `builder()` keeps the dynamic
/// filter when it replaces the children, the filter's consumer is found by
/// walking the probe input, and an empty build finishes without polling the
/// probe. Delete this node once DataFusion ships a hash join that spills.
#[derive(Debug)]
pub(in crate::datafusion) struct AdaptiveHashJoinExec {
    join: Arc<dyn ExecutionPlan>,
    /// `hash_mem` of the whole task, not of one partition.
    hash_memory_bytes: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    fallback_plans: FallbackPlans,
}

impl AdaptiveHashJoinExec {
    /// `join` must be a `HashJoinExec` that is not null-aware. Output order is
    /// dropped because a sort-merge partition does not keep the probe order;
    /// equivalences and partitioning hold for either algorithm.
    pub(in crate::datafusion) fn try_new(
        join: Arc<dyn ExecutionPlan>,
        hash_memory_bytes: usize,
    ) -> Result<Self> {
        if !join.is::<HashJoinExec>() {
            return Err(DataFusionError::Internal(format!(
                "AdaptiveHashJoinExec cannot run {}",
                join.name(),
            )));
        }
        let child = join.properties();
        let mut equivalences = child.eq_properties.clone();
        equivalences.clear_orderings();
        let properties = PlanProperties::new(
            equivalences,
            child.partitioning.clone(),
            child.emission_type,
            child.boundedness,
        );
        Ok(Self {
            join,
            hash_memory_bytes,
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
            fallback_plans: Arc::default(),
        })
    }

    pub(in crate::datafusion) const fn hash_memory_bytes(&self) -> usize {
        self.hash_memory_bytes
    }

    fn hash_join(join: &Arc<dyn ExecutionPlan>) -> &HashJoinExec {
        join.downcast_ref()
            .expect("try_new admits only HashJoinExec")
    }

    /// Spill metrics of a fallback plan. Its operators are built at run time
    /// and appear nowhere else in EXPLAIN.
    fn collect_spill_metrics(plan: &dyn ExecutionPlan, metrics: &mut MetricsSet) {
        if let Some(set) = plan.metrics() {
            for metric in set.iter() {
                if matches!(
                    metric.value(),
                    MetricValue::SpillCount(_)
                        | MetricValue::SpilledBytes(_)
                        | MetricValue::SpilledRows(_)
                ) {
                    metrics.push(Arc::clone(metric));
                }
            }
        }
        for child in plan.children() {
            Self::collect_spill_metrics(child.as_ref(), metrics);
        }
    }
}

impl DisplayAs for AdaptiveHashJoinExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str("AdaptiveHashJoinExec")
    }
}

impl ExecutionPlan for AdaptiveHashJoinExec {
    fn name(&self) -> &str {
        "AdaptiveHashJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.join]
    }

    /// The child's partitioning is the join's contract, so nothing may be
    /// inserted between this node and the join it runs.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn apply_expressions(
        &self,
        _visitor: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            children.swap_remove(0),
            self.hash_memory_bytes,
        )?))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    /// Let the join narrow or embed a projection, as it would without this
    /// node, and keep this node directly above the resulting join.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(swapped) = self.join.try_swapping_with_projection(projection)?
        else {
            return Ok(None);
        };
        swapped
            .transform_down(|node| {
                if !node.is::<HashJoinExec>() {
                    return Ok(Transformed::no(node));
                }
                let adaptive = Self::try_new(node, self.hash_memory_bytes)?;
                Ok(Transformed::new(
                    Arc::new(adaptive) as Arc<dyn ExecutionPlan>,
                    true,
                    TreeNodeRecursion::Stop,
                ))
            })
            .data()
            .map(Some)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let join = Self::hash_join(&self.join);
        let build = join.left().execute(partition, Arc::clone(&context))?;
        let probe = join.right().execute(partition, Arc::clone(&context))?;
        let fallback_partitions = MetricBuilder::new(&self.metrics)
            .counter("fallback_partitions", partition);
        let run = PartitionRun {
            join: Arc::clone(&self.join),
            partition,
            context,
            build_limit_bytes: self.hash_memory_bytes
                / self.properties.partitioning.partition_count(),
            fallback_partitions,
            fallback_plans: Arc::clone(&self.fallback_plans),
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream::once(run.execute(build, probe)).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let mut metrics = self.metrics.clone_inner();
        let plans = self
            .fallback_plans
            .lock()
            .expect("no code panics while holding the fallback plans");
        for plan in plans.iter() {
            Self::collect_spill_metrics(plan.as_ref(), &mut metrics);
        }
        Some(metrics)
    }
}

/// One partition of an [`AdaptiveHashJoinExec`] from its first build batch.
struct PartitionRun {
    join: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    build_limit_bytes: usize,
    fallback_partitions: Count,
    fallback_plans: FallbackPlans,
}

impl PartitionRun {
    async fn execute(
        self,
        mut build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let reservation =
            MemoryConsumer::new(format!("AdaptiveHashJoinBuild[{}]", self.partition))
                .register(self.context.memory_pool());
        let mut stage = StagedBuild::new(reservation, self.build_limit_bytes);
        let schema = build.schema();
        while let Some(batch) = build.next().await.transpose()? {
            if !stage.push(batch)? {
                let input = stage.replay().chain(build);
                let build = RecordBatchStreamAdapter::new(schema, input);
                return self.sort_merge(Box::pin(build), probe);
            }
        }
        let build = RecordBatchStreamAdapter::new(schema, stage.replay());
        self.hash(Box::pin(build), probe)
    }

    /// Continue the planned join on the staged batches. Its properties, metrics
    /// and dynamic filter all carry over.
    fn hash(
        self,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let join = AdaptiveHashJoinExec::hash_join(&self.join);
        let build = StagedInputExec::opened(Arc::clone(join.left()), build);
        let probe = StagedInputExec::opened(Arc::clone(join.right()), probe);
        join.builder()
            .with_new_children(vec![
                Arc::new(build) as Arc<dyn ExecutionPlan>,
                Arc::new(probe),
            ])?
            .build()?
            .execute(self.partition, self.context)
    }

    /// Replace this partition's join. A serial dynamic filter simply stays
    /// unset, so the probe scan reads everything.
    fn sort_merge(
        self,
        build: SendableRecordBatchStream,
        probe: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        self.fallback_partitions.add(1);
        let join = AdaptiveHashJoinExec::hash_join(&self.join);
        let plan = sort_merge_join(
            join,
            Arc::new(StagedInputExec::single_partition(build)),
            Arc::new(StagedInputExec::single_partition(probe)),
        )?;
        self.fallback_plans
            .lock()
            .expect("no code panics while holding the fallback plans")
            .push(Arc::clone(&plan));
        plan.execute(0, self.context)
    }
}
