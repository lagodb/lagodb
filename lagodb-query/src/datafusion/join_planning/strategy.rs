//! Join strategy fixed at planning time for each hash join.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

use super::adaptive::AdaptiveHashJoinExec;

/// Puts hash joins under [`AdaptiveHashJoinExec`] once JoinSelection has chosen
/// each join's mode and build side, neither of which this rule changes.
///
/// A Partitioned join and the single-partition serial join have one build per
/// partition. A CollectLeft join with several probe partitions does not, and
/// stays a plain hash join.
///
/// EnsureRequirements runs afterwards and adds the repartitions and coalesces
/// each mode needs.
#[derive(Debug)]
pub(super) struct JoinStrategyRule {
    hash_memory_bytes: usize,
}

impl JoinStrategyRule {
    pub(super) const fn new(hash_memory_bytes: usize) -> Self {
        Self { hash_memory_bytes }
    }

    fn place(
        &self,
        node: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(join) = node.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        // Sort-merge join cannot express a null-aware anti join.
        if join.null_aware {
            return Ok(Transformed::no(node));
        }
        match join.partition_mode() {
            PartitionMode::Partitioned => self.adaptive(node),
            PartitionMode::CollectLeft if config.execution.target_partitions == 1 => {
                self.adaptive(node)
            }
            _ => Ok(Transformed::no(node)),
        }
    }

    fn adaptive(
        &self,
        join: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let adaptive = AdaptiveHashJoinExec::try_new(join, self.hash_memory_bytes)?;
        Ok(Transformed::yes(Arc::new(adaptive)))
    }
}

impl PhysicalOptimizerRule for JoinStrategyRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| self.place(node, config)).data()
    }

    fn name(&self) -> &str {
        "lagodb_join_strategy"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
