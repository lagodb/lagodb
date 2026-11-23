//! Join strategy fixed at planning time for each hash join.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

use super::JoinMemoryPolicy;
use super::adaptive::AdaptiveHashJoinExec;
use super::null_sensitive::NullSensitiveAntiJoinExec;

/// Puts hash joins under [`AdaptiveHashJoinExec`] once JoinSelection has chosen
/// each join's mode and build side, neither of which this rule changes.
///
/// A Partitioned join and the single-partition serial join have one build per
/// partition. A CollectLeft join with several probe partitions does not, and
/// stays a plain hash join, except under [`NullSensitiveAntiJoinExec`], which
/// runs its join as one partition.
///
/// EnsureRequirements runs afterwards and adds the repartitions and coalesces
/// each mode needs.
#[derive(Debug)]
pub(super) struct JoinStrategyRule {
    policy: JoinMemoryPolicy,
}

impl JoinStrategyRule {
    pub(super) const fn new(policy: JoinMemoryPolicy) -> Self {
        Self { policy }
    }

    fn place(
        &self,
        node: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(anti) = node.downcast_ref::<NullSensitiveAntiJoinExec>() {
            // The serial join is already adaptive from its own visit.
            if !anti.join().is::<HashJoinExec>() {
                return Ok(Transformed::no(node));
            }
            let join = self.adaptive(Arc::clone(anti.join()))?.data;
            return Ok(Transformed::yes(Arc::new(NullSensitiveAntiJoinExec::new(
                anti.mode(),
                join,
            ))));
        }
        let Some(join) = node.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(node));
        };
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
        let adaptive = AdaptiveHashJoinExec::try_new(join, self.policy)?;
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
