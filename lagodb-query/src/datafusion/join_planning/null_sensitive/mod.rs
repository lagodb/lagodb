//! NULL-sensitive anti SubPlans as a hash join that builds the subquery.
//!
//! The compiler's `NullSensitiveAntiJoin` plans to a RightAnti hash join with
//! the subquery as its build input, under [`NullSensitiveAntiJoinExec`]. The
//! join is an ordinary one, so it runs under the adaptive join and spills like
//! every other hash join, serial or parallel; the node above it decides the
//! unmatched outer rows from what the build held.

mod exec;
mod facts;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result};
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{
    HashJoinExec, HashJoinExecBuilder, PartitionMode,
};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{
    Distribution, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    replace_children_if_necessary,
};
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};

pub(in crate::datafusion) use self::exec::NullSensitiveAntiJoinExec;
use crate::datafusion::compiler::NullSensitiveAntiJoin;
use crate::datafusion::join_planning::adaptive::AdaptiveHashJoinExec;

/// DataFusion's physical planner, with the logical nodes LagoDB compiles.
#[derive(Debug)]
pub(super) struct LagoQueryPlanner;

#[async_trait]
impl QueryPlanner for LagoQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            NullSensitiveAntiJoinPlanner,
        )])
        .create_physical_plan(logical_plan, session)
        .await
    }
}

struct NullSensitiveAntiJoinPlanner;

#[async_trait]
impl ExtensionPlanner for NullSensitiveAntiJoinPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(anti) = node.as_any().downcast_ref::<NullSensitiveAntiJoin>() else {
            return Ok(None);
        };
        let ([outer_plan, subquery_plan], [outer, subquery]) =
            (logical_inputs, physical_inputs)
        else {
            return Err(DataFusionError::Internal(
                "NullSensitiveAntiJoin has two inputs".to_owned(),
            ));
        };
        let outer_key = planner.create_physical_expr(
            anti.outer_key(),
            outer_plan.schema(),
            session,
            planning_ctx,
        )?;
        let subquery_key = planner.create_physical_expr(
            anti.subquery_key(),
            subquery_plan.schema(),
            session,
            planning_ctx,
        )?;
        // The hash join builds its left input. NULL keys match nothing, so a
        // RightAnti join returns every outer row with a NULL key, and the node
        // above decides those rows with the rest.
        let join = HashJoinExecBuilder::new(
            Arc::clone(subquery),
            Arc::clone(outer),
            vec![(subquery_key, outer_key)],
            JoinType::RightAnti,
        )
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_null_equality(NullEquality::NullEqualsNothing)
        .build()?;
        Ok(Some(Arc::new(NullSensitiveAntiJoinExec::new(
            anti.mode(),
            Arc::new(join),
        ))))
    }
}

/// Restores the subquery as the build input after JoinSelection, which swaps
/// a CollectLeft join whose probe input it estimates smaller. Swapping back
/// keeps the mode: a swapped anti join keeps its column order.
#[derive(Debug)]
pub(super) struct NullSensitiveBuildSideRule;

impl NullSensitiveBuildSideRule {
    fn restore(
        node: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(anti) = node.downcast_ref::<NullSensitiveAntiJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        let Some(join) = anti.join().downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        if *join.join_type() != JoinType::LeftAnti {
            return Ok(Transformed::no(node));
        }
        let join = join.swap_inputs(PartitionMode::CollectLeft)?;
        Ok(Transformed::yes(Arc::new(NullSensitiveAntiJoinExec::new(
            anti.mode(),
            join,
        ))))
    }
}

impl PhysicalOptimizerRule for NullSensitiveBuildSideRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(Self::restore).data()
    }

    fn name(&self) -> &str {
        "lagodb_null_sensitive_build_side"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Removes the round-robin repartition EnsureRequirements adds to spread the
/// probe input over more partitions. [`NullSensitiveAntiJoinExec`] merges every
/// probe partition into the one stream its join reads, so the repartition
/// would only split batches for that merge to rejoin. That holds below a
/// filter or projection too: it evaluates when the merged stream polls its
/// partition, in the node's own task, and spawns none of its own.
#[derive(Debug)]
pub(super) struct NullSensitiveProbeRule;

impl NullSensitiveProbeRule {
    fn merge(
        node: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(anti) = node.downcast_ref::<NullSensitiveAntiJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        let join = Self::join_without_fan_out(Arc::clone(anti.join()))?;
        if !join.transformed {
            return Ok(Transformed::no(node));
        }
        Ok(Transformed::yes(Arc::new(NullSensitiveAntiJoinExec::new(
            anti.mode(),
            join.data,
        ))))
    }

    /// The hash join, directly or under the adaptive node, with its probe
    /// input rebuilt.
    fn join_without_fan_out(
        join: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if join.is::<AdaptiveHashJoinExec>() {
            return join.map_children(Self::join_without_fan_out);
        }
        let Some(hash_join) = join.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(join));
        };
        let build = Arc::clone(hash_join.left());
        let probe = Self::without_fan_out(Arc::clone(hash_join.right()))?;
        if !probe.transformed {
            return Ok(Transformed::no(join));
        }
        replace_children_if_necessary(join, vec![build, probe.data])
            .map(Transformed::yes)
    }

    /// `plan` without the round-robin repartition on its chain of unary
    /// operators that take any distribution of their input and keep its
    /// partition count, the operators EnsureRequirements adds one below.
    fn without_fan_out(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
            return Ok(match repartition.partitioning() {
                Partitioning::RoundRobinBatch(_) => {
                    Transformed::yes(Arc::clone(repartition.input()))
                }
                _ => Transformed::no(plan),
            });
        }
        let passes_partitions_through = match plan.children().as_slice() {
            [child] => {
                matches!(
                    plan.input_distribution_requirements().child_distribution(0),
                    Some(Distribution::UnspecifiedDistribution)
                ) && child.output_partitioning().partition_count()
                    == plan.output_partitioning().partition_count()
            }
            _ => false,
        };
        if passes_partitions_through {
            plan.map_children(Self::without_fan_out)
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

impl PhysicalOptimizerRule for NullSensitiveProbeRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(Self::merge).data()
    }

    fn name(&self) -> &str {
        "lagodb_null_sensitive_probe"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
