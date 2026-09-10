//! DataFusion-aligned join input roles and operator cost.

use super::super::JoinNode;
use super::{CostingContext, PlanCost, PlanEstimate, QueryCostError};

/// Physical input roles selected by DataFusion's join optimizer.
///
/// LagoDB leaves `optimizer.join_reordering` at DataFusion's enabled default.
/// This cost-side decision mirrors JoinSelection's smaller-input policy using
/// the IR cardinalities owned by the estimator and places that input in the
/// first/left build role. Null-aware anti joins are the explicit non-swappable
/// exception.
#[derive(Debug, Clone, Copy)]
struct JoinCostInputs {
    build: PlanEstimate,
    probe: PlanEstimate,
}

impl JoinCostInputs {
    fn select(join: &JoinNode, left: PlanEstimate, right: PlanEstimate) -> Self {
        if !join.null_aware() && right.rows < left.rows {
            Self {
                build: right,
                probe: left,
            }
        } else {
            Self {
                build: left,
                probe: right,
            }
        }
    }
}

pub(super) struct JoinCostStrategy<'join> {
    context: CostingContext,
    join: &'join JoinNode,
    left: PlanEstimate,
    right: PlanEstimate,
}

impl<'join> JoinCostStrategy<'join> {
    pub(super) const fn new(
        context: CostingContext,
        join: &'join JoinNode,
        left: PlanEstimate,
        right: PlanEstimate,
    ) -> Self {
        Self {
            context,
            join,
            left,
            right,
        }
    }

    pub(super) fn estimate(self) -> Result<PlanEstimate, QueryCostError> {
        let inputs = JoinCostInputs::select(self.join, self.left, self.right);
        let pairwise_join = self.join.keys().is_empty()
            || self.join.mark_filter().is_some_and(|filter| filter.anti());
        let hash_key_units = if pairwise_join {
            0
        } else {
            self.join.keys().len()
        };
        let build_cost = inputs.build.rows * self.context.cpu_tuple_cost
            + self
                .context
                .expression_cost(inputs.build.rows, hash_key_units);
        let probe_cost = self
            .context
            .expression_cost(inputs.probe.rows, hash_key_units);
        let residual_cost = self.join.on_filter().map_or(0.0, |filter| {
            self.context
                .expression_cost(self.join.estimated_key_rows(), filter.cost_units())
        });
        let pairwise_cost = if pairwise_join {
            // DataFusion lowers filter-only SEMI/ANTI and the null-sensitive
            // anti Mark condition to NestedLoopJoin. Account for candidate-pair
            // traversal even when no hash-key work is performed.
            let units = if self.join.mark_filter().is_some_and(|filter| filter.anti())
            {
                2
            } else {
                1
            };
            self.context
                .expression_cost(self.join.estimated_key_rows(), units)
        } else {
            0.0
        };
        let mark_filter_cost = self.join.mark_filter().map_or(0.0, |_| {
            // The concrete Mark shape performs one boolean mark comparison
            // and one outer-column NULL test for every logical left row.
            self.context.expression_cost(self.left.rows, 2)
        });
        let rows = self.join.estimated_rows();
        let startup =
            inputs.build.cost.total + build_cost + inputs.probe.cost.startup;
        let total = self.left.cost.total
            + self.right.cost.total
            + build_cost
            + probe_cost
            + residual_cost
            + pairwise_cost
            + mark_filter_cost
            + rows * self.context.cpu_tuple_cost;
        PlanEstimate::try_new(
            rows,
            self.context.batches(rows),
            PlanCost::try_new(startup, total)?,
        )
    }
}
