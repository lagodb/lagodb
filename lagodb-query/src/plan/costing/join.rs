//! DataFusion-aligned join input roles and operator cost.

use super::{
    CostingContext, HashBuildEstimate, PlanCost, PlanEstimate, QueryCostError,
};
use crate::plan::ir::JoinNode;

/// Physical input roles selected by DataFusion's join optimizer.
///
/// LagoDB leaves `optimizer.join_reordering` at DataFusion's enabled default.
/// This cost-side decision mirrors JoinSelection's smaller-input policy and
/// places that input in the first/left build role. JoinSelection compares
/// `total_byte_size`, which `PlanStatisticsProvider` derives from these same IR
/// sizes, so a pruning projection above a nested join is the only input it can
/// see as narrower. Null-aware anti joins are the explicit non-swappable
/// exception.
#[derive(Debug, Clone, Copy)]
struct JoinCostInputs {
    build: PlanEstimate,
    probe: PlanEstimate,
}

impl JoinCostInputs {
    fn select(join: &JoinNode, left: PlanEstimate, right: PlanEstimate) -> Self {
        if !join.null_aware() && right.byte_size() < left.byte_size() {
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
        let fallback_sorts = if pairwise_join {
            None
        } else {
            self.fallback_sort_cost(inputs)
        };
        // A hash join emits while probing. Sorts read the probe input first.
        let (probe_startup, sort_cost) = fallback_sorts
            .map_or((inputs.probe.cost.startup, 0.0), |cost| {
                (inputs.probe.cost.total, cost)
            });
        let rows = self.join.estimated_rows();
        let startup =
            inputs.build.cost.total + build_cost + probe_startup + sort_cost;
        let total = self.left.cost.total
            + self.right.cost.total
            + build_cost
            + probe_cost
            + residual_cost
            + pairwise_cost
            + mark_filter_cost
            + sort_cost
            + rows * self.context.cpu_tuple_cost;
        PlanEstimate::try_new(
            rows,
            self.context.batches(rows),
            self.join.output_width(self.left.width, self.right.width),
            PlanCost::try_new(startup, total)?,
        )
    }

    /// The sorts both inputs need when PostgreSQL's sizing expects the build to
    /// outgrow `hash_mem`, which makes the join continue as a sort-merge join at
    /// run time. Charging them keeps Auto mode from offloading a join that
    /// PostgreSQL would run with batches, at hash-join prices. Each sort still
    /// decides for itself whether it spills: PostgreSQL sizes the hash table with
    /// per-tuple overhead a sort's input does not carry.
    ///
    /// The task count comes from scan task planning at execution, so a path is
    /// costed with its whole build in one `hash_mem`. A run with more tasks can
    /// only do better than this.
    fn fallback_sort_cost(&self, inputs: JoinCostInputs) -> Option<f64> {
        let memory = self.context.spill_sort_memory_bytes?;
        let build = HashBuildEstimate::new(inputs.build.rows, inputs.build.width);
        if self.join.null_aware() || build.fits_hash_memory() {
            return None;
        }
        let keys = self.join.keys().len();
        Some(
            self.context.spillable_sort_cost(inputs.build, keys, memory)
                + self.context.spillable_sort_cost(inputs.probe, keys, memory),
        )
    }
}
