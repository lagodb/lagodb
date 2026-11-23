//! DataFusion-aligned join input roles and operator cost.

use super::{
    CostingContext, HashBuildEstimate, PlanCost, PlanEstimate, QueryCostError,
};
use crate::HashJoinFallback;
use crate::plan::ir::JoinNode;

/// Physical input roles selected by DataFusion's join optimizer.
///
/// LagoDB leaves `optimizer.join_reordering` at DataFusion's enabled default.
/// This cost-side decision mirrors JoinSelection's smaller-input policy and
/// places that input in the first/left build role. JoinSelection compares
/// `total_byte_size`, which `PlanStatisticsProvider` derives from these same IR
/// sizes, so a pruning projection above a nested join is the only input it can
/// see as narrower. A NULL-sensitive anti join always builds its subquery,
/// the right input: LagoDB restores that side after JoinSelection.
#[derive(Debug, Clone, Copy)]
struct JoinCostInputs {
    build: PlanEstimate,
    probe: PlanEstimate,
}

/// Work a hash join adds when its build outgrows `hash_mem`.
#[derive(Debug, Clone, Copy)]
struct OverflowCost {
    cost: PlanCost,
    /// Whether the join reads its whole probe input before its first row.
    reads_probe_first: bool,
}

impl JoinCostInputs {
    fn select(join: &JoinNode, left: PlanEstimate, right: PlanEstimate) -> Self {
        if join.null_sensitive_anti_mode().is_some()
            || right.byte_size() < left.byte_size()
        {
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
        let pairwise_join = self.join.keys().is_empty();
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
            // DataFusion lowers filter-only SEMI/ANTI to NestedLoopJoin.
            // Account for candidate-pair traversal even when no hash-key work
            // is performed.
            self.context
                .expression_cost(self.join.estimated_key_rows(), 1)
        } else {
            0.0
        };
        let output_filter_cost = if self.join.null_sensitive_anti_mode().is_some() {
            // The subquery key is evaluated once more to find a NULL, and
            // the outer key is tested for NULL on each unmatched outer row.
            self.context.expression_cost(self.right.rows, 1)
                + self.context.expression_cost(self.left.rows, 1)
        } else if self.join.mark_filter().is_some() {
            // The semi Mark shape performs one boolean mark test and one
            // outer-column NULL test for every logical left row.
            self.context.expression_cost(self.left.rows, 2)
        } else {
            0.0
        };
        let overflow = if pairwise_join {
            None
        } else {
            self.overflow_cost(inputs)?
        };
        // A hash join emits while probing, and so does a hybrid join's first
        // group. A sort-merge fallback reads the probe input first.
        let (probe_startup, overflow_startup, overflow_total) = match overflow {
            None => (inputs.probe.cost.startup, 0.0, 0.0),
            Some(overflow) => (
                if overflow.reads_probe_first {
                    inputs.probe.cost.total
                } else {
                    inputs.probe.cost.startup
                },
                overflow.cost.startup(),
                overflow.cost.total(),
            ),
        };
        let rows = self.join.estimated_rows();
        let startup =
            inputs.build.cost.total + build_cost + probe_startup + overflow_startup;
        let total = self.left.cost.total
            + self.right.cost.total
            + build_cost
            + probe_cost
            + residual_cost
            + pairwise_cost
            + output_filter_cost
            + overflow_total
            + rows * self.context.cpu_tuple_cost;
        PlanEstimate::try_new(
            rows,
            self.context.batches(rows),
            self.join.output_width(self.left.width, self.right.width),
            PlanCost::try_new(startup, total)?,
        )
    }

    /// The work of the fallback a join runs when PostgreSQL's sizing expects the
    /// build to outgrow `hash_mem`. Charging it keeps Auto mode from offloading
    /// a join that PostgreSQL would run with batches, at hash-join prices.
    ///
    /// A hybrid hash join writes its build to runs before it emits a row, then
    /// joins its first group while it writes the rest of the probe, and reads
    /// both back once: the I/O `initial_cost_hashjoin` charges a batched join,
    /// which does not discount the first batch either. PostgreSQL keeps each
    /// tuple's hash in its batch file; the runs do not, so splitting both
    /// inputs evaluates and hashes their keys once more.
    ///
    /// One split makes a fixed number of buckets, so a build of more
    /// `hash_mem`s than that has buckets over it even with even keys, and each
    /// further split does all of that work again, as `cost_sort` charges each
    /// merge pass. The first group then does not fit either, so the whole probe
    /// is split before the first row. Splits that skewed keys cause are not
    /// charged, as PostgreSQL does not charge growing its batches.
    /// A sort-merge fallback sorts both inputs, and each sort decides for
    /// itself whether it spills: PostgreSQL sizes the hash table with
    /// per-tuple overhead a sort's input does not carry.
    ///
    /// The task count comes from scan task planning at execution, so a path is
    /// costed with its whole build in one `hash_mem`. A run with more tasks can
    /// only do better than this.
    fn overflow_cost(
        &self,
        inputs: JoinCostInputs,
    ) -> Result<Option<OverflowCost>, QueryCostError> {
        let Some(spill) = self.context.spill else {
            return Ok(None);
        };
        let memory_units =
            HashBuildEstimate::new(inputs.build.rows, inputs.build.width)
                .hash_memory_units();
        if memory_units <= 1.0 {
            return Ok(None);
        }
        let keys = self.join.keys().len();
        let (cost, reads_probe_first) = match spill.hash_join_fallback {
            HashJoinFallback::HybridHash => {
                let build_hashes =
                    self.context.expression_cost(inputs.build.rows, keys);
                let probe_hashes =
                    self.context.expression_cost(inputs.probe.rows, keys);
                let build_pages = self.context.page_cost(inputs.build);
                let probe_pages = self.context.page_cost(inputs.probe);
                let split =
                    build_hashes + probe_hashes + 2.0 * (build_pages + probe_pages);
                let buckets =
                    f64::from(1 << HashJoinFallback::HYBRID_HASH_BUCKET_BITS);
                let splits = (memory_units.ln() / buckets.ln()).ceil();
                if splits > 1.0 {
                    let first =
                        build_hashes + probe_hashes + build_pages + probe_pages;
                    (PlanCost::try_new(first, splits * split)?, true)
                } else {
                    (PlanCost::try_new(build_hashes + build_pages, split)?, false)
                }
            }
            HashJoinFallback::SortMerge => {
                let memory = spill.sort_memory_bytes;
                let sorts =
                    self.context.spillable_sort_cost(inputs.build, keys, memory)
                        + self.context.spillable_sort_cost(
                            inputs.probe,
                            keys,
                            memory,
                        );
                (PlanCost::try_new(sorts, sorts)?, true)
            }
        };
        Ok(Some(OverflowCost {
            cost,
            reads_probe_first,
        }))
    }
}
