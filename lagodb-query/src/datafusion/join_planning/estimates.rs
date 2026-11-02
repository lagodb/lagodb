//! PostgreSQL relation estimates keyed for lookup from a physical plan.

use std::collections::HashMap;

use arrow_schema::Schema;
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::physical_plan::ExecutionPlan;

use crate::datafusion::parallel::ParallelTableScanExec;
use crate::datafusion::table_scan::ExternalTableScanExec;
use crate::plan::{QueryFragment, QueryNode};

#[derive(Debug, Clone, Copy)]
pub(super) struct RelationEstimate {
    rows: f64,
    width: i32,
}

impl RelationEstimate {
    /// Statistics with estimated size only. PostgreSQL's per-column statistics
    /// stay in the planner, so columns remain unknown.
    pub(super) fn statistics(self, schema: &Schema) -> Statistics {
        let mut statistics = Statistics::new_unknown(schema);
        statistics.num_rows = Precision::Inexact(self.rows as usize);
        statistics.total_byte_size =
            Precision::Inexact((self.rows * f64::from(self.width)) as usize);
        statistics
    }
}

/// The estimate of each relation-tree subtree, keyed by its sorted scan set.
///
/// A scan belongs to exactly one relation-tree position, so a scan set names
/// one subtree. Where a Filter and its input cover the same scans, the Filter's
/// outer estimate wins: DataFusion can merge that filter into the join or leave
/// it above, and a join decision only reads the size of the complete input.
#[derive(Debug, Default)]
pub(super) struct PlanEstimates {
    relations: HashMap<Box<[usize]>, RelationEstimate>,
    has_join: bool,
}

impl PlanEstimates {
    /// Estimates for a fragment that joins, or `None` when no join decision
    /// exists. Metadata COUNT relies on DataFusion reading the exact row count
    /// of its scan, and that shape has no join.
    pub(super) fn for_joins(fragment: &QueryFragment) -> Option<Self> {
        let mut estimates = Self::default();
        estimates.record(fragment.root());
        estimates.has_join.then_some(estimates)
    }

    /// Record `node`'s relation subtree, returning its scans and estimate.
    /// Operators above the relation tree return `None`, which keeps a HAVING
    /// Filter from being read as a Filter over the relation.
    fn record(&mut self, node: &QueryNode) -> Option<(Vec<usize>, RelationEstimate)> {
        let (scans, estimate) = match node {
            QueryNode::Scan(scan) => (
                vec![scan.scan().index()],
                RelationEstimate {
                    rows: scan.estimated_rows(),
                    width: scan.estimated_width(),
                },
            ),
            QueryNode::Join(join) => {
                self.has_join = true;
                let (mut scans, left) = self.record(join.left())?;
                let (right_scans, right) = self.record(join.right())?;
                scans.extend(right_scans);
                let estimate = RelationEstimate {
                    rows: join.estimated_rows(),
                    width: join.output_width(left.width, right.width),
                };
                (scans, estimate)
            }
            QueryNode::Filter(filter) => {
                let (scans, input) = self.record(filter.input())?;
                let estimate = RelationEstimate {
                    rows: filter.estimated_rows(),
                    width: input.width,
                };
                (scans, estimate)
            }
            QueryNode::Aggregate(node) => return self.ascend(node.input()),
            QueryNode::Distinct(node) => return self.ascend(node.input()),
            QueryNode::Project(node) => return self.ascend(node.input()),
            QueryNode::Sort(node) => return self.ascend(node.input()),
            QueryNode::Limit(node) => return self.ascend(node.input()),
        };
        let mut key = scans.clone().into_boxed_slice();
        key.sort_unstable();
        self.relations.insert(key, estimate);
        Some((scans, estimate))
    }

    fn ascend(
        &mut self,
        input: &QueryNode,
    ) -> Option<(Vec<usize>, RelationEstimate)> {
        self.record(input);
        None
    }

    /// The estimate for the relation `plan` produces, when it covers exactly
    /// one recorded subtree. A rewrite that removed a scan, such as an empty
    /// relation, leaves no match and no estimate.
    pub(super) fn relation(
        &self,
        plan: &dyn ExecutionPlan,
    ) -> Option<RelationEstimate> {
        let mut scans = Vec::new();
        Self::collect_scans(plan, &mut scans);
        scans.sort_unstable();
        self.relations.get(scans.as_slice()).copied()
    }

    fn collect_scans(plan: &dyn ExecutionPlan, scans: &mut Vec<usize>) {
        if let Some(scan) = plan.downcast_ref::<ExternalTableScanExec>() {
            scans.push(scan.scan().index());
        } else if let Some(scan) = plan.downcast_ref::<ParallelTableScanExec>() {
            scans.push(scan.scan().index());
        }
        for child in plan.children() {
            Self::collect_scans(child.as_ref(), scans);
        }
    }
}
