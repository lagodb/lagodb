//! PostgreSQL evaluator boundaries used by query-offload policy and EXPLAIN.

use super::{QueryFragment, QueryNode};

impl QueryFragment {
    /// Count PostgreSQL evaluator boundaries in the validated fragment.
    ///
    /// This deliberately follows every expression-bearing node. The same fact
    /// drives auto-mode admission and the non-zero EXPLAIN warning, so policy
    /// and diagnostics cannot drift apart.
    pub fn postgres_fallback_count(&self) -> usize {
        Self::node_postgres_fallback_count(self.root())
    }

    fn node_postgres_fallback_count(node: &QueryNode) -> usize {
        match node {
            QueryNode::Scan(scan) => scan
                .filter()
                .map_or(0, |expression| expression.postgres_fallback_count()),
            QueryNode::Join(join) => {
                join.on_filter()
                    .map_or(0, |expression| expression.postgres_fallback_count())
                    + Self::node_postgres_fallback_count(join.left())
                    + Self::node_postgres_fallback_count(join.right())
            }
            QueryNode::Aggregate(aggregate) => {
                aggregate
                    .groups()
                    .iter()
                    .map(|group| group.expression().postgres_fallback_count())
                    .sum::<usize>()
                    + aggregate
                        .aggregates()
                        .iter()
                        .map(|aggregate| {
                            aggregate.argument().map_or(0, |argument| {
                                argument.postgres_fallback_count()
                            }) + aggregate
                                .filter()
                                .map_or(0, |filter| filter.postgres_fallback_count())
                                + aggregate
                                    .order_by()
                                    .iter()
                                    .map(|order| {
                                        order.expression().postgres_fallback_count()
                                    })
                                    .sum::<usize>()
                        })
                        .sum::<usize>()
                    + Self::node_postgres_fallback_count(aggregate.input())
            }
            QueryNode::Distinct(distinct) => {
                distinct
                    .keys()
                    .iter()
                    .map(|key| key.expression().postgres_fallback_count())
                    .sum::<usize>()
                    + Self::node_postgres_fallback_count(distinct.input())
            }
            QueryNode::Filter(filter) => {
                filter.predicate().postgres_fallback_count()
                    + Self::node_postgres_fallback_count(filter.input())
            }
            QueryNode::Project(project) => {
                project
                    .expressions()
                    .iter()
                    .map(|expression| {
                        expression.expression().postgres_fallback_count()
                    })
                    .sum::<usize>()
                    + Self::node_postgres_fallback_count(project.input())
            }
            QueryNode::Sort(sort) => Self::node_postgres_fallback_count(sort.input()),
            QueryNode::Limit(limit) => {
                Self::node_postgres_fallback_count(limit.input())
            }
        }
    }
}
