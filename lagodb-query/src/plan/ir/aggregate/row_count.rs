//! Scope of eager task planning for metadata COUNT(*).

use lagodb_core::query_contract::ScanId;

use super::{AggregateArguments, AggregateKind};
use crate::plan::{ExecutionExpr, QueryFragment, QueryNode};

impl QueryFragment {
    /// Only a single unfiltered COUNT(*) with a direct output projection may
    /// enumerate files before physical optimization. In particular, LIMIT and
    /// constant projections retain their existing lazy scan behavior.
    pub(crate) fn metadata_count_scan(&self) -> Option<ScanId> {
        let QueryNode::Project(project) = self.root() else {
            return None;
        };
        let [output] = project.expressions() else {
            return None;
        };
        let QueryNode::Aggregate(aggregate) = project.input() else {
            return None;
        };
        let [count] = aggregate.aggregates() else {
            return None;
        };
        if !aggregate.groups().is_empty()
            || count.kind() != AggregateKind::Count
            || !matches!(count.arguments(), AggregateArguments::None)
            || count.is_distinct()
            || !count.order_by().is_empty()
            || count.filter().is_some()
            || !matches!(output.expression(), ExecutionExpr::Output(id) if *id == count.output())
        {
            return None;
        }
        let QueryNode::Scan(scan) = aggregate.input() else {
            return None;
        };
        (scan.filter().is_none() && scan.columns().is_empty()).then_some(scan.scan())
    }
}
