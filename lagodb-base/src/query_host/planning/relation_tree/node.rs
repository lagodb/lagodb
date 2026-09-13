//! Planner relation-tree nodes and late scan materialization.

use lagodb_core::expr::ColumnRef;
use lagodb_core::query_contract::ScanId;
use lagodb_query::plan::{
    ExecutionExpr, FilterNode, JoinKey, JoinNode, JoinType, QueryNode, ScanNode,
};
use pgrx::pg_sys;

use super::super::candidate::ScannableRelation;
use super::super::expression::QueryExpressionPlanner;
use super::super::planned_query::PlannedScanInput;
use super::super::table_scan_filter::TableScanFilter;

pub(in crate::query_host::planning) struct PlannedRelationInput {
    pub(super) root: RelationNode,
    pub(super) scan_count: usize,
}

impl PlannedRelationInput {
    pub(in crate::query_host::planning) const fn scan_count(&self) -> usize {
        self.scan_count
    }

    pub(in crate::query_host::planning) fn materialize(
        self,
        expressions: &QueryExpressionPlanner,
    ) -> Option<(QueryNode, Box<[PlannedScanInput]>)> {
        let mut scans = Vec::with_capacity(self.scan_count);
        scans.resize_with(self.scan_count, || None);
        let root = self.root.materialize(expressions, &mut scans)?;
        let scans = scans
            .into_iter()
            .collect::<Option<Vec<_>>>()?
            .into_boxed_slice();
        Some((root, scans))
    }
}

pub(super) enum RelationNode {
    Scan(RelationScan),
    Join {
        join_type: JoinType,
        left: Box<Self>,
        right: Box<Self>,
        keys: Vec<JoinKey>,
        on_filters: Vec<ExecutionExpr>,
        null_aware: bool,
        mark_filter: Option<RelationMarkFilter>,
        estimated_key_rows: f64,
        estimated_rows: f64,
    },
    Filter {
        input: Box<Self>,
        predicates: Vec<ExecutionExpr>,
        estimated_rows: f64,
    },
}

#[derive(Clone, Copy)]
pub(super) struct RelationMarkFilter {
    pub(super) null_test: ColumnRef,
    pub(super) anti: bool,
}

pub(super) struct RelationScan {
    pub(super) root: *mut pg_sys::PlannerInfo,
    pub(super) scan: ScanId,
    pub(super) relation: ScannableRelation,
    pub(super) filter: Option<RelationScanFilter>,
    pub(super) estimated_rows: f64,
}

pub(super) struct RelationScanFilter {
    pub(super) exact: ExecutionExpr,
    pub(super) source: *mut pg_sys::Node,
}

impl RelationNode {
    pub(super) fn estimated_rows(&self) -> f64 {
        match self {
            Self::Scan(node) => node.estimated_rows,
            Self::Join { estimated_rows, .. } => *estimated_rows,
            Self::Filter { estimated_rows, .. } => *estimated_rows,
        }
    }

    pub(super) fn contains_join(&self) -> bool {
        match self {
            Self::Scan(_) => false,
            Self::Join { .. } => true,
            Self::Filter { input, .. } => input.contains_join(),
        }
    }

    /// Whether this subtree emits columns belonging to `scan`.
    pub(super) fn emits(&self, scan: ScanId) -> bool {
        match self {
            Self::Scan(node) => node.scan == scan,
            Self::Join {
                join_type,
                left,
                right,
                ..
            } => left.emits(scan) || (join_type.emits_right() && right.emits(scan)),
            Self::Filter { input, .. } => input.emits(scan),
        }
    }

    pub(super) unsafe fn add_relids(
        &self,
        mut relids: *mut pg_sys::Bitmapset,
    ) -> *mut pg_sys::Bitmapset {
        match self {
            Self::Scan(node) => unsafe {
                pg_sys::bms_add_member(relids, node.relation.range_table_index as i32)
            },
            Self::Join { left, right, .. } => {
                relids = unsafe { left.add_relids(relids) };
                unsafe { right.add_relids(relids) }
            }
            Self::Filter { input, .. } => unsafe { input.add_relids(relids) },
        }
    }

    fn materialize(
        self,
        expressions: &QueryExpressionPlanner,
        scans: &mut [Option<PlannedScanInput>],
    ) -> Option<QueryNode> {
        match self {
            Self::Scan(scan) => {
                let columns = expressions.columns_for_scan(scan.scan);
                let projected_columns =
                    columns.iter().map(|column| column.attno).collect();
                let (exact_filter, table_scan_filter) = match scan.filter {
                    Some(filter) => (
                        Some(filter.exact),
                        Some(TableScanFilter::new(filter.source)),
                    ),
                    None => (None, None),
                };
                let planned = PlannedScanInput {
                    scan: scan.scan,
                    root: scan.root,
                    input_rel: scan.relation.input_rel,
                    range_table_index: scan.relation.range_table_index,
                    range_table_entry: scan.relation.range_table_entry,
                    projected_columns,
                    table_scan_filter,
                    estimated_rows: scan.estimated_rows,
                };
                let destination = scans.get_mut(scan.scan.index())?;
                if destination.replace(planned).is_some() {
                    return None;
                }
                Some(QueryNode::Scan(ScanNode::new(
                    scan.scan,
                    columns.into_boxed_slice(),
                    exact_filter,
                )))
            }
            Self::Join {
                join_type,
                left,
                right,
                keys,
                on_filters,
                null_aware,
                mark_filter,
                estimated_key_rows,
                estimated_rows,
            } => {
                let left = left.materialize(expressions, scans)?;
                let right = right.materialize(expressions, scans)?;
                let join = if let Some(mark_filter) = mark_filter {
                    let [key] = keys.as_slice() else {
                        return None;
                    };
                    JoinNode::new_filtered_mark_subplan(
                        left,
                        right,
                        *key,
                        mark_filter.null_test,
                        mark_filter.anti,
                        estimated_key_rows,
                        estimated_rows,
                    )
                    .ok()?
                } else if null_aware {
                    let [key] = keys.as_slice() else {
                        return None;
                    };
                    JoinNode::new_null_aware_anti(
                        left,
                        right,
                        *key,
                        estimated_key_rows,
                        estimated_rows,
                    )
                    .ok()?
                } else {
                    JoinNode::new(
                        join_type,
                        left,
                        right,
                        keys.into_boxed_slice(),
                        Self::and(on_filters),
                        estimated_key_rows,
                        estimated_rows,
                    )
                    .ok()?
                };
                Some(QueryNode::Join(join))
            }
            Self::Filter {
                input,
                predicates,
                estimated_rows,
            } => {
                let input = input.materialize(expressions, scans)?;
                match Self::and(predicates) {
                    Some(predicate) => Some(QueryNode::Filter(
                        FilterNode::new(input, predicate, estimated_rows).ok()?,
                    )),
                    None => Some(input),
                }
            }
        }
    }

    fn and(mut expressions: Vec<ExecutionExpr>) -> Option<ExecutionExpr> {
        match expressions.len() {
            0 => None,
            1 => expressions.pop(),
            _ => Some(ExecutionExpr::And(expressions.into_boxed_slice())),
        }
    }
}
