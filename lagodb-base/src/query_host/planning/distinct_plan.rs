//! Query-level DISTINCT recognition and provider-neutral IR construction.

use lagodb_core::expr::RuntimeValueLayout;
use lagodb_core::query_contract::{OutputId, ScanId};
use lagodb_query::plan::{
    DistinctExpr, DistinctNode, ProjectNode, QueryFragment, QueryNode, QueryPlanData,
    QueryTupleLayout, QueryTupleSlot, ScanNode,
};
use pgrx::pg_sys;

use super::aggregate_plan::PlannedQuery;
use super::candidate::SingleRelationCandidate;
use super::expression::{
    ExpressionScope, ExpressionSourceCatalog, PredicateDomain, QueryExpressionPlanner,
};
use super::table_scan_filter::TableScanFilter;

pub(super) struct DistinctPlanBuilder<'a> {
    candidate: &'a SingleRelationCandidate,
    expressions: QueryExpressionPlanner,
}

impl<'a> DistinctPlanBuilder<'a> {
    pub(super) fn new(candidate: &'a SingleRelationCandidate) -> Self {
        Self {
            candidate,
            expressions: QueryExpressionPlanner::new(
                ExpressionSourceCatalog::for_relation(
                    candidate.range_table_index,
                    ScanId::from_index(0),
                ),
            ),
        }
    }

    pub(super) unsafe fn build(&mut self) -> Option<PlannedQuery> {
        let parse = unsafe { &*(*self.candidate.root).parse };
        let table_scan_filter = if unsafe { (*parse.jointree).quals.is_null() } {
            None
        } else {
            let predicate = unsafe {
                self.expressions.lower(
                    (*parse.jointree).quals,
                    ExpressionScope::predicate(PredicateDomain::Exact),
                )
            }
            .ok()?;
            Some(TableScanFilter::new(predicate, unsafe {
                (*parse.jointree).quals
            }))
        };

        let target = unsafe { &*self.candidate.path_target };
        let count = unsafe { pg_sys::list_length(target.exprs) };
        let mut keys = Vec::with_capacity(count as usize);
        let mut project = Vec::with_capacity(count as usize);
        let mut slots = Vec::with_capacity(count as usize);
        for index in 0..count {
            let expression = unsafe { pg_sys::list_nth(target.exprs, index) }
                .cast::<pg_sys::Expr>();
            let direct =
                unsafe { QueryExpressionPlanner::unwrap_relabel(expression) };
            if unsafe { (*direct).type_ } != pg_sys::NodeTag::T_Var {
                return None;
            }
            let scalar = unsafe {
                self.expressions
                    .lower(expression.cast(), ExpressionScope::scalar())
            }
            .ok()?;
            let result_type = QueryExpressionPlanner::expr_type(expression);
            let output = OutputId::from_index(index as usize);
            let key = DistinctExpr::try_new(scalar, result_type, output).ok()?;
            project.push(output);
            slots.push(QueryTupleSlot::new(
                output,
                result_type.type_oid,
                result_type.typmod,
                result_type.collation,
                true,
            ));
            keys.push(key);
        }
        if keys.is_empty() {
            return None;
        }

        let columns = self.expressions.columns();
        let projected_columns = columns.iter().map(|column| column.attno).collect();
        let scan = QueryNode::Scan(ScanNode::new(
            ScanId::from_index(0),
            columns.into_boxed_slice(),
            table_scan_filter
                .as_ref()
                .map(|filter| filter.exact_residual().clone()),
        ));
        let distinct = QueryNode::Distinct(
            DistinctNode::new(scan, keys.into_boxed_slice()).ok()?,
        );
        let fragment = QueryFragment::new(QueryNode::Project(
            ProjectNode::new(distinct, project.into_boxed_slice()).ok()?,
        ));
        let query = QueryPlanData::new(
            fragment,
            QueryTupleLayout::from_slots(slots.into_boxed_slice()).ok()?,
            RuntimeValueLayout::new(self.expressions.take_runtime_layout()),
            1,
        )
        .ok()?;
        Some(PlannedQuery {
            query,
            runtime_exprs: self.expressions.take_runtime_exprs(),
            scan_target_exprs: (0..count)
                .map(|index| unsafe { pg_sys::list_nth(target.exprs, index) }.cast())
                .collect(),
            projected_columns,
            table_scan_filter,
        })
    }
}
