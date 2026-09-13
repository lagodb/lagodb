//! Shared projection and query-plan construction for relation trees.

use lagodb_core::expr::RuntimeValueLayout;
use lagodb_core::query_contract::OutputId;
use lagodb_query::plan::{
    ProjectExpr, ProjectNode, QueryFragment, QueryNode, QueryPlanData,
    QueryTupleLayout, QueryTupleSlot,
};
use pgrx::pg_sys;

use super::expression::{ExpressionScope, QueryExpressionPlanner};
use super::planned_query::PlannedQuery;
use super::relation_tree::{PlannedRelationInput, PlannedRelationTree};

/// Finalizes any provider-neutral relation tree into the CustomScan's direct
/// column projection. Ordinary joins and SubPlan-derived joins deliberately
/// share this layer so their output and runtime-value contracts cannot drift.
pub(super) struct RelationPlanBuilder {
    root: *mut pg_sys::PlannerInfo,
    path_target: *mut pg_sys::PathTarget,
    relation_input: Option<PlannedRelationInput>,
    scan_count: usize,
    expressions: QueryExpressionPlanner,
}

impl RelationPlanBuilder {
    pub(super) fn new(
        root: *mut pg_sys::PlannerInfo,
        path_target: *mut pg_sys::PathTarget,
        relation_tree: PlannedRelationTree,
    ) -> Self {
        let (relation_input, expressions) = relation_tree.into_parts();
        let scan_count = relation_input.scan_count();
        Self {
            root,
            path_target,
            relation_input: Some(relation_input),
            scan_count,
            expressions,
        }
    }

    pub(super) unsafe fn build(&mut self) -> Option<PlannedQuery> {
        let target = unsafe { &*self.path_target };
        let target_count = unsafe { pg_sys::list_length(target.exprs) };
        let mut project = Vec::with_capacity(target_count as usize);
        let mut slots = Vec::with_capacity(target_count as usize);
        let mut scan_target_exprs = Vec::with_capacity(target_count as usize);
        for index in 0..target_count {
            let expression = unsafe { pg_sys::list_nth(target.exprs, index) }
                .cast::<pg_sys::Expr>();
            let scalar = unsafe {
                self.expressions
                    .lower(expression.cast(), ExpressionScope::scalar(self.root))
            }
            .ok()?;
            let result_type = QueryExpressionPlanner::expr_type(expression);
            let output = OutputId::from_index(index as usize);
            project.push(ProjectExpr::new(scalar, result_type, output, true));
            slots.push(QueryTupleSlot::new(
                output,
                result_type.type_oid,
                result_type.typmod,
                result_type.collation,
                true,
            ));
            scan_target_exprs.push(expression);
        }
        let (input, scans) =
            self.relation_input.take()?.materialize(&self.expressions)?;
        let fragment = QueryFragment::new(QueryNode::Project(ProjectNode::new(
            input,
            project.into_boxed_slice(),
        )));
        let query = QueryPlanData::new(
            fragment,
            QueryTupleLayout::from_slots(slots.into_boxed_slice()),
            RuntimeValueLayout::new(self.expressions.take_runtime_layout()),
            self.scan_count,
        )
        .ok()?;
        Some(PlannedQuery {
            query,
            runtime_exprs: self.expressions.take_runtime_exprs(),
            scan_target_exprs,
            scans,
        })
    }
}
