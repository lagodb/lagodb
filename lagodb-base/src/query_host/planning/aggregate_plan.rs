//! Aggregate expression recognition and provider-neutral query IR construction.

use std::ffi::c_int;

use lagodb_core::expr::RuntimeValueLayout;
use lagodb_core::query_contract::OutputId;
use lagodb_query::plan::{
    AggCall, AggregateNode, ExecutionExpr, FilterNode, ProjectExpr, ProjectNode,
    QueryFragment, QueryNode, QueryPlanData, QueryTupleLayout, QueryTupleSlot,
};
use pgrx::pg_sys;

use super::aggregate_block::AggregateBlockBuilder;
use super::aggregate_output::AggregateScanSlots;
use super::expression::{PredicateDomain, QueryExpressionPlanner};
use super::planned_query::PlannedQuery;
use super::planner_info::PlannerInfoExt;
use super::relation_tree::{PlannedRelationInput, PlannedRelationTree};

pub(super) struct AggregatePlanBuilder {
    input: Option<PlannedRelationInput>,
    root: *mut pg_sys::PlannerInfo,
    path_target: *mut pg_sys::PathTarget,
    scan_count: usize,
    aggregate_rows: f64,
    output_rows: f64,
    expressions: QueryExpressionPlanner,
}

impl AggregatePlanBuilder {
    pub(super) fn over_relation_tree(
        root: *mut pg_sys::PlannerInfo,
        path_target: *mut pg_sys::PathTarget,
        relation_tree: PlannedRelationTree,
        aggregate_rows: f64,
        output_rows: f64,
    ) -> Self {
        let (input, expressions) = relation_tree.into_parts();
        let scan_count = input.scan_count();
        Self {
            input: Some(input),
            root,
            path_target,
            scan_count,
            aggregate_rows,
            output_rows,
            expressions,
        }
    }

    pub(super) unsafe fn build(&mut self) -> Option<PlannedQuery> {
        let parse = unsafe { &*(*self.root).parse };
        let mut block =
            AggregateBlockBuilder::new(self.root, &mut self.expressions, 0);
        let processed_groups = unsafe { (*self.root).processed_group_clause() };
        let group_count = unsafe { pg_sys::list_length(processed_groups) };
        for index in 0..group_count {
            let clause = unsafe { pg_sys::list_nth(processed_groups, index) }
                .cast::<pg_sys::SortGroupClause>();
            let target = unsafe {
                pg_sys::get_sortgroupref_tle(
                    (*clause).tleSortGroupRef,
                    parse.targetList,
                )
            };
            if target.is_null() {
                return None;
            }
            unsafe {
                block.add_group((*target).expr, (*clause).eqop, (*clause).hashable)
            }?;
        }

        let mut scan_slots = AggregateScanSlots::default();
        let output_target = self.path_target;
        let target_count = unsafe { pg_sys::list_length((*output_target).exprs) };
        for index in 0..target_count {
            let expression =
                unsafe { pg_sys::list_nth((*output_target).exprs, index) }
                    .cast::<pg_sys::Expr>();
            unsafe { scan_slots.add_target(expression, &mut block) }?;
        }
        if scan_slots.is_empty() {
            return None;
        }
        let mut project = Vec::new();
        let mut slots = Vec::new();
        let mut scan_target_exprs = Vec::new();
        for (index, (scan_expression, semantic_output)) in
            scan_slots.iter().enumerate()
        {
            let result_type = QueryExpressionPlanner::expr_type(scan_expression);
            let nullable = block
                .aggregate(semantic_output)
                .is_none_or(AggCall::nullable);
            let physical_output = OutputId::from_index(index);
            project.push(ProjectExpr::new(
                ExecutionExpr::Output(semantic_output),
                result_type,
                physical_output,
                nullable,
            ));
            scan_target_exprs.push(scan_expression);
            slots.push(QueryTupleSlot::new(
                physical_output,
                result_type.type_oid,
                result_type.typmod,
                result_type.collation,
                nullable,
            ));
        }
        let having = if parse.havingQual.is_null() {
            None
        } else {
            let pulled = unsafe {
                pg_sys::pull_var_clause(
                    parse.havingQual,
                    pg_sys::PVC_INCLUDE_AGGREGATES as c_int,
                )
            };
            let count = unsafe { pg_sys::list_length(pulled) };
            for index in 0..count {
                let expression =
                    unsafe { pg_sys::list_nth(pulled, index) }.cast::<pg_sys::Expr>();
                if unsafe { (*expression).type_ } != pg_sys::NodeTag::T_Aggref {
                    continue;
                }
                // Register the Aggref so HAVING can reference its output.
                unsafe { block.aggregate_output(expression) }?;
            }
            Some(unsafe {
                block.lower_over_outputs(
                    parse.havingQual,
                    Some(PredicateDomain::Having),
                )
            }?)
        };
        let (groups, aggregates) = block.into_parts();

        let (input, scans) = self.input.take()?.materialize(&self.expressions)?;
        let aggregate = QueryNode::Aggregate(
            AggregateNode::new(
                input,
                groups,
                aggregates,
                self.aggregate_rows,
                unsafe { (*self.path_target).width },
            )
            .ok()?,
        );
        let aggregate = match having {
            Some(predicate) => QueryNode::Filter(
                FilterNode::new(aggregate, predicate, self.output_rows).ok()?,
            ),
            None => aggregate,
        };
        let fragment = QueryFragment::new(QueryNode::Project(ProjectNode::new(
            aggregate,
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
            // PostgreSQL evaluates the target over the raw grouping and
            // Aggref slots; its PathTarget cost charges exactly that work
            // (`set_pathtarget_cost_width`, Aggrefs free like Vars).
            postgres_projection_cost: unsafe { (*self.path_target).cost },
        })
    }
}
