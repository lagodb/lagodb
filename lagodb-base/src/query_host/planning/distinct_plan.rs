//! Query-level DISTINCT recognition over the shared relation tree.

use lagodb_core::expr::RuntimeValueLayout;
use lagodb_core::query_contract::OutputId;
use lagodb_query::plan::{
    DistinctExpr, DistinctNode, ExecutionExpr, ProjectExpr, ProjectNode,
    QueryFragment, QueryNode, QueryPlanData, QueryTupleLayout, QueryTupleSlot,
};
use pgrx::pg_sys;

use super::candidate::SingleRelationCandidate;
use super::expression::{ExpressionScope, QueryExpressionPlanner};
use super::path_installation::QueryPathInstallation;
use super::planned_query::PlannedQuery;
use super::query_shape::QueryShape;
use super::relation_tree::{
    PlannedRelationInput, PlannedRelationTree, RelationTreePlanner,
};
use crate::gucs::{QueryOffloadMode, query_offload_mode};
use crate::query_host::error::QueryHostError;

pub(super) struct DistinctCandidate;

impl DistinctCandidate {
    pub(super) unsafe fn install(
        root: *mut pg_sys::PlannerInfo,
        stage: pg_sys::UpperRelationKind::Type,
        input_rel: *mut pg_sys::RelOptInfo,
        output_rel: *mut pg_sys::RelOptInfo,
    ) -> Result<bool, QueryHostError> {
        if query_offload_mode() == QueryOffloadMode::Off
            || stage != pg_sys::UpperRelationKind::UPPERREL_DISTINCT
        {
            return Ok(false);
        }
        let parse = unsafe { &*(*root).parse };
        // PostgreSQL intentionally leaves the primary DISTINCT upper rel's
        // reltarget empty.  The stage target exported for extensions is the
        // sort-input target used by the native DISTINCT paths.
        let path_target = unsafe {
            (*root).upper_targets
                [pg_sys::UpperRelationKind::UPPERREL_DISTINCT as usize]
        };
        if !QueryShape::new(parse).supports_distinct()
            || !unsafe {
                SingleRelationCandidate::target_list_is_safe(root, parse.targetList)
            }
            || !unsafe {
                SingleRelationCandidate::expression_list_is_safe(
                    root,
                    (*path_target).exprs,
                )
            }
        {
            return Ok(false);
        }
        let Some(relation_tree) =
            (unsafe { RelationTreePlanner::build_upper_input(root, input_rel) })
        else {
            return Ok(false);
        };
        let output_rows = unsafe { Self::first_path_rows(output_rel) };
        let mut builder =
            DistinctPlanBuilder::new(root, path_target, relation_tree, output_rows);
        let Some(planned) = (unsafe { builder.build() }) else {
            return Ok(false);
        };
        QueryPathInstallation::new(output_rel, path_target, planned, output_rows)
            .install()?;
        Ok(true)
    }

    unsafe fn first_path_rows(relation: *mut pg_sys::RelOptInfo) -> f64 {
        // PostgreSQL has costed at least one native DISTINCT path before the
        // upper-path hook and remains the authority for result cardinality.
        let path = unsafe { pg_sys::list_nth((*relation).pathlist, 0) }
            .cast::<pg_sys::Path>();
        unsafe { (*path).rows }
    }
}

struct DistinctPlanBuilder {
    root: *mut pg_sys::PlannerInfo,
    path_target: *mut pg_sys::PathTarget,
    input: Option<PlannedRelationInput>,
    scan_count: usize,
    estimated_rows: f64,
    expressions: QueryExpressionPlanner,
}

impl DistinctPlanBuilder {
    fn new(
        root: *mut pg_sys::PlannerInfo,
        path_target: *mut pg_sys::PathTarget,
        relation_tree: PlannedRelationTree,
        estimated_rows: f64,
    ) -> Self {
        let (input, expressions) = relation_tree.into_parts();
        let scan_count = input.scan_count();
        Self {
            root,
            path_target,
            input: Some(input),
            scan_count,
            estimated_rows,
            expressions,
        }
    }

    unsafe fn build(&mut self) -> Option<PlannedQuery> {
        let target = unsafe { &*self.path_target };
        let count = unsafe { pg_sys::list_length(target.exprs) };
        let mut keys = Vec::with_capacity(count as usize);
        let mut project = Vec::with_capacity(count as usize);
        let mut slots = Vec::with_capacity(count as usize);
        let mut scan_target_exprs = Vec::with_capacity(count as usize);
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
                    .lower(expression.cast(), ExpressionScope::scalar(self.root))
            }
            .ok()?;
            let result_type = QueryExpressionPlanner::expr_type(expression);
            let output = OutputId::from_index(index as usize);
            keys.push(DistinctExpr::try_new(scalar, result_type, output).ok()?);
            project.push(ProjectExpr::new(
                ExecutionExpr::Output(output),
                result_type,
                output,
                true,
            ));
            slots.push(QueryTupleSlot::new(
                output,
                result_type.type_oid,
                result_type.typmod,
                result_type.collation,
                true,
            ));
            scan_target_exprs.push(expression);
        }
        if keys.is_empty() {
            return None;
        }
        let (input, scans) = self.input.take()?.materialize(&self.expressions)?;
        let distinct = QueryNode::Distinct(
            DistinctNode::new(input, keys.into_boxed_slice(), self.estimated_rows)
                .ok()?,
        );
        let fragment = QueryFragment::new(QueryNode::Project(ProjectNode::new(
            distinct,
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
