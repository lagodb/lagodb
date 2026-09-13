//! Aggregate expression recognition and provider-neutral query IR construction.

use std::ffi::{CStr, CString, c_int, c_void};

use lagodb_core::expr::{ExprType, RuntimeValueLayout};
use lagodb_core::query_contract::OutputId;
use lagodb_query::plan::{
    AggCall, AggregateArguments, AggregateNode, AggregateOrderExpr, ExecutionExpr,
    FilterNode, GroupExpr, ProjectExpr, ProjectNode, QueryFragment, QueryNode,
    QueryPlanData, QueryTupleLayout, QueryTupleSlot, ScalarSemantics, SortDirection,
};
use pgrx::pg_sys;

use super::expression::{
    ExpressionScope, OutputCatalog, PredicateDomain, QueryExpressionPlanner,
    ResolvedOutput,
};
use super::planned_query::PlannedQuery;
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
        let mut groups = Vec::new();
        let processed_groups = unsafe { (*self.root).processed_groupClause };
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
            let scalar = unsafe {
                self.expressions
                    .lower((*target).expr.cast(), ExpressionScope::scalar(self.root))
            }
            .ok()?;
            let result_type =
                QueryExpressionPlanner::expr_type(unsafe { (*target).expr });
            if !ScalarSemantics::Exact.supports_grouping(
                result_type,
                unsafe { (*clause).eqop },
                unsafe { (*clause).hashable },
            ) {
                return None;
            }
            let output = OutputId::from_index(groups.len());
            groups.push((
                unsafe { (*target).expr },
                GroupExpr::try_new(scalar, result_type, output).ok()?,
            ));
        }

        let mut aggregates: Vec<(*mut pg_sys::Expr, AggCall)> = Vec::new();
        let mut project = Vec::new();
        let mut slots = Vec::new();
        let mut scan_target_exprs = Vec::new();
        let output_target = self.path_target;
        let target_count = unsafe { pg_sys::list_length((*output_target).exprs) };
        for index in 0..target_count {
            let expression =
                unsafe { pg_sys::list_nth((*output_target).exprs, index) }
                    .cast::<pg_sys::Expr>();
            let (semantic_output, scan_expression) = if let Some((_, group)) =
                groups.iter().find(|(group_expression, _)| unsafe {
                    pg_sys::equal(
                        group_expression.cast::<c_void>(),
                        expression.cast::<c_void>(),
                    )
                }) {
                (group.output(), expression)
            } else {
                let aggregate_expression =
                    unsafe { Self::single_aggregate_output(expression) }?;
                if let Some((_, aggregate)) =
                    aggregates.iter().find(|(existing, _)| unsafe {
                        pg_sys::equal(
                            existing.cast::<c_void>(),
                            aggregate_expression.cast::<c_void>(),
                        )
                    })
                {
                    (aggregate.output(), aggregate_expression)
                } else {
                    let output =
                        OutputId::from_index(groups.len() + aggregates.len());
                    let aggregate = unsafe {
                        self.build_aggregate(
                            aggregate_expression.cast::<pg_sys::Aggref>(),
                            output,
                        )
                    }?;
                    aggregates.push((aggregate_expression, aggregate));
                    (output, aggregate_expression)
                }
            };
            let result_type = QueryExpressionPlanner::expr_type(scan_expression);
            let nullable = aggregates
                .iter()
                .find(|(_, aggregate)| aggregate.output() == semantic_output)
                .is_none_or(|(_, aggregate)| aggregate.nullable());
            let physical_output = OutputId::from_index(index as usize);
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
                if let Some((_, aggregate)) =
                    aggregates.iter().find(|(existing, _)| unsafe {
                        pg_sys::equal(
                            existing.cast::<c_void>(),
                            expression.cast::<c_void>(),
                        )
                    })
                {
                    if !aggregate.supports_having_result() {
                        return None;
                    }
                    continue;
                }
                let output = OutputId::from_index(groups.len() + aggregates.len());
                let aggregate = unsafe {
                    self.build_aggregate(expression.cast::<pg_sys::Aggref>(), output)
                }?;
                if !aggregate.supports_having_result() {
                    return None;
                }
                aggregates.push((expression, aggregate));
            }
            let catalog = HavingOutputCatalog {
                entries: groups
                    .iter()
                    .map(|(expression, group)| {
                        (*expression, group.output(), group.result_type())
                    })
                    .chain(aggregates.iter().map(|(expression, aggregate)| {
                        (
                            *expression,
                            aggregate.output(),
                            aggregate.execution_result_type(),
                        )
                    }))
                    .collect(),
            };
            Some(
                unsafe {
                    self.expressions.lower(
                        parse.havingQual,
                        ExpressionScope::output_predicate(
                            self.root,
                            &catalog,
                            PredicateDomain::Having,
                        ),
                    )
                }
                .ok()?,
            )
        };
        if (groups.is_empty() && aggregates.is_empty()) || project.is_empty() {
            return None;
        }

        let (input, scans) = self.input.take()?.materialize(&self.expressions)?;
        let aggregate = QueryNode::Aggregate(
            AggregateNode::new(
                input,
                groups
                    .into_iter()
                    .map(|(_, group)| group)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                aggregates
                    .into_iter()
                    .map(|(_, aggregate)| aggregate)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                self.aggregate_rows,
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
        })
    }

    /// Return the sole aggregate computed by one output expression.
    ///
    /// PostgreSQL evaluates wrappers such as `COALESCE(COUNT(*), 0)` in the
    /// CustomScan result projection. The scan slot carries only the raw Aggref,
    /// which `set_customscan_references()` rewrites to `INDEX_VAR`. Expressions
    /// that also read a grouping Var need more than one scan-slot input and are
    /// therefore outside this one-output/one-slot contract.
    unsafe fn single_aggregate_output(
        expression: *mut pg_sys::Expr,
    ) -> Option<*mut pg_sys::Expr> {
        if unsafe { (*expression).type_ } == pg_sys::NodeTag::T_Aggref {
            return Some(expression);
        }
        let references = unsafe {
            pg_sys::pull_var_clause(
                expression.cast(),
                pg_sys::PVC_INCLUDE_AGGREGATES as c_int,
            )
        };
        if unsafe { pg_sys::list_length(references) } != 1 {
            return None;
        }
        let reference =
            unsafe { pg_sys::list_nth(references, 0) }.cast::<pg_sys::Expr>();
        (unsafe { (*reference).type_ } == pg_sys::NodeTag::T_Aggref)
            .then_some(reference)
    }

    unsafe fn build_aggregate(
        &mut self,
        aggregate: *mut pg_sys::Aggref,
        output: OutputId,
    ) -> Option<AggCall> {
        let aggregate = unsafe { &*aggregate };
        if !aggregate.aggdirectargs.is_null()
            || aggregate.aggvariadic
            || aggregate.agglevelsup != 0
            || aggregate.aggsplit != pg_sys::AggSplit::AGGSPLIT_SIMPLE
        {
            return None;
        }
        let semantic_arguments = unsafe { Self::semantic_arguments(aggregate.args) };
        let arguments = if aggregate.aggstar {
            if !semantic_arguments.is_empty() {
                return None;
            }
            AggregateArguments::None
        } else if u32::from(aggregate.aggfnoid) == pg_sys::F_STRING_AGG_TEXT_TEXT {
            let &[value, delimiter] = semantic_arguments.as_slice() else {
                return None;
            };
            let value = unsafe {
                self.expressions
                    .lower((*value).expr.cast(), ExpressionScope::scalar(self.root))
            }
            .ok()?;
            let delimiter = unsafe { Self::string_agg_delimiter((*delimiter).expr) }?;
            AggregateArguments::StringAgg { value, delimiter }
        } else {
            let &[target] = semantic_arguments.as_slice() else {
                return None;
            };
            AggregateArguments::Unary(
                unsafe {
                    self.expressions.lower(
                        (*target).expr.cast(),
                        ExpressionScope::scalar(self.root),
                    )
                }
                .ok()?,
            )
        };
        let distinct = !aggregate.aggdistinct.is_null();
        let retain_order = matches!(
            u32::from(aggregate.aggfnoid),
            pg_sys::F_ARRAY_AGG_ANYNONARRAY | pg_sys::F_STRING_AGG_TEXT_TEXT
        );
        let order_by =
            unsafe { self.build_aggregate_order_by(aggregate, retain_order) }?;
        let filter = if aggregate.aggfilter.is_null() {
            None
        } else {
            Some(
                unsafe {
                    self.expressions.lower(
                        aggregate.aggfilter.cast(),
                        ExpressionScope::predicate(self.root, PredicateDomain::Exact),
                    )
                }
                .ok()?,
            )
        };
        AggCall::try_new(
            aggregate.aggfnoid,
            arguments,
            distinct,
            order_by.into_boxed_slice(),
            filter,
            ExprType {
                type_oid: aggregate.aggtype,
                typmod: unsafe {
                    pg_sys::exprTypmod(aggregate as *const _ as *const pg_sys::Node)
                },
                collation: aggregate.aggcollid,
            },
            output,
        )
        .ok()
    }

    unsafe fn semantic_arguments(
        arguments: *mut pg_sys::List,
    ) -> Vec<*mut pg_sys::TargetEntry> {
        let count = unsafe { pg_sys::list_length(arguments) };
        let mut semantic = Vec::with_capacity(count as usize);
        for index in 0..count {
            let target = unsafe { pg_sys::list_nth(arguments, index) }
                .cast::<pg_sys::TargetEntry>();
            if !unsafe { (*target).resjunk } {
                semantic.push(target);
            }
        }
        semantic
    }

    unsafe fn build_aggregate_order_by(
        &mut self,
        aggregate: &pg_sys::Aggref,
        retain_order: bool,
    ) -> Option<Vec<AggregateOrderExpr>> {
        let count = unsafe { pg_sys::list_length(aggregate.aggorder) };
        let mut order_by = Vec::with_capacity(count as usize);
        for index in 0..count {
            let clause = unsafe { pg_sys::list_nth(aggregate.aggorder, index) }
                .cast::<pg_sys::SortGroupClause>();
            let target = unsafe {
                pg_sys::get_sortgroupref_tle(
                    (*clause).tleSortGroupRef,
                    aggregate.args,
                )
            };
            if target.is_null() {
                return None;
            }
            let target_expression = unsafe { (*target).expr };
            let direct =
                unsafe { QueryExpressionPlanner::unwrap_relabel(target_expression) };
            if unsafe { (*direct).type_ } != pg_sys::NodeTag::T_Var {
                return None;
            }
            let mut opfamily = pg_sys::InvalidOid;
            let mut opcintype = pg_sys::InvalidOid;
            let mut strategy = 0_i16;
            if !unsafe {
                pg_sys::get_ordering_op_properties(
                    (*clause).sortop,
                    &mut opfamily,
                    &mut opcintype,
                    &mut strategy,
                )
            } {
                return None;
            }
            let direction = match u32::try_from(strategy).ok()? {
                pg_sys::BTLessStrategyNumber => SortDirection::Ascending,
                pg_sys::BTGreaterStrategyNumber => SortDirection::Descending,
                _ => return None,
            };
            if retain_order {
                let expression = unsafe {
                    self.expressions.lower(
                        target_expression.cast(),
                        ExpressionScope::scalar(self.root),
                    )
                }
                .ok()?;
                order_by.push(
                    AggregateOrderExpr::try_new(expression, direction, unsafe {
                        (*clause).nulls_first
                    })
                    .ok()?,
                );
            }
        }
        Some(order_by)
    }

    unsafe fn string_agg_delimiter(expression: *mut pg_sys::Expr) -> Option<CString> {
        let expression =
            unsafe { QueryExpressionPlanner::unwrap_relabel(expression) };
        if unsafe { (*expression).type_ } != pg_sys::NodeTag::T_Const {
            return None;
        }
        let constant = expression.cast::<pg_sys::Const>();
        if unsafe { (*constant).consttype != pg_sys::TEXTOID } {
            return None;
        }
        // PostgreSQL treats a NULL STRING_AGG delimiter as no separator.
        // DataFusion requires a non-null literal delimiter, so the empty
        // literal is the equivalent representation at this plan boundary.
        if unsafe { (*constant).constisnull } {
            return Some(c"".to_owned());
        }
        let text =
            unsafe { (*constant).constvalue.cast_mut_ptr::<pg_sys::varlena>() };
        let delimiter = unsafe { pg_sys::text_to_cstring(text) };
        (!delimiter.is_null())
            .then(|| unsafe { CStr::from_ptr(delimiter).to_owned() })
    }
}

struct HavingOutputCatalog {
    entries: Vec<(*mut pg_sys::Expr, OutputId, ExprType)>,
}

impl OutputCatalog for HavingOutputCatalog {
    unsafe fn resolve_output(
        &self,
        expression: *mut pg_sys::Expr,
    ) -> Option<ResolvedOutput> {
        self.entries
            .iter()
            .find_map(|(candidate, output, execution_type)| {
                unsafe {
                    pg_sys::equal(
                        candidate.cast::<c_void>(),
                        expression.cast::<c_void>(),
                    )
                }
                .then_some(ResolvedOutput {
                    output: *output,
                    execution_type: *execution_type,
                })
            })
    }
}
