//! Shared relational lowering for planning admission and physical execution.
//!
//! Only leaf bindings differ: planning uses schemas and typed runtime values;
//! execution uses bound providers, evaluated values and PostgreSQL UDF state.

use datafusion::common::{Column, DataFusionError, NullEquality};
use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilderOptions;
use datafusion::logical_expr::{
    Expr, JoinType as DataFusionJoinType, LogicalPlan, LogicalPlanBuilder, lit,
};
use lagodb_core::expr::{RuntimeValue, RuntimeValueLayout};

use crate::datafusion::postgres_eval::PgExprRuntime;
use crate::datafusion::scan_binding::ScanBindings;
use crate::plan::{ExecutionExpr, JoinType, QueryNode, SortDirection};

use super::DataFusionPlanError;
use super::expression::DataFusionExpressionCompiler;
use super::runtime_value::LimitWindow;

pub(in crate::datafusion) struct DataFusionLogicalPlanCompiler<'a> {
    scans: &'a ScanBindings,
    expressions: DataFusionExpressionCompiler<'a>,
    values: Option<&'a [RuntimeValue]>,
}

impl<'a> DataFusionLogicalPlanCompiler<'a> {
    pub(super) fn new(
        scans: &'a ScanBindings,
        values: &'a [RuntimeValue],
        postgres: PgExprRuntime,
    ) -> Self {
        Self {
            scans,
            expressions: DataFusionExpressionCompiler::new(scans, values, postgres),
            values: Some(values),
        }
    }

    pub(in crate::datafusion) fn for_planning(
        scans: &'a ScanBindings,
        values: &'a RuntimeValueLayout,
    ) -> Self {
        Self {
            scans,
            expressions: DataFusionExpressionCompiler::for_planning(scans, values),
            values: None,
        }
    }

    pub(in crate::datafusion) fn compile(
        &self,
        node: &QueryNode,
    ) -> Result<LogicalPlan, DataFusionPlanError> {
        self.node(node)?.build().map_err(Into::into)
    }

    fn node(
        &self,
        node: &QueryNode,
    ) -> Result<LogicalPlanBuilder, DataFusionPlanError> {
        let expressions = &self.expressions;
        match node {
            QueryNode::Scan(scan) => {
                let binding = self.scans.get(scan.scan()).ok_or(
                    DataFusionPlanError::MissingScan {
                        index: scan.scan().index(),
                    },
                )?;
                let predicate = scan
                    .filter()
                    .map(|predicate| expressions.compile(predicate))
                    .transpose()?;
                let input = binding.plan()?;
                match predicate {
                    Some(predicate) => input.filter(predicate).map_err(Into::into),
                    None => Ok(input),
                }
            }
            QueryNode::Aggregate(aggregate) => {
                let input = self.node(aggregate.input())?;
                let groups = aggregate
                    .groups()
                    .iter()
                    .map(|group| {
                        Ok(expressions.compile(group.expression())?.alias(
                            DataFusionExpressionCompiler::output_name(group.output()),
                        ))
                    })
                    .collect::<Result<Vec<_>, DataFusionPlanError>>()?;
                let aggregates = aggregate
                    .aggregates()
                    .iter()
                    .map(|aggregate| Self::aggregate(aggregate, expressions))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut output = input
                    .with_options(
                        LogicalPlanBuilderOptions::new()
                            .with_add_implicit_group_by_exprs(true),
                    )
                    .aggregate(groups, aggregates)?;
                // Finalize only SUM(real), after native partial/final
                // aggregation and before any parent HAVING or expression.
                for call in aggregate.aggregates() {
                    if let Some(adapter) = Self::aggregate_result_adapter(call) {
                        let name =
                            DataFusionExpressionCompiler::output_name(call.output());
                        let value = Expr::Column(Column::from_name(&name));
                        let replacement = adapter.call(vec![value]).alias(&name);
                        let projection = output
                            .schema()
                            .columns()
                            .into_iter()
                            .map(|column| {
                                if column.name == name {
                                    replacement.clone()
                                } else {
                                    Expr::Column(column)
                                }
                            })
                            .collect::<Vec<_>>();
                        output = output.project(projection)?;
                    }
                }
                Ok(output)
            }
            QueryNode::Join(join) => {
                let left = self.node(join.left())?;
                let right = self.node(join.right())?.build()?;
                let mut on = Vec::with_capacity(
                    join.keys().len() + usize::from(join.on_filter().is_some()),
                );
                for key in join.keys() {
                    on.push(
                        expressions
                            .compile(&ExecutionExpr::Column(key.left()))?
                            .eq(expressions
                                .compile(&ExecutionExpr::Column(key.right()))?),
                    );
                }
                if let Some(filter) = join.on_filter() {
                    on.push(expressions.compile(filter)?);
                }
                // DataFusion rejects a non-inner join with no condition.
                // EXISTS/NOT EXISTS are exactly keyless semi/anti joins;
                // a constant-true filter preserves their empty/non-empty
                // semantics and selects the nested-loop implementation.
                if on.is_empty()
                    && matches!(
                        join.join_type(),
                        JoinType::LeftSemi | JoinType::LeftAnti
                    )
                {
                    on.push(lit(true));
                }
                if join.null_aware() {
                    return Self::null_aware_anti_join(left, right, &on);
                }
                if join.mark_filter().is_some_and(|filter| filter.anti()) {
                    let [equality] = on.as_slice() else {
                        return Err(DataFusionError::Plan(
                            "anti Mark join requires exactly one key".to_owned(),
                        )
                        .into());
                    };
                    // LeftMark itself emits a non-null boolean. Extending
                    // its match condition with an inner-key NULL match is
                    // what preserves NOT IN's UNKNOWN result for non-null
                    // outer keys; the immediate filter below then inverts
                    // that closed mark while retaining outer NULL rows.
                    let inner_null = expressions
                        .compile(&ExecutionExpr::Column(join.keys()[0].right()))?
                        .is_null();
                    on = vec![equality.clone().or(inner_null)];
                }
                let left_projection = join.mark_filter().map(|_| {
                    left.schema()
                        .columns()
                        .into_iter()
                        .map(Expr::Column)
                        .collect::<Vec<_>>()
                });
                // DataFusion 55 lowers join_on through join_detailed with
                // NullEqualsNothing. That matches ordinary PostgreSQL
                // equality and filter-only semi/anti join semantics.
                let joined =
                    left.join_on(right, Self::join_type(join.join_type()), on)?;
                if let Some(filter) = join.mark_filter() {
                    // DataFusion appends the synthetic mark field to the
                    // LeftMark schema. Resolve that exact qualified field
                    // instead of looking up the public name "mark", which
                    // can also be a real left-table column.
                    let mark_column = joined
                        .schema()
                        .columns()
                        .last()
                        .cloned()
                        .expect("LeftMark always appends its mark column");
                    let mark = Expr::Column(mark_column).eq(lit(!filter.anti()));
                    let outer_null = expressions
                        .compile(&ExecutionExpr::Column(filter.null_test()))?
                        .is_null();
                    joined
                        .filter(mark.or(outer_null))?
                        .project(
                            left_projection
                                .expect("mark filter captured its left projection"),
                        )
                        .map_err(Into::into)
                } else {
                    Ok(joined)
                }
            }
            QueryNode::Distinct(distinct) => {
                let input = self.node(distinct.input())?;
                let keys = distinct
                    .keys()
                    .iter()
                    .map(|key| {
                        Ok(expressions.compile(key.expression())?.alias(
                            DataFusionExpressionCompiler::output_name(key.output()),
                        ))
                    })
                    .collect::<Result<Vec<_>, DataFusionPlanError>>()?;
                input
                    .with_options(
                        LogicalPlanBuilderOptions::new()
                            .with_add_implicit_group_by_exprs(true),
                    )
                    .aggregate(keys, Vec::<Expr>::new())
                    .map_err(Into::into)
            }
            QueryNode::Filter(filter) => {
                let input = self.node(filter.input())?;
                input
                    .filter(expressions.compile(filter.predicate())?)
                    .map_err(Into::into)
            }
            QueryNode::Project(project) => {
                let input = self.node(project.input())?;
                let expressions = project
                    .expressions()
                    .iter()
                    .enumerate()
                    .map(|(position, expression)| {
                        Ok(expressions
                            .compile(expression.expression())?
                            .alias(Self::result_name(position)))
                    })
                    .collect::<Result<Vec<_>, DataFusionPlanError>>()?;
                input.project(expressions).map_err(Into::into)
            }
            QueryNode::Sort(sort) => {
                let input = self.node(sort.input())?;
                let QueryNode::Project(project) = sort.input() else {
                    unreachable!("validated Sort input is the output Project")
                };
                let keys = sort
                    .keys()
                    .iter()
                    .map(|key| {
                        let position = project
                            .expressions()
                            .iter()
                            .position(|expression| {
                                expression.output() == key.output()
                            })
                            .expect("validated Sort key names one Project output");
                        let expression = Expr::Column(Column::from_name(
                            Self::result_name(position),
                        ));
                        expression.sort(
                            matches!(key.direction(), SortDirection::Ascending),
                            key.nulls_first(),
                        )
                    })
                    .collect::<Vec<_>>();
                input.sort(keys).map_err(Into::into)
            }
            QueryNode::Limit(limit) => {
                let input = self.node(limit.input())?;
                match self.values {
                    Some(values) => {
                        let window = LimitWindow::bind(limit, values)?;
                        input.limit(window.offset, window.count).map_err(Into::into)
                    }
                    None => {
                        let offset = limit
                            .offset()
                            .map(|id| expressions.compile(&ExecutionExpr::Value(id)))
                            .transpose()?;
                        let count = limit
                            .count()
                            .map(|id| expressions.compile(&ExecutionExpr::Value(id)))
                            .transpose()?;
                        input.limit_by_expr(offset, count).map_err(Into::into)
                    }
                }
            }
        }
    }

    const fn join_type(join_type: JoinType) -> DataFusionJoinType {
        match join_type {
            JoinType::Inner => DataFusionJoinType::Inner,
            JoinType::Left => DataFusionJoinType::Left,
            JoinType::Right => DataFusionJoinType::Right,
            JoinType::Full => DataFusionJoinType::Full,
            JoinType::LeftSemi => DataFusionJoinType::LeftSemi,
            JoinType::LeftAnti => DataFusionJoinType::LeftAnti,
            JoinType::LeftMark => DataFusionJoinType::LeftMark,
        }
    }

    /// DataFusion's NOT IN contract requires a filter-based, single-key
    /// LeftAnti join with the logical plan's null-aware flag enabled.
    fn null_aware_anti_join(
        left: LogicalPlanBuilder,
        right: LogicalPlan,
        on: &[Expr],
    ) -> Result<LogicalPlanBuilder, DataFusionPlanError> {
        let [predicate] = on else {
            return Err(DataFusionError::Plan(
                "null-aware anti join requires exactly one key".to_owned(),
            )
            .into());
        };
        left.join_detailed_with_options(
            right,
            DataFusionJoinType::LeftAnti,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            Some(predicate.clone()),
            NullEquality::NullEqualsNothing,
            true,
        )
        .map_err(Into::into)
    }

    /// Physical result names belong to PostgreSQL slot positions, not semantic
    /// output identities. A SELECT list may legally project the same group or
    /// aggregate output more than once, while DataFusion requires projection
    /// field names to be unique.
    fn result_name(position: usize) -> String {
        format!("__lagodb_result_{position}")
    }
}
