//! Compilation of validated query semantics into DataFusion physical plans.

use std::sync::Arc;

use datafusion::common::{Column, DataFusionError, NullEquality};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::expr_fn::{
    array_agg, avg, bool_and, bool_or, count, max, min, stddev, stddev_pop, sum,
    var_pop, var_sample,
};
use datafusion::functions_aggregate::string_agg::string_agg;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::{
    Expr, JoinType as DataFusionJoinType, LogicalPlanBuilder, lit,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use lagodb_core::expr::{RuntimeValue, RuntimeValueId};
use pgrx::{FromDatum, pg_sys};

use crate::plan::{
    AggCall, AggregateKind, ExecutionExpr, JoinType, LimitNode, QueryFragment,
    QueryNode, SortDirection,
};

use super::expression_compiler::DataFusionExpressionCompiler;
use super::numeric_aggregate;
use super::physical_plan::CompiledPhysicalPlan;
use super::postgres_eval::PgExprRuntime;
use super::scan_binding::ScanBindings;
use super::table_scan::ExternalTableProvider;

#[derive(Debug, thiserror::Error)]
pub(super) enum DataFusionPlanError {
    #[error("DataFusion plan compilation failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("query fragment references missing table scan {index}")]
    MissingScan { index: usize },
    #[error(
        "query expression references unprojected attribute {attno} in scan {scan}"
    )]
    MissingColumn {
        scan: usize,
        attno: pg_sys::AttrNumber,
    },
    #[error(
        "query expression uses unsupported PostgreSQL comparison operator {oid:?}"
    )]
    UnsupportedOperator { oid: pg_sys::Oid },
    #[error("query runtime value {index} is missing")]
    MissingRuntimeValue { index: usize },
    #[error("query runtime value has unsupported PostgreSQL type {oid:?}")]
    UnsupportedRuntimeType { oid: pg_sys::Oid },
    #[error("query runtime value could not be decoded as PostgreSQL type {oid:?}")]
    InvalidRuntimeValue { oid: pg_sys::Oid },
    #[error("LIMIT/OFFSET value is outside the execution range")]
    InvalidLimit,
    #[error("STRING_AGG delimiter is not valid UTF-8")]
    InvalidStringAggDelimiter,
    #[error("PostgreSQL expression fallback has an unsupported type contract")]
    UnsupportedPostgresExpression,
}

pub(super) struct DataFusionPlanCompiler<'session> {
    session: &'session SessionContext,
    postgres: PgExprRuntime,
}

struct LimitWindow {
    offset: usize,
    count: Option<usize>,
}

impl LimitWindow {
    fn bind(
        limit: &LimitNode,
        values: &[RuntimeValue],
    ) -> Result<Self, DataFusionPlanError> {
        Ok(Self {
            offset: Self::value(limit.offset(), values)?.unwrap_or(0),
            count: Self::value(limit.count(), values)?,
        })
    }

    fn value(
        id: Option<RuntimeValueId>,
        values: &[RuntimeValue],
    ) -> Result<Option<usize>, DataFusionPlanError> {
        let Some(id) = id else {
            return Ok(None);
        };
        let value = values
            .get(id.index())
            .copied()
            .ok_or(DataFusionPlanError::MissingRuntimeValue { index: id.index() })?;
        if value.is_null() {
            return Ok(None);
        }
        let value = unsafe { i64::from_datum(value.datum(), false) }
            .ok_or(DataFusionPlanError::InvalidLimit)?;
        usize::try_from(value)
            .map(Some)
            .map_err(|_| DataFusionPlanError::InvalidLimit)
    }

    fn supports_bounded_top_k(&self) -> bool {
        self.count.is_none_or(|count| {
            self.offset
                .checked_add(count)
                .is_some_and(|top_k| top_k <= i64::MAX as usize)
        })
    }
}

impl<'session> DataFusionPlanCompiler<'session> {
    pub(super) const fn new(
        session: &'session SessionContext,
        postgres: PgExprRuntime,
    ) -> Self {
        Self { session, postgres }
    }

    pub(super) async fn compile(
        &self,
        fragment: &QueryFragment,
        scans: &[Arc<ExternalTableProvider>],
        values: &[RuntimeValue],
    ) -> Result<CompiledPhysicalPlan, DataFusionPlanError> {
        let scans = ScanBindings::new(scans);
        if let QueryNode::Limit(limit) = fragment.root() {
            let window = LimitWindow::bind(limit, values)?;
            if !window.supports_bounded_top_k() {
                let frame = self.compile_node(limit.input(), &scans, values).await?;
                let input = frame.create_physical_plan().await?;
                let exact_limit = Arc::new(GlobalLimitExec::new(
                    input,
                    window.offset,
                    window.count,
                ));
                return Ok(CompiledPhysicalPlan::try_new(exact_limit)?);
            }
        }
        let frame = self.compile_node(fragment.root(), &scans, values).await?;
        let physical_plan = frame.create_physical_plan().await?;
        Ok(CompiledPhysicalPlan::try_new(physical_plan)?)
    }

    fn compile_node<'a>(
        &'a self,
        node: &'a QueryNode,
        scans: &'a ScanBindings,
        values: &'a [RuntimeValue],
    ) -> LocalBoxFuture<'a, Result<DataFrame, DataFusionPlanError>> {
        async move {
            let expressions =
                DataFusionExpressionCompiler::new(scans, values, self.postgres);
            match node {
                QueryNode::Scan(scan) => {
                    let binding = scans.get(scan.scan()).ok_or(
                        DataFusionPlanError::MissingScan {
                            index: scan.scan().index(),
                        },
                    )?;
                    let predicate = scan
                        .filter()
                        .map(|predicate| expressions.compile(predicate))
                        .transpose()?;
                    let input = binding.frame(self.session)?;
                    match predicate {
                        Some(predicate) => {
                            input.filter(predicate).map_err(Into::into)
                        }
                        None => Ok(input),
                    }
                }
                QueryNode::Aggregate(aggregate) => {
                    let input =
                        self.compile_node(aggregate.input(), scans, values).await?;
                    let groups = aggregate
                        .groups()
                        .iter()
                        .map(|group| {
                            Ok(expressions.compile(group.expression())?.alias(
                                DataFusionExpressionCompiler::output_name(
                                    group.output(),
                                ),
                            ))
                        })
                        .collect::<Result<Vec<_>, DataFusionPlanError>>()?;
                    let aggregates = aggregate
                        .aggregates()
                        .iter()
                        .map(|aggregate| Self::aggregate(aggregate, &expressions))
                        .collect::<Result<Vec<_>, _>>()?;
                    input.aggregate(groups, aggregates).map_err(Into::into)
                }
                QueryNode::Join(join) => {
                    let left = self.compile_node(join.left(), scans, values).await?;
                    let right =
                        self.compile_node(join.right(), scans, values).await?;
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
                            .select(
                                left_projection.expect(
                                    "mark filter captured its left projection",
                                ),
                            )
                            .map_err(Into::into)
                    } else {
                        Ok(joined)
                    }
                }
                QueryNode::Distinct(distinct) => {
                    let input =
                        self.compile_node(distinct.input(), scans, values).await?;
                    let keys = distinct
                        .keys()
                        .iter()
                        .map(|key| {
                            Ok(expressions.compile(key.expression())?.alias(
                                DataFusionExpressionCompiler::output_name(
                                    key.output(),
                                ),
                            ))
                        })
                        .collect::<Result<Vec<_>, DataFusionPlanError>>()?;
                    input.aggregate(keys, Vec::new()).map_err(Into::into)
                }
                QueryNode::Filter(filter) => {
                    let input =
                        self.compile_node(filter.input(), scans, values).await?;
                    input
                        .filter(expressions.compile(filter.predicate())?)
                        .map_err(Into::into)
                }
                QueryNode::Project(project) => {
                    let input =
                        self.compile_node(project.input(), scans, values).await?;
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
                    input.select(expressions).map_err(Into::into)
                }
                QueryNode::Sort(sort) => {
                    let input =
                        self.compile_node(sort.input(), scans, values).await?;
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
                                .expect(
                                    "validated Sort key names one Project output",
                                );
                            let expression = Expr::Column(Column::from_name(
                                Self::result_name(position),
                            ));
                            expression.sort(
                                matches!(key.direction(), SortDirection::Ascending),
                                key.nulls_first(),
                            )
                        })
                        .collect();
                    input.sort(keys).map_err(Into::into)
                }
                QueryNode::Limit(limit) => {
                    let input =
                        self.compile_node(limit.input(), scans, values).await?;
                    let offset =
                        LimitWindow::value(limit.offset(), values)?.unwrap_or(0);
                    let count = LimitWindow::value(limit.count(), values)?;
                    input.limit(offset, count).map_err(Into::into)
                }
            }
        }
        .boxed_local()
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
        left: DataFrame,
        right: DataFrame,
        on: &[Expr],
    ) -> Result<DataFrame, DataFusionPlanError> {
        let [predicate] = on else {
            return Err(DataFusionError::Plan(
                "null-aware anti join requires exactly one key".to_owned(),
            )
            .into());
        };
        let (session, left_plan) = left.into_parts();
        let right_plan = right.into_unoptimized_plan();
        let plan = LogicalPlanBuilder::from(left_plan)
            .join_detailed_with_options(
                right_plan,
                DataFusionJoinType::LeftAnti,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                Some(predicate.clone()),
                NullEquality::NullEqualsNothing,
                true,
            )?
            .build()?;
        Ok(DataFrame::new(session, plan))
    }

    fn aggregate(
        aggregate: &AggCall,
        expressions: &DataFusionExpressionCompiler<'_>,
    ) -> Result<Expr, DataFusionPlanError> {
        let argument = aggregate
            .argument()
            .map(|argument| expressions.compile(argument))
            .transpose()?;
        let expression = match (aggregate.kind(), argument) {
            (AggregateKind::Count, None) => count(lit(1_i64)),
            (AggregateKind::Count, Some(argument)) => count(argument),
            // Float MIN/MAX delegates to DataFusion. Its native partial ordering
            // does not implement PostgreSQL's rule that NaN is greater than every
            // non-NaN value, so a NaN result can depend on input order. This
            // execution-semantic difference is explicitly admitted by the
            // current aggregate capability.
            (AggregateKind::Min, Some(argument)) => min(argument),
            (AggregateKind::Max, Some(argument)) => max(argument),
            // Keep integer-input SUM/AVG on DataFusion's optimized native
            // AggregateExec path. See AggregateKind's semantic gate for the
            // intentional PostgreSQL precision differences this inherits.
            (AggregateKind::Sum, Some(argument)) => sum(argument),
            (AggregateKind::Average, Some(argument)) => avg(argument),
            (AggregateKind::NumericSum, Some(argument)) => {
                numeric_aggregate::sum().call(vec![argument])
            }
            (AggregateKind::NumericAverage, Some(argument)) => {
                numeric_aggregate::avg().call(vec![argument])
            }
            // Native variance/stddev coerces integer input to Float64 before
            // accumulation, then the output codec constructs the declared PG
            // NUMERIC result. Values beyond exact Float64 integer precision do
            // not retain PostgreSQL's arbitrary-precision transition semantics.
            (AggregateKind::VarianceSample, Some(argument)) => var_sample(argument),
            (AggregateKind::VariancePopulation, Some(argument)) => var_pop(argument),
            (AggregateKind::StddevSample, Some(argument)) => stddev(argument),
            (AggregateKind::StddevPopulation, Some(argument)) => stddev_pop(argument),
            (AggregateKind::BoolAnd, Some(argument)) => bool_and(argument),
            (AggregateKind::BoolOr, Some(argument)) => bool_or(argument),
            (AggregateKind::ArrayAgg, Some(argument)) => array_agg(argument),
            (AggregateKind::StringAgg, Some(argument)) => {
                let delimiter = aggregate
                    .arguments()
                    .delimiter()
                    .expect("validated STRING_AGG has a delimiter")
                    .to_str()
                    .map_err(|_| DataFusionPlanError::InvalidStringAggDelimiter)?;
                string_agg(argument, lit(delimiter.to_owned()))
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "invalid aggregate semantics".to_owned(),
                )
                .into());
            }
        };
        let expression =
            Self::apply_aggregate_modifiers(expression, aggregate, expressions)?;
        Ok(expression.alias(DataFusionExpressionCompiler::output_name(
            aggregate.output(),
        )))
    }

    fn apply_aggregate_modifiers(
        expression: Expr,
        aggregate: &AggCall,
        expressions: &DataFusionExpressionCompiler<'_>,
    ) -> Result<Expr, DataFusionPlanError> {
        let Expr::AggregateFunction(AggregateFunction { mut params, func }) =
            expression
        else {
            return Err(DataFusionError::Plan(
                "aggregate lowering produced a non-aggregate expression".to_owned(),
            )
            .into());
        };
        // The first aggregate-DISTINCT capability intentionally uses
        // DataFusion's native state semantics. In DataFusion 55, floating-point
        // DISTINCT keys use their bit representation: -0/+0 and distinct NaN
        // payloads can remain separate even though PostgreSQL considers each
        // pair equal. AVG/variance coerce integer inputs to Float64 before
        // DISTINCT evaluation, so distinct int8 values outside Float64's exact
        // integer domain (above 2^53 in magnitude) can collapse to one key.
        // Collated text uses Arrow byte equality, aggregate-local text ORDER BY
        // uses Arrow byte ordering, and growing distinct state is reserved
        // after accumulator update.
        // These limitations are recorded at the capability boundary and do
        // not add per-datum normalization or reservation checks to the hot
        // path.
        params.distinct = aggregate.uses_distinct_state();
        params.filter = aggregate
            .filter()
            .map(|filter| expressions.compile(filter))
            .transpose()?
            .map(Box::new);
        if matches!(
            aggregate.kind(),
            AggregateKind::ArrayAgg | AggregateKind::StringAgg
        ) {
            params.order_by = aggregate
                .order_by()
                .iter()
                .map(|order| {
                    Ok(Sort::new(
                        expressions.compile(order.expression())?,
                        matches!(order.direction(), SortDirection::Ascending),
                        order.nulls_first(),
                    ))
                })
                .collect::<Result<Vec<_>, DataFusionPlanError>>()?;
        }
        Ok(Expr::AggregateFunction(AggregateFunction { func, params }))
    }

    /// Physical result names belong to PostgreSQL slot positions, not semantic
    /// output identities. A SELECT list may legally project the same group or
    /// aggregate output more than once, while DataFusion requires projection
    /// field names to be unique.
    fn result_name(position: usize) -> String {
        format!("__lagodb_result_{position}")
    }
}
