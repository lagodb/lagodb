//! Compilation of validated query semantics into DataFusion physical plans.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::expr_fn::{
    array_agg, avg, bool_and, bool_or, count, max, min, stddev, stddev_pop, sum,
    var_pop, var_sample,
};
use datafusion::functions_aggregate::string_agg::string_agg;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::{Expr, col, lit};
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use lagodb_core::expr::RuntimeValue;
use pgrx::pg_sys;

use crate::plan::{AggCall, AggregateKind, QueryFragment, QueryNode, SortDirection};

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
    #[error("STRING_AGG delimiter is not valid UTF-8")]
    InvalidStringAggDelimiter,
    #[error("PostgreSQL expression fallback has an unsupported type contract")]
    UnsupportedPostgresExpression,
}

pub(super) struct DataFusionPlanCompiler<'session> {
    session: &'session SessionContext,
    postgres: PgExprRuntime,
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
        let frame = self.compile_node(fragment.root(), &scans, values).await?;
        let physical_plan = frame.create_physical_plan().await?;
        Ok(CompiledPhysicalPlan::new(physical_plan))
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
                    let input = binding.frame(self.session)?;
                    match scan.filter() {
                        Some(predicate) => input
                            .filter(expressions.compile(predicate)?)
                            .map_err(Into::into),
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
                        .outputs()
                        .iter()
                        .enumerate()
                        .map(|(position, output)| {
                            col(DataFusionExpressionCompiler::output_name(*output))
                                .alias(Self::result_name(position))
                        })
                        .collect::<Vec<_>>();
                    input.select(expressions).map_err(Into::into)
                }
            }
        }
        .boxed_local()
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
            // accepted compatibility boundary is documented in the S3 capability
            // contract.
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
        // DISTINCT keys use their bit representation (-0/+0 and NaN therefore
        // differ from PostgreSQL); AVG/variance coerce integer inputs to
        // Float64 before DISTINCT evaluation; collated text uses Arrow byte
        // equality; aggregate-local text ORDER BY uses Arrow byte ordering;
        // and growing distinct state is reserved after accumulator update.
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
