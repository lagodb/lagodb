//! Shared aggregate expression lowering for admission and execution.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::functions_aggregate::expr_fn::{
    array_agg, avg, bool_and, bool_or, count, max, min, stddev, stddev_pop, sum,
    var_pop, var_sample,
};
use datafusion::functions_aggregate::string_agg::string_agg;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::{Expr, ScalarUDF, lit};
use pgrx::pg_sys;

use crate::datafusion::{numeric_aggregate, real_sum};
use crate::plan::{AggCall, AggregateKind, SortDirection};

use super::DataFusionPlanError;
use super::expression::DataFusionExpressionCompiler;
use super::logical::DataFusionLogicalPlanCompiler;

impl DataFusionLogicalPlanCompiler<'_> {
    /// Adapt a native aggregate result before parent expressions consume it.
    /// Planning applies the same UDF to infer the adapted type; execution calls
    /// it after aggregation, without changing partial or final aggregate state.
    pub(in crate::datafusion) fn aggregate_result_adapter(
        aggregate: &AggCall,
    ) -> Option<Arc<ScalarUDF>> {
        (aggregate.kind() == AggregateKind::Sum
            && aggregate.result_type().type_oid == pg_sys::FLOAT4OID)
            .then(real_sum::result)
    }

    pub(in crate::datafusion) fn aggregate(
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
}
