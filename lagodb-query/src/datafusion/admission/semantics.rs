//! PG semantics not established by DataFusion's type coercion alone.
//! These explicit restrictions supplement the IR capabilities; they do not
//! change the provider's supported types or define every operator's semantics.

use arrow_schema::{DataType, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator};

use super::AdmissionDeclineReason;

pub(super) struct RepresentationSemantics;

impl RepresentationSemantics {
    pub(super) fn check_plan(
        plan: &LogicalPlan,
    ) -> Result<(), AdmissionDeclineReason> {
        let inputs = plan.inputs();
        for input in &inputs {
            Self::check_plan(input)?;
        }
        let schema = merge_schema(&inputs);
        // Query DISTINCT lowers to Aggregate grouping keys. Check these at the
        // relational boundary; a bare column expression performs no comparison.
        if let LogicalPlan::Aggregate(aggregate) = plan {
            for key in &aggregate.group_expr {
                Self::check_equality_key(key, &schema)?;
            }
        }
        let mut result = Ok(());
        plan.apply_expressions(|expression| {
            result = Self::check(expression, &schema);
            Ok(if result.is_ok() {
                TreeNodeRecursion::Continue
            } else {
                TreeNodeRecursion::Stop
            })
        })
        .map_err(AdmissionDeclineReason::ExpressionType)?;
        result
    }

    fn check_equality_key(
        expression: &Expr,
        schema: &DFSchema,
    ) -> Result<(), AdmissionDeclineReason> {
        let data_type = expression
            .get_type(schema)
            .map_err(AdmissionDeclineReason::ExpressionType)?;
        if Self::nanoseconds(&data_type) {
            Err(AdmissionDeclineReason::NanosecondComparison)
        } else {
            Ok(())
        }
    }

    fn check(
        expression: &Expr,
        schema: &DFSchema,
    ) -> Result<(), AdmissionDeclineReason> {
        let mut decline = None;
        let visited = expression.apply(|expression| {
            decline = match expression {
                Expr::BinaryExpr(binary) if matches!(binary.op,
                    Operator::Eq | Operator::NotEq | Operator::Lt
                    | Operator::LtEq | Operator::Gt | Operator::GtEq
                ) => {
                    // PG decoding floors nanoseconds to microseconds, so
                    // comparing the source values does not implement PG
                    // equality or ordering, even when DataFusion accepts it.
                    (Self::nanoseconds(&binary.left.get_type(schema)?)
                        || Self::nanoseconds(&binary.right.get_type(schema)?))
                        .then_some(AdmissionDeclineReason::NanosecondComparison)
                }
                Expr::InList(list) => Self::nanoseconds(&list.expr.get_type(schema)?)
                    .then_some(AdmissionDeclineReason::NanosecondComparison),
                Expr::AggregateFunction(aggregate) if aggregate.params.distinct => {
                    for argument in &aggregate.params.args {
                        if Self::nanoseconds(&argument.get_type(schema)?) {
                            decline = Some(AdmissionDeclineReason::NanosecondComparison);
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    None
                }
                Expr::Cast(cast) => {
                    // CASE/COALESCE can silently promote a PG microsecond
                    // literal to the source's nanoseconds. A finite PG value
                    // need not fit that narrower range. Keep such combinations
                    // in PG instead of adding value checks or per-row casts.
                    (Self::nanoseconds(cast.field.data_type())
                        && matches!(cast.expr.get_type(schema)?,
                            DataType::Timestamp(unit, _) if unit != TimeUnit::Nanosecond))
                        .then_some(AdmissionDeclineReason::NanosecondPromotion)
                }
                _ => None,
            };
            Ok(if decline.is_none() { TreeNodeRecursion::Continue } else { TreeNodeRecursion::Stop })
        });
        visited.map_err(AdmissionDeclineReason::ExpressionType)?;
        decline.map_or(Ok(()), Err)
    }

    fn nanoseconds(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, _))
    }
}
