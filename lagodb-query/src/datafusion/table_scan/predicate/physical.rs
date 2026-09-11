//! Physical runtime-filter lowering for one projected scan node.

use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::common::ScalarValue;
use datafusion::functions::math::nans::IsNanFunc;
use datafusion::functions::string::starts_with::StartsWithFunc;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};
use datafusion::physical_expr::{PhysicalExpr, conjunction_opt};
use lagodb_core::runtime_api::{
    RuntimePredicateScalar, RuntimePredicateValue, RuntimePruningPredicate,
};

use super::DataFusionPredicateScalar;

pub(in crate::datafusion::table_scan) struct PhysicalPredicateAdapter<'a> {
    source_position_by_output: &'a [usize],
    output_schema: &'a Schema,
}

/// One lowering decision for a runtime expression. `exact_residual` contains
/// precisely the expression subtree not represented in `provider_predicate`.
pub(in crate::datafusion::table_scan) struct PhysicalPredicatePlan<'a> {
    provider_predicate: Option<RuntimePruningPredicate<'a>>,
    exact_residual: Option<Arc<dyn PhysicalExpr>>,
}

impl<'a> PhysicalPredicatePlan<'a> {
    pub(in crate::datafusion::table_scan) fn into_parts(
        self,
    ) -> (
        Option<RuntimePruningPredicate<'a>>,
        Option<Arc<dyn PhysicalExpr>>,
    ) {
        (self.provider_predicate, self.exact_residual)
    }
}

impl<'a> PhysicalPredicateAdapter<'a> {
    pub(in crate::datafusion::table_scan) const fn new(
        source_position_by_output: &'a [usize],
        output_schema: &'a Schema,
    ) -> Self {
        Self {
            source_position_by_output,
            output_schema,
        }
    }

    pub(in crate::datafusion::table_scan) fn plan<'expr>(
        &self,
        expression: &'expr Arc<dyn PhysicalExpr>,
    ) -> PhysicalPredicatePlan<'expr> {
        if let Some(binary) = expression.downcast_ref::<BinaryExpr>() {
            return match binary.op() {
                Operator::And => self.and(binary),
                Operator::Or => self.or(expression, binary),
                operator => Self::leaf(
                    expression,
                    self.comparison(binary.left(), *operator, binary.right()),
                ),
            };
        }
        Self::leaf(expression, self.compile_leaf(expression))
    }

    fn compile_leaf<'expr>(
        &self,
        expression: &'expr Arc<dyn PhysicalExpr>,
    ) -> Option<RuntimePruningPredicate<'expr>> {
        if let Some(literal) = expression.downcast_ref::<Literal>() {
            return match literal.value() {
                ScalarValue::Boolean(Some(true)) => {
                    Some(RuntimePruningPredicate::AlwaysTrue)
                }
                ScalarValue::Boolean(Some(false) | None) | ScalarValue::Null => {
                    Some(RuntimePruningPredicate::AlwaysFalse)
                }
                _ => None,
            };
        }
        if let Some(is_null) = expression.downcast_ref::<IsNullExpr>() {
            return self
                .column(is_null.arg())
                .map(RuntimePredicateScalar::Column)
                .map(RuntimePruningPredicate::IsNull);
        }
        if let Some(is_not_null) = expression.downcast_ref::<IsNotNullExpr>() {
            return self
                .column(is_not_null.arg())
                .map(RuntimePredicateScalar::Column)
                .map(RuntimePruningPredicate::IsNotNull);
        }
        if let Some(not) = expression.downcast_ref::<NotExpr>() {
            return self.negated_unary(not.arg());
        }
        if let Some(function) = expression
            .downcast_ref::<ScalarFunctionExpr>()
            .filter(|function| function.fun().inner().is::<IsNanFunc>())
        {
            return self.nan_test(function.args(), false);
        }
        expression
            .downcast_ref::<ScalarFunctionExpr>()
            .filter(|function| function.fun().inner().is::<StartsWithFunc>())
            .and_then(|function| self.starts_with(function.args()))
    }

    fn nan_test<'expr>(
        &self,
        arguments: &'expr [Arc<dyn PhysicalExpr>],
        is_not_nan: bool,
    ) -> Option<RuntimePruningPredicate<'expr>> {
        let [value] = arguments else {
            return None;
        };
        let value = RuntimePredicateScalar::Column(self.column(value)?);
        Some(if is_not_nan {
            RuntimePruningPredicate::IsNotNan(value)
        } else {
            RuntimePruningPredicate::IsNan(value)
        })
    }

    fn negated_unary<'expr>(
        &self,
        expression: &'expr Arc<dyn PhysicalExpr>,
    ) -> Option<RuntimePruningPredicate<'expr>> {
        let function = expression.downcast_ref::<ScalarFunctionExpr>()?;
        if function.fun().inner().is::<IsNanFunc>() {
            self.nan_test(function.args(), true)
        } else if function.fun().inner().is::<StartsWithFunc>() {
            Some(RuntimePruningPredicate::Not(Box::new(
                self.starts_with(function.args())?,
            )))
        } else {
            None
        }
    }

    fn leaf<'expr>(
        expression: &'expr Arc<dyn PhysicalExpr>,
        provider_predicate: Option<RuntimePruningPredicate<'expr>>,
    ) -> PhysicalPredicatePlan<'expr> {
        let exact_residual =
            provider_predicate.is_none().then(|| Arc::clone(expression));
        PhysicalPredicatePlan {
            provider_predicate,
            exact_residual,
        }
    }

    fn and<'expr>(&self, binary: &'expr BinaryExpr) -> PhysicalPredicatePlan<'expr> {
        let (left_provider, left_residual) = self.plan(binary.left()).into_parts();
        let (right_provider, right_residual) = self.plan(binary.right()).into_parts();
        PhysicalPredicatePlan {
            provider_predicate: Self::combine_provider(left_provider, right_provider),
            exact_residual: conjunction_opt(
                left_residual.into_iter().chain(right_residual),
            ),
        }
    }

    fn or<'expr>(
        &self,
        expression: &'expr Arc<dyn PhysicalExpr>,
        binary: &'expr BinaryExpr,
    ) -> PhysicalPredicatePlan<'expr> {
        let (left_provider, left_residual) = self.plan(binary.left()).into_parts();
        let (right_provider, right_residual) = self.plan(binary.right()).into_parts();
        let provider_predicate = match (left_provider, right_provider) {
            (Some(left), Some(right)) => Some(left.or(right)),
            _ => None,
        };
        let exact_residual = if provider_predicate.is_some()
            && left_residual.is_none()
            && right_residual.is_none()
        {
            None
        } else {
            Some(Arc::clone(expression))
        };
        PhysicalPredicatePlan {
            provider_predicate,
            exact_residual,
        }
    }

    fn combine_provider<'expr>(
        left: Option<RuntimePruningPredicate<'expr>>,
        right: Option<RuntimePruningPredicate<'expr>>,
    ) -> Option<RuntimePruningPredicate<'expr>> {
        match (left, right) {
            (Some(left), Some(right)) => Some(left.and(right)),
            (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
            (None, None) => None,
        }
    }

    fn comparison<'expr>(
        &self,
        left: &'expr Arc<dyn PhysicalExpr>,
        operator: Operator,
        right: &'expr Arc<dyn PhysicalExpr>,
    ) -> Option<RuntimePruningPredicate<'expr>> {
        if matches!(operator, Operator::Eq | Operator::Gt)
            && let (Some(left_column), Some(right_column)) =
                (self.column(left), self.column(right))
            && left_column == right_column
        {
            let column = RuntimePredicateScalar::Column(left_column);
            return Some(if operator == Operator::Eq {
                RuntimePruningPredicate::StrictTrue(column)
            } else {
                RuntimePruningPredicate::StrictFalse(column)
            });
        }
        if let (Some(column), Some(value)) =
            (self.comparison_column(left), self.value(right))
        {
            return Some(RuntimePruningPredicate::Comparison {
                operator: DataFusionPredicateScalar::comparison_operator(operator)?,
                left: column,
                right: RuntimePredicateScalar::Value(value),
            });
        }
        let (Some(value), Some(column)) =
            (self.value(left), self.comparison_column(right))
        else {
            return None;
        };
        Some(RuntimePruningPredicate::Comparison {
            operator: DataFusionPredicateScalar::comparison_operator(
                DataFusionPredicateScalar::reverse_operator(operator),
            )?,
            left: column,
            right: RuntimePredicateScalar::Value(value),
        })
    }

    fn starts_with<'expr>(
        &self,
        arguments: &'expr [Arc<dyn PhysicalExpr>],
    ) -> Option<RuntimePruningPredicate<'expr>> {
        let [value, prefix] = arguments else {
            return None;
        };
        let column = self.column(value)?;
        let prefix = self.value(prefix)?;
        if !matches!(prefix, RuntimePredicateValue::String(_)) {
            return None;
        }
        Some(RuntimePruningPredicate::StartsWith {
            value: RuntimePredicateScalar::Column(column),
            prefix: RuntimePredicateScalar::Value(prefix),
        })
    }

    fn column(&self, expression: &Arc<dyn PhysicalExpr>) -> Option<usize> {
        let output_position = expression.downcast_ref::<Column>()?.index();
        self.source_position_by_output.get(output_position).copied()
    }

    fn comparison_column(
        &self,
        expression: &Arc<dyn PhysicalExpr>,
    ) -> Option<RuntimePredicateScalar<'static>> {
        if let Some(column) = self.column(expression) {
            return Some(RuntimePredicateScalar::Column(column));
        }
        let cast = expression.downcast_ref::<CastExpr>()?;
        let output_position = cast.expr().downcast_ref::<Column>()?.index();
        let column = self
            .source_position_by_output
            .get(output_position)
            .copied()?;
        let source = self.output_schema.field(output_position).data_type();
        let target =
            DataFusionPredicateScalar::integer_widening(source, cast.cast_type())?;
        Some(RuntimePredicateScalar::WidenedIntegerColumn { column, target })
    }

    fn value<'expr>(
        &self,
        expression: &'expr Arc<dyn PhysicalExpr>,
    ) -> Option<RuntimePredicateValue<'expr>> {
        let value = expression.downcast_ref::<Literal>()?.value();
        DataFusionPredicateScalar::value(value)
    }
}
