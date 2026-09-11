//! Logical-plan predicate lowering at the `TableProvider` boundary.

use arrow_schema::Schema;
use datafusion::common::ScalarValue;
use datafusion::functions::math::nans::IsNanFunc;
use datafusion::functions::string::starts_with::StartsWithFunc;
use datafusion::logical_expr::{Expr, Operator};
use lagodb_core::runtime_api::{
    RuntimePredicateScalar, RuntimePredicateValue, RuntimePruningPredicate,
};

use super::DataFusionPredicateScalar;

/// One logical lowering decision. `complete` records whether the provider
/// predicate represents the entire DataFusion expression rather than only a
/// safe superset used for pruning.
pub(in crate::datafusion::table_scan) struct LogicalPredicatePlan<'expr> {
    predicate: RuntimePruningPredicate<'expr>,
    complete: bool,
}

impl<'expr> LogicalPredicatePlan<'expr> {
    fn complete(predicate: RuntimePruningPredicate<'expr>) -> Self {
        Self {
            predicate,
            complete: true,
        }
    }

    pub(in crate::datafusion::table_scan) const fn predicate(
        &self,
    ) -> &RuntimePruningPredicate<'expr> {
        &self.predicate
    }

    pub(in crate::datafusion::table_scan) fn into_predicate(
        self,
    ) -> RuntimePruningPredicate<'expr> {
        self.predicate
    }

    pub(in crate::datafusion::table_scan) const fn is_complete(&self) -> bool {
        self.complete
    }
}

/// Resolve DataFusion columns to statement-bound source positions and lower
/// representable expression shapes. Provider capability is not decided here.
pub(in crate::datafusion::table_scan) struct LogicalPredicateAdapter<'a> {
    source_schema: &'a Schema,
}

impl<'a> LogicalPredicateAdapter<'a> {
    pub(in crate::datafusion::table_scan) const fn new(
        source_schema: &'a Schema,
    ) -> Self {
        Self { source_schema }
    }

    pub(in crate::datafusion::table_scan) fn lower<'expr>(
        &self,
        expression: &'expr Expr,
    ) -> Option<LogicalPredicatePlan<'expr>> {
        match expression {
            Expr::BinaryExpr(binary) => match binary.op {
                Operator::And => Self::combine_and(
                    self.lower(&binary.left),
                    self.lower(&binary.right),
                ),
                Operator::Or => Self::combine_or(
                    self.lower(&binary.left),
                    self.lower(&binary.right),
                ),
                operator => self
                    .comparison(&binary.left, operator, &binary.right)
                    .map(LogicalPredicatePlan::complete),
            },
            Expr::Literal(value, _) => {
                Self::boolean_literal(value).map(LogicalPredicatePlan::complete)
            }
            Expr::IsNull(value) => self
                .column(value)
                .map(RuntimePredicateScalar::Column)
                .map(RuntimePruningPredicate::IsNull)
                .map(LogicalPredicatePlan::complete),
            Expr::IsNotNull(value) => self
                .column(value)
                .map(RuntimePredicateScalar::Column)
                .map(RuntimePruningPredicate::IsNotNull)
                .map(LogicalPredicatePlan::complete),
            Expr::Not(value) => self
                .negated_unary(value)
                .map(LogicalPredicatePlan::complete),
            Expr::ScalarFunction(function)
                if function.func.inner().is::<IsNanFunc>() =>
            {
                self.nan_test(&function.args, false)
                    .map(LogicalPredicatePlan::complete)
            }
            Expr::ScalarFunction(function)
                if function.func.inner().is::<StartsWithFunc>() =>
            {
                self.starts_with(&function.args)
                    .map(LogicalPredicatePlan::complete)
            }
            _ => None,
        }
    }

    fn nan_test<'expr>(
        &self,
        arguments: &'expr [Expr],
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
        expression: &'expr Expr,
    ) -> Option<RuntimePruningPredicate<'expr>> {
        let Expr::ScalarFunction(function) = expression else {
            return None;
        };
        if function.func.inner().is::<IsNanFunc>() {
            self.nan_test(&function.args, true)
        } else if function.func.inner().is::<StartsWithFunc>() {
            Some(RuntimePruningPredicate::Not(Box::new(
                self.starts_with(&function.args)?,
            )))
        } else {
            None
        }
    }

    fn combine_and<'expr>(
        left: Option<LogicalPredicatePlan<'expr>>,
        right: Option<LogicalPredicatePlan<'expr>>,
    ) -> Option<LogicalPredicatePlan<'expr>> {
        match (left, right) {
            (Some(left), Some(right)) => Some(LogicalPredicatePlan {
                predicate: left.predicate.and(right.predicate),
                complete: left.complete && right.complete,
            }),
            (Some(plan), None) | (None, Some(plan)) => Some(LogicalPredicatePlan {
                predicate: plan.predicate,
                complete: false,
            }),
            (None, None) => None,
        }
    }

    fn combine_or<'expr>(
        left: Option<LogicalPredicatePlan<'expr>>,
        right: Option<LogicalPredicatePlan<'expr>>,
    ) -> Option<LogicalPredicatePlan<'expr>> {
        let (Some(left), Some(right)) = (left, right) else {
            return None;
        };
        Some(LogicalPredicatePlan {
            predicate: left.predicate.or(right.predicate),
            complete: left.complete && right.complete,
        })
    }

    fn comparison<'expr>(
        &self,
        left: &'expr Expr,
        operator: Operator,
        right: &'expr Expr,
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
            (self.comparison_column(left), Self::value(right))
        {
            return Some(RuntimePruningPredicate::Comparison {
                operator: DataFusionPredicateScalar::comparison_operator(operator)?,
                left: column,
                right: RuntimePredicateScalar::Value(value),
            });
        }
        let (Some(value), Some(column)) =
            (Self::value(left), self.comparison_column(right))
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
        arguments: &'expr [Expr],
    ) -> Option<RuntimePruningPredicate<'expr>> {
        let [value, prefix] = arguments else {
            return None;
        };
        let column = self.column(value)?;
        let prefix = Self::value(prefix)?;
        if !matches!(prefix, RuntimePredicateValue::String(_)) {
            return None;
        }
        Some(RuntimePruningPredicate::StartsWith {
            value: RuntimePredicateScalar::Column(column),
            prefix: RuntimePredicateScalar::Value(prefix),
        })
    }

    fn column(&self, expression: &Expr) -> Option<usize> {
        let Expr::Column(column) = expression else {
            return None;
        };
        self.source_schema.index_of(&column.name).ok()
    }

    fn comparison_column(
        &self,
        expression: &Expr,
    ) -> Option<RuntimePredicateScalar<'static>> {
        if let Some(column) = self.column(expression) {
            return Some(RuntimePredicateScalar::Column(column));
        }
        let Expr::Cast(cast) = expression else {
            return None;
        };
        let column = self.column(&cast.expr)?;
        let source = self.source_schema.field(column).data_type();
        let target = cast.field.data_type();
        let target = DataFusionPredicateScalar::integer_widening(source, target)?;
        Some(RuntimePredicateScalar::WidenedIntegerColumn { column, target })
    }

    fn boolean_literal(
        value: &ScalarValue,
    ) -> Option<RuntimePruningPredicate<'static>> {
        match value {
            ScalarValue::Boolean(Some(true)) => {
                Some(RuntimePruningPredicate::AlwaysTrue)
            }
            ScalarValue::Boolean(Some(false) | None) | ScalarValue::Null => {
                Some(RuntimePruningPredicate::AlwaysFalse)
            }
            _ => None,
        }
    }

    fn value(expression: &Expr) -> Option<RuntimePredicateValue<'_>> {
        let Expr::Literal(value, _) = expression else {
            return None;
        };
        DataFusionPredicateScalar::value(value)
    }
}
