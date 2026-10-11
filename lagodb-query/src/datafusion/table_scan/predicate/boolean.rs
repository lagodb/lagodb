//! SQL boolean predicates at the DataFusion scan boundary.

use arrow_schema::DataType;
use datafusion::logical_expr::Expr;
use lagodb_core::expr::BooleanTestKind;
use lagodb_core::runtime_api::{
    RuntimeComparisonOperator, RuntimePredicateScalar, RuntimePredicateValue,
    RuntimePruningPredicate,
};

use super::logical::{LogicalPredicateAdapter, LogicalPredicatePlan};

impl LogicalPredicateAdapter<'_> {
    pub(super) fn boolean_column(
        &self,
        expression: &Expr,
        expected: bool,
    ) -> Option<RuntimePruningPredicate<'static>> {
        let column = self.column(expression)?;
        if self.source_schema.field(column).data_type() != &DataType::Boolean {
            return None;
        }
        Some(RuntimePruningPredicate::Comparison {
            operator: RuntimeComparisonOperator::Equal,
            left: RuntimePredicateScalar::Column(column),
            right: RuntimePredicateScalar::Value(RuntimePredicateValue::Boolean(
                expected,
            )),
        })
    }

    pub(super) fn boolean_test(
        &self,
        expression: &Expr,
        kind: BooleanTestKind,
    ) -> Option<LogicalPredicatePlan<'static>> {
        let column = self.column(expression)?;
        if self.source_schema.field(column).data_type() != &DataType::Boolean {
            return None;
        }
        Some(LogicalPredicatePlan::complete(
            RuntimePruningPredicate::BooleanTest {
                kind,
                value: RuntimePredicateScalar::Column(column),
            },
        ))
    }
}
