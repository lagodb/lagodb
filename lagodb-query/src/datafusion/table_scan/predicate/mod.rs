//! DataFusion adapters for LagoDB's provider-neutral predicate IR.
//!
//! `InListExpr` deliberately remains a DataFusion filter instead of becoming a
//! provider predicate. DataFusion evaluates constant sets with a hash-based
//! static filter, whereas the current Iceberg reader performs one full-batch
//! equality and OR per literal.

mod logical;
mod physical;

use std::borrow::Cow;

use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use lagodb_core::runtime_api::{
    RuntimeComparisonOperator, RuntimeIntegerWidening, RuntimePredicateValue,
};

pub(super) use logical::LogicalPredicateAdapter;
pub(super) use physical::PhysicalPredicateAdapter;

struct DataFusionPredicateScalar;

impl DataFusionPredicateScalar {
    // Floating-point literals intentionally remain residual. This adapter
    // consumes DataFusion expressions and must not reinterpret NaN with
    // PostgreSQL's ordering rules.
    fn value(value: &ScalarValue) -> Option<RuntimePredicateValue<'_>> {
        match value {
            ScalarValue::Boolean(Some(value)) => {
                Some(RuntimePredicateValue::Boolean(*value))
            }
            ScalarValue::Int8(Some(value)) => {
                Some(RuntimePredicateValue::Int32(i32::from(*value)))
            }
            ScalarValue::Int16(Some(value)) => {
                Some(RuntimePredicateValue::Int32(i32::from(*value)))
            }
            ScalarValue::Int32(Some(value)) => {
                Some(RuntimePredicateValue::Int32(*value))
            }
            ScalarValue::Int64(Some(value)) => {
                Some(RuntimePredicateValue::Int64(*value))
            }
            ScalarValue::Date32(Some(value)) => {
                Some(RuntimePredicateValue::Date32(*value))
            }
            ScalarValue::TimestampMicrosecond(Some(value), timezone) => {
                Some(if timezone.is_some() {
                    RuntimePredicateValue::TimestampTzMicrosecond(*value)
                } else {
                    RuntimePredicateValue::TimestampMicrosecond(*value)
                })
            }
            ScalarValue::Decimal128(Some(coefficient), precision, scale)
                if *precision <= 38 && *scale >= 0 && *scale as u8 <= *precision =>
            {
                Some(RuntimePredicateValue::Decimal128 {
                    coefficient: *coefficient,
                    precision: *precision,
                    scale: *scale,
                })
            }
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)) => {
                Some(RuntimePredicateValue::String(Cow::Borrowed(value.as_str())))
            }
            _ => None,
        }
    }

    fn comparison_operator(operator: Operator) -> Option<RuntimeComparisonOperator> {
        match operator {
            Operator::Eq => Some(RuntimeComparisonOperator::Equal),
            Operator::NotEq => Some(RuntimeComparisonOperator::NotEqual),
            Operator::Lt => Some(RuntimeComparisonOperator::LessThan),
            Operator::LtEq => Some(RuntimeComparisonOperator::LessThanOrEqual),
            Operator::Gt => Some(RuntimeComparisonOperator::GreaterThan),
            Operator::GtEq => Some(RuntimeComparisonOperator::GreaterThanOrEqual),
            _ => None,
        }
    }

    fn integer_widening(
        source: &DataType,
        target: &DataType,
    ) -> Option<RuntimeIntegerWidening> {
        match (source, target) {
            (DataType::Int16, DataType::Int32) => {
                Some(RuntimeIntegerWidening::ToInt32)
            }
            (DataType::Int16 | DataType::Int32, DataType::Int64) => {
                Some(RuntimeIntegerWidening::ToInt64)
            }
            _ => None,
        }
    }

    const fn reverse_operator(operator: Operator) -> Operator {
        match operator {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            operator => operator,
        }
    }
}
