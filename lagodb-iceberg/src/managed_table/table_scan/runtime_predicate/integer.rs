//! Lossless integer-widening predicates for the runtime adapter.

use arrow_schema::DataType;
use iceberg_lite::expr::Predicate;
use iceberg_lite::spec::Datum;
use lagodb_core::expr::pushdown::PredicatePlan;
use lagodb_core::runtime_api::{
    RuntimeComparisonOperator, RuntimeIntegerWidening, RuntimePredicateValue,
};

use crate::engine::predicate::policy::{
    Int32OutOfRange, PredicatePushdownPolicy, PredicateValueKind,
};
use crate::error::IcebergResult;

use super::IcebergPredicatePlanner;

impl IcebergPredicatePlanner<'_> {
    pub(super) fn widened_integer_comparison(
        &self,
        column: usize,
        widening: RuntimeIntegerWidening,
        operator: RuntimeComparisonOperator,
        value: &RuntimePredicateValue<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        if !self.supports_integer_widening(column, widening) {
            return Ok(PredicatePlan::Unsupported);
        }
        let operator_class = Self::operator_class(operator);
        let Some(capability) = PredicatePushdownPolicy::comparison_capability(
            PredicateValueKind::Integer,
            operator_class,
        ) else {
            return Ok(PredicatePlan::Unsupported);
        };
        let (predicate, complementable) = match (widening, value) {
            (
                RuntimeIntegerWidening::ToInt32,
                RuntimePredicateValue::Int32(value),
            ) => (
                self.reference(column)?.comparison(
                    Self::predicate_operator(operator),
                    Datum::int(*value),
                ),
                true,
            ),
            (
                RuntimeIntegerWidening::ToInt64,
                RuntimePredicateValue::Int64(value),
            ) => match Int32OutOfRange::narrow(*value) {
                Ok(value) => (
                    self.reference(column)?.comparison(
                        Self::predicate_operator(operator),
                        Datum::int(value),
                    ),
                    true,
                ),
                Err(boundary)
                    if boundary.comparison_matches_non_null(operator_class) =>
                {
                    (self.reference(column)?.null_test(true), false)
                }
                Err(_) => (Predicate::AlwaysFalse, false),
            },
            _ => return Ok(PredicatePlan::Unsupported),
        };
        Ok(if complementable {
            Self::accepted(capability, predicate)
        } else {
            Self::accepted_without_complement(capability, predicate)
        })
    }

    fn supports_integer_widening(
        &self,
        column: usize,
        widening: RuntimeIntegerWidening,
    ) -> bool {
        let Some(field) = self.arrow_schema.fields().get(column) else {
            return false;
        };
        matches!(
            (field.data_type(), widening),
            (
                DataType::Int16,
                RuntimeIntegerWidening::ToInt32 | RuntimeIntegerWidening::ToInt64
            ) | (DataType::Int32, RuntimeIntegerWidening::ToInt64,)
        )
    }
}
