//! Provider negotiation and construction for concrete table-scan predicates.

mod adapter;
mod integer;

use arrow_schema::{DataType, Schema, TimeUnit};
use iceberg_lite::expr::{Predicate, PredicateOperator};
use iceberg_lite::spec::Datum;
use lagodb_core::expr::pushdown::PredicatePlan;
use lagodb_core::runtime_api::{
    RuntimeComparisonOperator, RuntimePredicateScalar, RuntimePredicateValue,
    RuntimePruningPredicate,
};

use crate::engine::predicate::IcebergPredicateBuilder;
use crate::engine::predicate::policy::{
    ComparisonOpClass, PredicatePushdownPolicy, PredicateValueKind,
    SupportedPredicateCapability,
};
use crate::error::{IcebergError, IcebergResult};

pub(super) struct IcebergPredicatePlanner<'a> {
    arrow_schema: &'a Schema,
    field_ids: &'a [i32],
}

impl<'a> IcebergPredicatePlanner<'a> {
    pub(super) const fn new(arrow_schema: &'a Schema, field_ids: &'a [i32]) -> Self {
        Self {
            arrow_schema,
            field_ids,
        }
    }

    pub(super) fn plan(
        &self,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        predicate.plan_with(self)
    }

    fn comparison(
        &self,
        operator: RuntimeComparisonOperator,
        left: &RuntimePredicateScalar<'_>,
        right: &RuntimePredicateScalar<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let (column, widening, operator, value) = match (left, right) {
            (
                RuntimePredicateScalar::Column(column),
                RuntimePredicateScalar::Value(value),
            ) => (*column, None, operator, value),
            (
                RuntimePredicateScalar::WidenedIntegerColumn { column, target },
                RuntimePredicateScalar::Value(value),
            ) => (*column, Some(*target), operator, value),
            (
                RuntimePredicateScalar::Value(value),
                RuntimePredicateScalar::Column(column),
            ) => (*column, None, Self::mirror_operator(operator), value),
            (
                RuntimePredicateScalar::Value(value),
                RuntimePredicateScalar::WidenedIntegerColumn { column, target },
            ) => (
                *column,
                Some(*target),
                Self::mirror_operator(operator),
                value,
            ),
            _ => return Ok(PredicatePlan::Unsupported),
        };
        if let Some(widening) = widening {
            return self
                .widened_integer_comparison(column, widening, operator, value);
        }
        let Some(column_kind) = self.column_kind(column) else {
            return Ok(PredicatePlan::Unsupported);
        };
        if column_kind != Self::value_kind(value)
            || !self.decimal_shape_matches(column, value)
        {
            return Ok(PredicatePlan::Unsupported);
        }
        let operator_class = Self::operator_class(operator);
        let Some(capability) = PredicatePushdownPolicy::comparison_capability(
            column_kind,
            operator_class,
        ) else {
            return Ok(PredicatePlan::Unsupported);
        };
        let predicate = self
            .reference(column)?
            .comparison(Self::predicate_operator(operator), Self::datum(value)?);
        Ok(Self::accepted(capability, predicate))
    }

    fn null_test(
        &self,
        value: &RuntimePredicateScalar<'_>,
        is_not_null: bool,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let RuntimePredicateScalar::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let column = *column;
        let Some(column_kind) = self.column_kind(column) else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PredicatePushdownPolicy::supports_null_test(column_kind) {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(PredicatePlan::Exact(
            self.reference(column)?.null_test(is_not_null),
        ))
    }

    fn nan_test(
        &self,
        value: &RuntimePredicateScalar<'_>,
        is_not_nan: bool,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let RuntimePredicateScalar::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let column = *column;
        let Some(column_kind) = self.column_kind(column) else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PredicatePushdownPolicy::supports_nan_test(column_kind) {
            return Ok(PredicatePlan::Unsupported);
        }
        let predicate = self.reference(column)?.nan_test(is_not_nan);
        // Both native complements are unsafe under SQL three-valued logic:
        // NotNan admits NULL, while complementing the guarded NotNan truth
        // set also admits NULL. The positive predicate remains exact.
        Ok(PredicatePlan::ExactNoComplement(predicate))
    }

    fn starts_with(
        &self,
        value: &RuntimePredicateScalar<'_>,
        prefix: &RuntimePredicateScalar<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let (
            RuntimePredicateScalar::Column(column),
            RuntimePredicateScalar::Value(prefix),
        ) = (value, prefix)
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        let column = *column;
        let Some(column_kind) = self.column_kind(column) else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PredicatePushdownPolicy::supports_starts_with(column_kind)
            || Self::value_kind(prefix) != PredicateValueKind::String
        {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(PredicatePlan::ExactNoComplement(
            self.reference(column)?.starts_with(Self::datum(prefix)?),
        ))
    }

    fn accepted(
        capability: SupportedPredicateCapability,
        predicate: Predicate,
    ) -> PredicatePlan<Predicate> {
        match capability {
            SupportedPredicateCapability::Exact => PredicatePlan::Exact(predicate),
            SupportedPredicateCapability::Conservative => {
                PredicatePlan::Conservative(predicate)
            }
        }
    }

    fn accepted_without_complement(
        capability: SupportedPredicateCapability,
        predicate: Predicate,
    ) -> PredicatePlan<Predicate> {
        match capability {
            SupportedPredicateCapability::Exact => {
                PredicatePlan::ExactNoComplement(predicate)
            }
            SupportedPredicateCapability::Conservative => {
                PredicatePlan::Conservative(predicate)
            }
        }
    }

    fn column_kind(&self, column: usize) -> Option<PredicateValueKind> {
        let field = self.arrow_schema.fields().get(column)?;
        match field.data_type() {
            DataType::Boolean => Some(PredicateValueKind::Boolean),
            DataType::Int8 | DataType::Int16 | DataType::Int32 => {
                Some(PredicateValueKind::Integer)
            }
            DataType::Int64 => Some(PredicateValueKind::Long),
            DataType::Date32 => Some(PredicateValueKind::Date),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                Some(if timezone.is_some() {
                    PredicateValueKind::Timestamptz
                } else {
                    PredicateValueKind::Timestamp
                })
            }
            DataType::Decimal128(_, _) => Some(PredicateValueKind::Decimal),
            DataType::Float32 | DataType::Float64 => Some(PredicateValueKind::Float),
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => {
                Some(PredicateValueKind::String)
            }
            _ => None,
        }
    }

    const fn value_kind(value: &RuntimePredicateValue<'_>) -> PredicateValueKind {
        match value {
            RuntimePredicateValue::Boolean(_) => PredicateValueKind::Boolean,
            RuntimePredicateValue::Int32(_) => PredicateValueKind::Integer,
            RuntimePredicateValue::Int64(_) => PredicateValueKind::Long,
            RuntimePredicateValue::Date32(_) => PredicateValueKind::Date,
            RuntimePredicateValue::TimestampMicrosecond(_) => {
                PredicateValueKind::Timestamp
            }
            RuntimePredicateValue::TimestampTzMicrosecond(_) => {
                PredicateValueKind::Timestamptz
            }
            RuntimePredicateValue::Decimal128 { .. } => PredicateValueKind::Decimal,
            RuntimePredicateValue::String(_) => PredicateValueKind::String,
        }
    }

    fn decimal_shape_matches(
        &self,
        column: usize,
        value: &RuntimePredicateValue<'_>,
    ) -> bool {
        match (self.arrow_schema.field(column).data_type(), value) {
            (
                DataType::Decimal128(column_precision, column_scale),
                RuntimePredicateValue::Decimal128 {
                    precision, scale, ..
                },
            ) => column_precision == precision && column_scale == scale,
            (DataType::Decimal128(_, _), _) => false,
            (_, RuntimePredicateValue::Decimal128 { .. }) => false,
            _ => true,
        }
    }

    fn reference(&self, column: usize) -> IcebergResult<IcebergPredicateBuilder> {
        let field_id = self.field_ids.get(column).copied().ok_or(
            IcebergError::InvariantViolated(
                "table-scan predicate column exceeds the bound projection",
            ),
        )?;
        let field = self.arrow_schema.fields().get(column).ok_or(
            IcebergError::InvariantViolated(
                "table-scan predicate column exceeds the bound Arrow schema",
            ),
        )?;
        Ok(IcebergPredicateBuilder::new(field.name().clone(), field_id))
    }

    const fn operator_class(
        operator: RuntimeComparisonOperator,
    ) -> ComparisonOpClass {
        match operator {
            RuntimeComparisonOperator::Equal => ComparisonOpClass::Equal,
            RuntimeComparisonOperator::NotEqual => ComparisonOpClass::NotEqual,
            RuntimeComparisonOperator::LessThan => ComparisonOpClass::Less,
            RuntimeComparisonOperator::LessThanOrEqual => {
                ComparisonOpClass::LessEqual
            }
            RuntimeComparisonOperator::GreaterThan => ComparisonOpClass::Greater,
            RuntimeComparisonOperator::GreaterThanOrEqual => {
                ComparisonOpClass::GreaterEqual
            }
        }
    }

    const fn predicate_operator(
        operator: RuntimeComparisonOperator,
    ) -> PredicateOperator {
        match operator {
            RuntimeComparisonOperator::Equal => PredicateOperator::Eq,
            RuntimeComparisonOperator::NotEqual => PredicateOperator::NotEq,
            RuntimeComparisonOperator::LessThan => PredicateOperator::LessThan,
            RuntimeComparisonOperator::LessThanOrEqual => {
                PredicateOperator::LessThanOrEq
            }
            RuntimeComparisonOperator::GreaterThan => PredicateOperator::GreaterThan,
            RuntimeComparisonOperator::GreaterThanOrEqual => {
                PredicateOperator::GreaterThanOrEq
            }
        }
    }

    const fn mirror_operator(
        operator: RuntimeComparisonOperator,
    ) -> RuntimeComparisonOperator {
        match operator {
            RuntimeComparisonOperator::Equal => RuntimeComparisonOperator::Equal,
            RuntimeComparisonOperator::NotEqual => {
                RuntimeComparisonOperator::NotEqual
            }
            RuntimeComparisonOperator::LessThan => {
                RuntimeComparisonOperator::GreaterThan
            }
            RuntimeComparisonOperator::LessThanOrEqual => {
                RuntimeComparisonOperator::GreaterThanOrEqual
            }
            RuntimeComparisonOperator::GreaterThan => {
                RuntimeComparisonOperator::LessThan
            }
            RuntimeComparisonOperator::GreaterThanOrEqual => {
                RuntimeComparisonOperator::LessThanOrEqual
            }
        }
    }

    fn datum(value: &RuntimePredicateValue<'_>) -> IcebergResult<Datum> {
        Ok(match value {
            RuntimePredicateValue::Boolean(value) => Datum::bool(*value),
            RuntimePredicateValue::Int32(value) => Datum::int(*value),
            RuntimePredicateValue::Int64(value) => Datum::long(*value),
            RuntimePredicateValue::Date32(value) => Datum::date(*value),
            RuntimePredicateValue::TimestampMicrosecond(value) => {
                Datum::timestamp_micros(*value)
            }
            RuntimePredicateValue::TimestampTzMicrosecond(value) => {
                Datum::timestamptz_micros(*value)
            }
            RuntimePredicateValue::Decimal128 {
                coefficient,
                precision,
                scale,
            } => Datum::decimal_from_unscaled(
                *coefficient,
                u32::from(*precision),
                u32::try_from(*scale).map_err(|_| {
                    IcebergError::InvariantViolated(
                        "table-scan decimal predicate has a negative scale",
                    )
                })?,
            )?,
            RuntimePredicateValue::String(value) => Datum::string(value.as_ref()),
        })
    }
}
