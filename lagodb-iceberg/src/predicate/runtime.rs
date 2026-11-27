//! Runtime pruning-predicate negotiation over bound Arrow scan projections.

use arrow_schema::{
    DataType, Schema, TimeUnit,
    extension::{ExtensionType, Uuid as ArrowUuid},
};
use iceberg_lite::expr::Predicate;
use iceberg_lite::spec::Datum;
use lagodb_core::expr::pushdown::{PredicatePlan, PredicatePlanner};
use lagodb_core::expr::{BooleanTestKind, PgComparisonKind};
use lagodb_core::runtime_api::{
    RuntimeComparisonOperator, RuntimeIntegerWidening, RuntimePredicateScalar,
    RuntimePredicateValue, RuntimePruningPredicate,
};
use uuid::Uuid;

use crate::error::{IcebergError, IcebergResult};

use super::semantics::{
    IcebergColumnRef, Int32OutOfRange, PredicateCapabilities, PredicateCapability,
    PredicateValueKind,
};

pub(crate) struct IcebergRuntimePlanner<'a> {
    arrow_schema: &'a Schema,
    field_ids: &'a [i32],
}

impl<'a> IcebergRuntimePlanner<'a> {
    pub(crate) const fn new(arrow_schema: &'a Schema, field_ids: &'a [i32]) -> Self {
        Self {
            arrow_schema,
            field_ids,
        }
    }

    pub(crate) fn plan(
        &self,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let adapter = RuntimePredicateAdapter {
            arrow_schema: self.arrow_schema,
            field_ids: self.field_ids,
        };
        predicate.plan_with(&adapter)
    }
}

struct RuntimePredicateAdapter<'a> {
    arrow_schema: &'a Schema,
    field_ids: &'a [i32],
}

impl<'a> RuntimePredicateAdapter<'a> {
    fn plan_comparison(
        &self,
        operator: RuntimeComparisonOperator,
        left: &RuntimePredicateScalar<'_>,
        right: &RuntimePredicateScalar<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let operator = Self::operator_kind(operator);
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
            ) => (*column, None, operator.commuted(), value),
            (
                RuntimePredicateScalar::Value(value),
                RuntimePredicateScalar::WidenedIntegerColumn { column, target },
            ) => (*column, Some(*target), operator.commuted(), value),
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
        let Some(capability) =
            PredicateCapabilities::comparison_capability(column_kind, operator)
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(datum) = self.comparison_datum(column, value)? else {
            return Ok(PredicatePlan::Unsupported);
        };
        let predicate = self.column_ref(column)?.pg_comparison(operator, datum);
        Ok(Self::accepted(capability, predicate))
    }

    fn widened_integer_comparison(
        &self,
        column: usize,
        widening: RuntimeIntegerWidening,
        operator: PgComparisonKind,
        value: &RuntimePredicateValue<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        if !self.supports_integer_widening(column, widening) {
            return Ok(PredicatePlan::Unsupported);
        }
        let Some(capability) = PredicateCapabilities::comparison_capability(
            PredicateValueKind::Integer,
            operator,
        ) else {
            return Ok(PredicatePlan::Unsupported);
        };
        let (predicate, complementable) = match (widening, value) {
            (
                RuntimeIntegerWidening::ToInt32,
                RuntimePredicateValue::Int32(value),
            ) => (
                self.column_ref(column)?
                    .pg_comparison(operator, Datum::int(*value)),
                true,
            ),
            (
                RuntimeIntegerWidening::ToInt64,
                RuntimePredicateValue::Int64(value),
            ) => match Int32OutOfRange::narrow(*value) {
                Ok(value) => (
                    self.column_ref(column)?
                        .pg_comparison(operator, Datum::int(value)),
                    true,
                ),
                Err(boundary) if boundary.comparison_matches_non_null(operator) => {
                    (self.column_ref(column)?.null_test(true), false)
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
            ) | (DataType::Int32, RuntimeIntegerWidening::ToInt64)
        )
    }

    fn plan_null_test(
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
        if !PredicateCapabilities::supports_null_test(column_kind) {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(PredicatePlan::Exact(
            self.column_ref(column)?.null_test(is_not_null),
        ))
    }

    fn plan_nan_test(
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
        if !PredicateCapabilities::supports_nan_test(column_kind) {
            return Ok(PredicatePlan::Unsupported);
        }
        let predicate = self.column_ref(column)?.nan_test(is_not_nan);
        // Both native complements are unsafe under SQL three-valued logic:
        // NotNan admits NULL, while complementing the guarded NotNan truth
        // set also admits NULL. The positive predicate remains exact.
        Ok(PredicatePlan::ExactNoComplement(predicate))
    }

    fn plan_starts_with(
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
        if !PredicateCapabilities::supports_starts_with(column_kind)
            || Self::value_kind(prefix) != PredicateValueKind::String
        {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(PredicatePlan::ExactNoComplement(
            self.column_ref(column)?.starts_with(Self::datum(prefix)?),
        ))
    }

    /// Source-bound runtime literals; physical binary width does not identify
    /// UUID without checking the Arrow extension type.
    fn comparison_datum(
        &self,
        column: usize,
        value: &RuntimePredicateValue<'_>,
    ) -> IcebergResult<Option<Datum>> {
        let field = self.arrow_schema.field(column);
        if let RuntimePredicateValue::Binary(bytes) = value {
            return Ok(match field.data_type() {
                DataType::FixedSizeBinary(width)
                    if bytes.len() == *width as usize =>
                {
                    if field.extension_type_name() == Some(ArrowUuid::NAME) {
                        let bytes = bytes
                            .as_ref()
                            .try_into()
                            .expect("UUID field has width 16");
                        Some(Datum::uuid(Uuid::from_bytes(bytes)))
                    } else {
                        Some(Datum::fixed(bytes.iter().copied()))
                    }
                }
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                    Some(Datum::binary(bytes.iter().copied()))
                }
                _ => None,
            });
        }
        Self::datum(value).map(Some)
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
            RuntimePredicateValue::Binary(value) => {
                Datum::binary(value.iter().copied())
            }
        })
    }

    fn accepted(
        capability: PredicateCapability,
        predicate: Predicate,
    ) -> PredicatePlan<Predicate> {
        match capability {
            PredicateCapability::Exact => PredicatePlan::Exact(predicate),
            PredicateCapability::Conservative => {
                PredicatePlan::Conservative(predicate)
            }
        }
    }

    fn accepted_without_complement(
        capability: PredicateCapability,
        predicate: Predicate,
    ) -> PredicatePlan<Predicate> {
        match capability {
            PredicateCapability::Exact => PredicatePlan::ExactNoComplement(predicate),
            PredicateCapability::Conservative => {
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
            DataType::Time64(TimeUnit::Microsecond) => Some(PredicateValueKind::Time),
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => Some(PredicateValueKind::Binary),
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
            RuntimePredicateValue::Binary(_) => PredicateValueKind::Binary,
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

    fn column_ref(&self, column: usize) -> IcebergResult<IcebergColumnRef<'a>> {
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
        Ok(IcebergColumnRef::new(field.name(), field_id))
    }

    const fn operator_kind(operator: RuntimeComparisonOperator) -> PgComparisonKind {
        match operator {
            RuntimeComparisonOperator::Equal => PgComparisonKind::Equal,
            RuntimeComparisonOperator::NotEqual => PgComparisonKind::NotEqual,
            RuntimeComparisonOperator::LessThan => PgComparisonKind::Less,
            RuntimeComparisonOperator::LessThanOrEqual => PgComparisonKind::LessEqual,
            RuntimeComparisonOperator::GreaterThan => PgComparisonKind::Greater,
            RuntimeComparisonOperator::GreaterThanOrEqual => {
                PgComparisonKind::GreaterEqual
            }
        }
    }
}

impl<'predicate>
    PredicatePlanner<RuntimePredicateScalar<'predicate>, RuntimeComparisonOperator>
    for RuntimePredicateAdapter<'_>
{
    type Predicate = Predicate;
    type Error = IcebergError;

    fn always_true(&self) -> IcebergResult<PredicatePlan<Predicate>> {
        Ok(PredicatePlan::Exact(Predicate::AlwaysTrue))
    }

    fn always_false(&self) -> IcebergResult<PredicatePlan<Predicate>> {
        Ok(PredicatePlan::Exact(Predicate::AlwaysFalse))
    }

    fn strict_true(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let RuntimePredicateScalar::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(column_kind) = self.column_kind(*column) else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PredicateCapabilities::supports_null_test(column_kind) {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(PredicatePlan::ExactNoComplement(
            self.column_ref(*column)?.null_test(true),
        ))
    }

    fn strict_false(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let RuntimePredicateScalar::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        self.column_ref(*column)?;
        Ok(PredicatePlan::ExactNoComplement(Predicate::AlwaysFalse))
    }

    fn comparison(
        &self,
        operator: &RuntimeComparisonOperator,
        left: &RuntimePredicateScalar<'predicate>,
        right: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.plan_comparison(*operator, left, right)
    }

    fn boolean_test(
        &self,
        kind: BooleanTestKind,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        let RuntimePredicateScalar::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let Some(column_kind) = self.column_kind(*column) else {
            return Ok(PredicatePlan::Unsupported);
        };
        if !PredicateCapabilities::supports_boolean_test(column_kind) {
            return Ok(PredicatePlan::Unsupported);
        }
        // The shared walker negates BooleanTest by flipping its semantic kind.
        // A native complement of the compiled truth set is not used.
        Ok(PredicatePlan::ExactNoComplement(
            self.column_ref(*column)?.boolean_test(kind),
        ))
    }

    fn is_null(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.plan_null_test(value, false)
    }

    fn is_not_null(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.plan_null_test(value, true)
    }

    fn is_nan(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.plan_nan_test(value, false)
    }

    fn is_not_nan(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.plan_nan_test(value, true)
    }

    fn starts_with(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
        prefix: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.plan_starts_with(value, prefix)
    }

    // Query offload keeps `IN` in DataFusion; see `table_scan_predicate`.
    fn in_set(
        &self,
        _operator: &RuntimeComparisonOperator,
        _value: &RuntimePredicateScalar<'predicate>,
        _set: &RuntimePredicateScalar<'predicate>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        Ok(PredicatePlan::Unsupported)
    }

    fn conjunction(&self, predicates: Vec<Predicate>) -> Predicate {
        predicates
            .into_iter()
            .reduce(Predicate::and)
            .expect("predicate negotiation accepted a conjunction child")
    }

    fn disjunction(&self, predicates: Vec<Predicate>) -> Predicate {
        predicates
            .into_iter()
            .reduce(Predicate::or)
            .expect("predicate negotiation accepted a disjunction child")
    }

    fn negate(&self, predicate: Predicate) -> Predicate {
        !predicate
    }
}
