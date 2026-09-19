//! Runtime value binding for already-planned Iceberg predicates.

use iceberg_lite::expr::{Predicate, PredicateOperator};
use iceberg_lite::spec::Datum;
use lagodb_arrow::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
use lagodb_core::expr::pushdown::FilterBindResult;
use lagodb_core::expr::{RuntimeValue, RuntimeValueBindings};
use lagodb_core::tuple::Decimal128ComparisonValue;
use pgrx::FromDatum;

use crate::error::IcebergError;

use super::IcebergPredicateBuilder;
use super::error::IcebergFilterError;
use super::plan::{
    PlannedComparisonOperator, PlannedIcebergColumn, PlannedIcebergNode,
    PlannedIcebergPredicate, PlannedValueType,
};
use super::policy::Int32OutOfRange;

pub(crate) struct BoundIcebergPredicate {
    schema_id: i32,
    predicate: Predicate,
}

impl BoundIcebergPredicate {
    pub(crate) fn validate_schema<'a>(
        predicates: impl IntoIterator<Item = &'a Self>,
        execution_schema_id: i32,
    ) -> Result<(), IcebergFilterError> {
        for predicate in predicates {
            if predicate.schema_id != execution_schema_id {
                return Err(IcebergFilterError::SchemaMismatch {
                    planned: predicate.schema_id,
                    execution: execution_schema_id,
                });
            }
        }
        Ok(())
    }

    pub(crate) fn conjoin<'a>(
        predicates: impl IntoIterator<Item = &'a Self>,
    ) -> Option<Predicate> {
        let mut predicates = predicates.into_iter();
        let first = predicates.next()?.predicate.clone();
        Some(predicates.fold(first, |left, right| {
            Predicate::and(left, right.predicate.clone())
        }))
    }
}

impl PlannedIcebergPredicate {
    pub(crate) fn bind(
        &self,
        values: RuntimeValueBindings<'_>,
    ) -> Result<FilterBindResult<BoundIcebergPredicate>, IcebergFilterError> {
        let binder = IcebergFilterBinder { values };
        let Some(predicate) = binder.bind_node(self.root(), false)? else {
            return Ok(FilterBindResult::ValueNotRepresentable);
        };
        Ok(FilterBindResult::Bound(BoundIcebergPredicate {
            schema_id: self.schema_id(),
            predicate,
        }))
    }
}

struct IcebergFilterBinder<'a> {
    values: RuntimeValueBindings<'a>,
}

impl IcebergFilterBinder<'_> {
    fn bind_node(
        &self,
        node: &PlannedIcebergNode,
        negated: bool,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        match node {
            PlannedIcebergNode::AlwaysTrue => Ok(Some(if negated {
                Predicate::AlwaysFalse
            } else {
                Predicate::AlwaysTrue
            })),
            PlannedIcebergNode::AlwaysFalse => Ok(Some(if negated {
                Predicate::AlwaysTrue
            } else {
                Predicate::AlwaysFalse
            })),
            PlannedIcebergNode::Comparison {
                operator,
                column,
                value,
                value_type,
            } => self.bind_comparison(
                *operator,
                column,
                *value_type,
                self.values.value(*value),
                negated,
            ),
            PlannedIcebergNode::IsNull(column) => {
                Ok(Some(Self::null_test(column, negated)))
            }
            PlannedIcebergNode::IsNotNull(column) => {
                Ok(Some(Self::null_test(column, !negated)))
            }
            PlannedIcebergNode::IsNan(column) => {
                Ok(Some(Self::nan_test(column, negated)))
            }
            PlannedIcebergNode::IsNotNan(column) => {
                Ok(Some(Self::nan_test(column, !negated)))
            }
            PlannedIcebergNode::StartsWith { column, prefix } => {
                self.bind_starts_with(column, self.values.value(*prefix), negated)
            }
            PlannedIcebergNode::And(children) => self.bind_logical(
                children,
                negated,
                if negated {
                    LogicalKind::Or
                } else {
                    LogicalKind::And
                },
            ),
            PlannedIcebergNode::Or(children) => self.bind_logical(
                children,
                negated,
                if negated {
                    LogicalKind::And
                } else {
                    LogicalKind::Or
                },
            ),
            PlannedIcebergNode::Not(child) => self.bind_node(child, !negated),
        }
    }

    fn bind_starts_with(
        &self,
        column: &PlannedIcebergColumn,
        value: RuntimeValue,
        negated: bool,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        if value.is_null() {
            return Ok(Some(Predicate::AlwaysFalse));
        }
        let Some(BoundComparisonValue::Datum(prefix)) =
            (unsafe { Self::decode_value(PlannedValueType::String, value) })?
        else {
            return Ok(None);
        };
        let predicate = Self::builder(column).starts_with(prefix);
        Ok(Some(if negated { !predicate } else { predicate }))
    }

    fn bind_comparison(
        &self,
        operator: PlannedComparisonOperator,
        column: &PlannedIcebergColumn,
        value_type: PlannedValueType,
        value: RuntimeValue,
        negated: bool,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        // A strict SQL comparison with NULL is UNKNOWN. Its truth set remains
        // empty under NOT, so folding must happen after carrying negation to
        // the leaf rather than by negating `AlwaysFalse` at a parent node.
        if value.is_null() {
            return Ok(Some(Predicate::AlwaysFalse));
        }
        let Some(value) = (unsafe { Self::decode_value(value_type, value) })? else {
            return Ok(None);
        };
        let predicate = match value {
            BoundComparisonValue::Datum(datum) => {
                let operator = if negated {
                    operator.negated()
                } else {
                    operator
                };
                Self::builder(column).comparison(operator.into(), datum)
            }
            BoundComparisonValue::OutsideFinite(value) => {
                let operator = if negated {
                    operator.negated()
                } else {
                    operator
                };
                if value
                    .matches_finite_column(operator.kind())
                    .expect("outside-finite Decimal128 value")
                {
                    Self::null_test(column, true)
                } else {
                    Predicate::AlwaysFalse
                }
            }
            BoundComparisonValue::OutsideInteger(value) => {
                let operator = if negated {
                    operator.negated()
                } else {
                    operator
                };
                if value.comparison_matches_non_null(operator.kind()) {
                    Self::null_test(column, true)
                } else {
                    Predicate::AlwaysFalse
                }
            }
        };
        Ok(Some(predicate))
    }

    fn bind_logical(
        &self,
        children: &[PlannedIcebergNode],
        negated: bool,
        kind: LogicalKind,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        let (mut result, combine): (_, fn(Predicate, Predicate) -> Predicate) =
            match kind {
                LogicalKind::And => (Predicate::AlwaysTrue, Predicate::and),
                LogicalKind::Or => (Predicate::AlwaysFalse, Predicate::or),
            };
        for child in children {
            let Some(child) = self.bind_node(child, negated)? else {
                return Ok(None);
            };
            result = combine(result, child);
        }
        Ok(Some(result))
    }

    fn null_test(column: &PlannedIcebergColumn, is_not_null: bool) -> Predicate {
        Self::builder(column).null_test(is_not_null)
    }

    fn nan_test(column: &PlannedIcebergColumn, is_not_nan: bool) -> Predicate {
        Self::builder(column).nan_test(is_not_nan)
    }

    fn builder(column: &PlannedIcebergColumn) -> IcebergPredicateBuilder {
        IcebergPredicateBuilder::new(column.debug_name.clone(), column.field_id)
    }

    /// # Safety
    ///
    /// The value metadata must describe its non-NULL PostgreSQL Datum, whose
    /// memory remains live for this binding call.
    unsafe fn decode_value(
        value_type: PlannedValueType,
        value: RuntimeValue,
    ) -> Result<Option<BoundComparisonValue>, IcebergFilterError> {
        let type_oid = value.metadata().value_type.type_oid;
        let datum = unsafe { value.datum() };
        let decoded = match value_type {
            PlannedValueType::Int2 => Some(BoundComparisonValue::Datum(Datum::int(
                unsafe { i16::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?
                    as i32,
            ))),
            PlannedValueType::Int4 => Some(BoundComparisonValue::Datum(Datum::int(
                unsafe { i32::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
            ))),
            PlannedValueType::Int8 => Some(BoundComparisonValue::Datum(Datum::long(
                unsafe { i64::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
            ))),
            PlannedValueType::Int8ToInt => {
                let value = unsafe { i64::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                Some(match Int32OutOfRange::narrow(value) {
                    Ok(value) => BoundComparisonValue::Datum(Datum::int(value)),
                    Err(boundary) => BoundComparisonValue::OutsideInteger(boundary),
                })
            }
            PlannedValueType::Date => {
                let days = unsafe { i32::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                if matches!(days, i32::MIN | i32::MAX) {
                    None
                } else {
                    pg_epoch_days_to_unix_days(days)
                        .map(Datum::date)
                        .map(BoundComparisonValue::Datum)
                }
            }
            PlannedValueType::Timestamp => {
                let micros = unsafe { i64::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                Self::timestamp_micros(micros, false)
            }
            PlannedValueType::Timestamptz => {
                let micros = unsafe { i64::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                Self::timestamp_micros(micros, true)
            }
            PlannedValueType::String => {
                Some(BoundComparisonValue::Datum(Datum::string(
                    // The persisted String plan proves the planner resolved
                    // `Utf8ServerEncoding`; execution intentionally does not
                    // repeat that statement-invariant check.
                    unsafe { String::from_datum(datum, false) }
                        .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
                )))
            }
            PlannedValueType::Decimal128(semantics) => Some(
                match unsafe { semantics.codec().encode_comparison_datum(datum) }? {
                    Decimal128ComparisonValue::Finite(coefficient) => {
                        BoundComparisonValue::Datum(
                            Datum::decimal_from_unscaled(
                                coefficient,
                                u32::from(semantics.precision()),
                                u32::try_from(semantics.scale()).expect(
                                    "validated Decimal128 scale is non-negative",
                                ),
                            )
                            .map_err(IcebergError::from)?,
                        )
                    }
                    Decimal128ComparisonValue::NegativeInfinity => {
                        BoundComparisonValue::OutsideFinite(
                            Decimal128ComparisonValue::NegativeInfinity,
                        )
                    }
                    special @ (Decimal128ComparisonValue::PositiveInfinity
                    | Decimal128ComparisonValue::NaN) => {
                        BoundComparisonValue::OutsideFinite(special)
                    }
                },
            ),
        };
        Ok(decoded)
    }

    fn timestamp_micros(
        value: i64,
        with_timezone: bool,
    ) -> Option<BoundComparisonValue> {
        if matches!(value, i64::MIN | i64::MAX) {
            return None;
        }
        pg_epoch_micros_to_unix_micros(value).map(|value| {
            BoundComparisonValue::Datum(if with_timezone {
                Datum::timestamptz_micros(value)
            } else {
                Datum::timestamp_micros(value)
            })
        })
    }
}

enum BoundComparisonValue {
    Datum(Datum),
    OutsideFinite(Decimal128ComparisonValue),
    OutsideInteger(Int32OutOfRange),
}

#[derive(Clone, Copy)]
enum LogicalKind {
    And,
    Or,
}

impl From<PlannedComparisonOperator> for PredicateOperator {
    fn from(value: PlannedComparisonOperator) -> Self {
        match value {
            PlannedComparisonOperator::Eq => Self::Eq,
            PlannedComparisonOperator::NotEq => Self::NotEq,
            PlannedComparisonOperator::Lt => Self::LessThan,
            PlannedComparisonOperator::Le => Self::LessThanOrEq,
            PlannedComparisonOperator::Gt => Self::GreaterThan,
            PlannedComparisonOperator::Ge => Self::GreaterThanOrEq,
        }
    }
}
