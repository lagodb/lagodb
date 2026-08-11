//! Runtime value binding for already-planned Iceberg predicates.

use iceberg_lite::expr::{
    BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression,
};
use iceberg_lite::spec::Datum;
use pg_arrow_conv::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
use pg_lakebase_core::expr::pushdown::{
    FilterBindResult, FilterValue, FilterValueBindings,
};
use pgrx::FromDatum;

use super::error::IcebergFilterError;
use super::planned::{
    PlannedComparisonOperator, PlannedIcebergColumn, PlannedIcebergNode,
    PlannedIcebergPredicate, PlannedValueType,
};

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
        values: FilterValueBindings<'_>,
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
    values: FilterValueBindings<'a>,
}

impl IcebergFilterBinder<'_> {
    fn bind_node(
        &self,
        node: &PlannedIcebergNode,
        negated: bool,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        match node {
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

    fn bind_comparison(
        &self,
        operator: PlannedComparisonOperator,
        column: &PlannedIcebergColumn,
        value_type: PlannedValueType,
        value: FilterValue,
        negated: bool,
    ) -> Result<Option<Predicate>, IcebergFilterError> {
        // A strict SQL comparison with NULL is UNKNOWN. Its truth set remains
        // empty under NOT, so folding must happen after carrying negation to
        // the leaf rather than by negating `AlwaysFalse` at a parent node.
        if value.is_null() {
            return Ok(Some(Predicate::AlwaysFalse));
        }
        let Some(datum) = (unsafe { Self::decode_datum(value_type, value) })? else {
            return Ok(None);
        };
        let predicate = Predicate::Binary(BinaryExpression::new(
            operator.into(),
            Self::reference(column),
            datum,
        ));
        Ok(Some(if negated { !predicate } else { predicate }))
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
        Predicate::Unary(UnaryExpression::new(
            if is_not_null {
                PredicateOperator::NotNull
            } else {
                PredicateOperator::IsNull
            },
            Self::reference(column),
        ))
    }

    fn reference(column: &PlannedIcebergColumn) -> Reference {
        Reference::new_bound_field(column.debug_name.clone(), column.field_id)
    }

    /// # Safety
    ///
    /// The value metadata must describe its non-NULL PostgreSQL Datum, whose
    /// memory remains live for this binding call.
    unsafe fn decode_datum(
        value_type: PlannedValueType,
        value: FilterValue,
    ) -> Result<Option<Datum>, IcebergFilterError> {
        let type_oid = value.metadata().value_type.type_oid;
        let datum = unsafe { value.datum() };
        let decoded = match value_type {
            PlannedValueType::Int2 => Some(Datum::int(
                unsafe { i16::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?
                    as i32,
            )),
            PlannedValueType::Int4 => Some(Datum::int(
                unsafe { i32::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
            )),
            PlannedValueType::Int8 => Some(Datum::long(
                unsafe { i64::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
            )),
            PlannedValueType::Date => {
                let days = unsafe { i32::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                if matches!(days, i32::MIN | i32::MAX) {
                    None
                } else {
                    pg_epoch_days_to_unix_days(days).map(Datum::date)
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
            PlannedValueType::String => Some(Datum::string(
                unsafe { String::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
            )),
        };
        Ok(decoded)
    }

    fn timestamp_micros(value: i64, with_timezone: bool) -> Option<Datum> {
        if matches!(value, i64::MIN | i64::MAX) {
            return None;
        }
        pg_epoch_micros_to_unix_micros(value).map(|value| {
            if with_timezone {
                Datum::timestamptz_micros(value)
            } else {
                Datum::timestamp_micros(value)
            }
        })
    }
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
