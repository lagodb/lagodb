//! Runtime value binding for already-planned Iceberg predicates.

mod value;

use iceberg_lite::expr::{Predicate, PredicateOperator};
use iceberg_lite::spec::Datum;
use lagodb_core::expr::pushdown::FilterBindResult;
use lagodb_core::expr::{RuntimeValue, RuntimeValueBindings};
use lagodb_core::tuple::Decimal128ComparisonValue;

use super::IcebergPredicateBuilder;
use super::error::IcebergFilterError;
use super::plan::{
    PlannedComparisonOperator, PlannedIcebergColumn, PlannedIcebergNode,
    PlannedIcebergPredicate, PlannedValueType,
};
use super::policy::Int32OutOfRange;
use super::temporal::TimestampComparison;

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
            PlannedIcebergNode::BooleanTest { kind, column } => {
                let kind = if negated { kind.negated() } else { *kind };
                Ok(Some(Self::builder(column).boolean_test(kind)))
            }
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
        let operator = if negated {
            operator.negated()
        } else {
            operator
        };
        let predicate = match value {
            BoundComparisonValue::Timestamp(value) => {
                return Ok(value.bind(Self::builder(column), operator));
            }
            BoundComparisonValue::Datum(datum) => {
                Self::builder(column).comparison(operator.into(), datum)
            }
            BoundComparisonValue::OutsideFinite(value) => {
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
}

enum BoundComparisonValue {
    Timestamp(TimestampComparison),
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
