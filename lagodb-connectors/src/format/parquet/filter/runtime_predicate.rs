//! Negotiation of query-offload runtime predicates against the bound schema.
//!
//! Columns are bound-schema positions whose names are the Parquet root names.
//! The result is the same bound tree the FDW binder produces, so both paths
//! share per-file lowering, row filtering and row-group pruning. Float
//! comparisons and sets stay with the query engine, as for Iceberg.

use arrow_schema::{DataType, Schema, TimeUnit};
use lagodb_core::datum::Decimal128Semantics;
use lagodb_core::expr::BooleanTestKind;
use lagodb_core::expr::pushdown::{PredicatePlan, PredicatePlanner};
use lagodb_core::runtime_api::{
    RuntimeComparisonOperator, RuntimeIntegerWidening, RuntimePredicateScalar,
    RuntimePredicateValue, RuntimePruningPredicate,
};

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::ParquetBoundPredicate;
use super::bound::BoundNode;
use super::integer::IntegerWidth;
use super::operator::ComparisonOperator;
use super::value::BoundValue;

pub(crate) struct ParquetRuntimePlanner<'a> {
    schema: &'a Schema,
}

impl<'a> ParquetRuntimePlanner<'a> {
    pub(crate) const fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    pub(crate) fn plan(
        &self,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> Result<PredicatePlan<ParquetBoundPredicate>, ConnectorError> {
        let planner = RuntimeNodePlanner {
            schema: self.schema,
        };
        Ok(match predicate.plan_with(&planner)? {
            PredicatePlan::Unsupported => PredicatePlan::Unsupported,
            PredicatePlan::Partial(node) => {
                PredicatePlan::Partial(ParquetBoundPredicate::new(node))
            }
            PredicatePlan::Conservative(node) => {
                PredicatePlan::Conservative(ParquetBoundPredicate::new(node))
            }
            PredicatePlan::Exact(node) => {
                PredicatePlan::Exact(ParquetBoundPredicate::new(node))
            }
            PredicatePlan::ExactNoComplement(node) => {
                PredicatePlan::ExactNoComplement(ParquetBoundPredicate::new(node))
            }
        })
    }
}

/// The core walker's leaf planner, producing module-private bound trees.
struct RuntimeNodePlanner<'a> {
    schema: &'a Schema,
}

impl<'a> RuntimeNodePlanner<'a> {
    fn column(
        &self,
        column: usize,
    ) -> Result<(&'a str, &'a DataType), ConnectorError> {
        let field = self.schema.fields().get(column).ok_or_else(|| {
            ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                "table-scan predicate column exceeds the bound schema",
            )
        })?;
        Ok((field.name(), field.data_type()))
    }

    fn comparison_plan(
        &self,
        operator: RuntimeComparisonOperator,
        left: &RuntimePredicateScalar<'_>,
        right: &RuntimePredicateScalar<'_>,
    ) -> Result<PredicatePlan<BoundNode>, ConnectorError> {
        let (column, widening, operator, value) = match (left, right) {
            (
                RuntimePredicateScalar::Column(column),
                RuntimePredicateScalar::Value(value),
            ) => (*column, None, ComparisonOperator::from(operator), value),
            (
                RuntimePredicateScalar::WidenedIntegerColumn { column, target },
                RuntimePredicateScalar::Value(value),
            ) => (
                *column,
                Some(*target),
                ComparisonOperator::from(operator),
                value,
            ),
            (
                RuntimePredicateScalar::Value(value),
                RuntimePredicateScalar::Column(column),
            ) => (
                *column,
                None,
                ComparisonOperator::from(operator).mirrored(),
                value,
            ),
            (
                RuntimePredicateScalar::Value(value),
                RuntimePredicateScalar::WidenedIntegerColumn { column, target },
            ) => (
                *column,
                Some(*target),
                ComparisonOperator::from(operator).mirrored(),
                value,
            ),
            _ => return Ok(PredicatePlan::Unsupported),
        };
        let (name, data_type) = self.column(column)?;
        let value = match (data_type, widening, value) {
            (DataType::Boolean, None, RuntimePredicateValue::Boolean(value))
                if matches!(
                    operator,
                    ComparisonOperator::Eq | ComparisonOperator::NotEq
                ) =>
            {
                BoundValue::Bool(*value)
            }
            (DataType::Int32, None, RuntimePredicateValue::Int32(value)) => {
                BoundValue::I32(*value)
            }
            (DataType::Int64, None, RuntimePredicateValue::Int64(value)) => {
                BoundValue::I64(*value)
            }
            (
                DataType::Int16,
                Some(RuntimeIntegerWidening::ToInt32),
                RuntimePredicateValue::Int32(value),
            ) => {
                return Ok(Self::narrowed(
                    name,
                    operator,
                    IntegerWidth::I16,
                    i64::from(*value),
                ));
            }
            (
                DataType::Int16,
                Some(RuntimeIntegerWidening::ToInt64),
                RuntimePredicateValue::Int64(value),
            ) => {
                return Ok(Self::narrowed(name, operator, IntegerWidth::I16, *value));
            }
            (
                DataType::Int32,
                Some(RuntimeIntegerWidening::ToInt64),
                RuntimePredicateValue::Int64(value),
            ) => {
                return Ok(Self::narrowed(name, operator, IntegerWidth::I32, *value));
            }
            (DataType::Date32, None, RuntimePredicateValue::Date32(value)) => {
                BoundValue::Date32(*value)
            }
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                None,
                RuntimePredicateValue::TimestampMicrosecond(value),
            )
            | (
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
                None,
                RuntimePredicateValue::TimestampTzMicrosecond(value),
            ) => BoundValue::Timestamp(*value),
            (
                DataType::Decimal128(column_precision, column_scale),
                None,
                RuntimePredicateValue::Decimal128 {
                    coefficient,
                    precision,
                    scale,
                },
            ) if column_precision == precision && column_scale == scale => {
                let Some(semantics) = Decimal128Semantics::new(*precision, *scale)
                else {
                    return Ok(PredicatePlan::Unsupported);
                };
                BoundValue::Decimal128 {
                    coefficient: *coefficient,
                    semantics,
                }
            }
            (DataType::Utf8, None, RuntimePredicateValue::String(value)) => {
                BoundValue::String(value.as_ref().into())
            }
            (
                DataType::LargeBinary | DataType::FixedSizeBinary(_),
                None,
                RuntimePredicateValue::Binary(value),
            ) => BoundValue::Binary(value.as_ref().into()),
            _ => return Ok(PredicatePlan::Unsupported),
        };
        Ok(PredicatePlan::Exact(BoundNode::Comparison {
            operator,
            column: name.into(),
            value,
        }))
    }

    /// A widened integer value narrowed to the column's width; a value
    /// outside it decides the comparison for every non-NULL row, and that
    /// constant truth set has no NULL-preserving complement.
    fn narrowed(
        column: &str,
        operator: ComparisonOperator,
        width: IntegerWidth,
        value: i64,
    ) -> PredicatePlan<BoundNode> {
        match width.narrow(value) {
            Ok(value) => PredicatePlan::Exact(BoundNode::Comparison {
                operator,
                column: column.into(),
                value,
            }),
            Err(outside) if outside.comparison_matches_non_null(operator) => {
                PredicatePlan::ExactNoComplement(BoundNode::IsNotNull(column.into()))
            }
            Err(_) => PredicatePlan::ExactNoComplement(BoundNode::NeverTrue),
        }
    }

    /// A unary test on a column whose type `admits` it.
    fn column_test(
        &self,
        value: &RuntimePredicateScalar<'_>,
        admits: fn(&DataType) -> bool,
        build: impl FnOnce(Box<str>) -> BoundNode,
        accepted: fn(BoundNode) -> PredicatePlan<BoundNode>,
    ) -> Result<PredicatePlan<BoundNode>, ConnectorError> {
        let RuntimePredicateScalar::Column(column) = value else {
            return Ok(PredicatePlan::Unsupported);
        };
        let (name, data_type) = self.column(*column)?;
        if !admits(data_type) {
            return Ok(PredicatePlan::Unsupported);
        }
        Ok(accepted(build(name.into())))
    }

    /// Validity of a nested column belongs to its group, as in Iceberg.
    fn admits_null_test(data_type: &DataType) -> bool {
        !data_type.is_nested()
    }

    fn admits_nan_test(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Float32 | DataType::Float64)
    }

    fn admits_boolean_test(data_type: &DataType) -> bool {
        matches!(data_type, DataType::Boolean)
    }
}

impl<'predicate>
    PredicatePlanner<RuntimePredicateScalar<'predicate>, RuntimeComparisonOperator>
    for RuntimeNodePlanner<'_>
{
    type Predicate = BoundNode;
    type Error = ConnectorError;

    fn always_true(&self) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        Ok(PredicatePlan::Exact(BoundNode::AlwaysTrue))
    }

    fn always_false(&self) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        Ok(PredicatePlan::Exact(BoundNode::NeverTrue))
    }

    fn strict_true(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        self.column_test(
            value,
            Self::admits_null_test,
            BoundNode::IsNotNull,
            PredicatePlan::ExactNoComplement,
        )
    }

    fn strict_false(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        self.column_test(
            value,
            |_| true,
            |_| BoundNode::NeverTrue,
            PredicatePlan::ExactNoComplement,
        )
    }

    fn comparison(
        &self,
        operator: &RuntimeComparisonOperator,
        left: &RuntimePredicateScalar<'predicate>,
        right: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        self.comparison_plan(*operator, left, right)
    }

    fn boolean_test(
        &self,
        kind: BooleanTestKind,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        // A boolean test never yields NULL, so `BoundNode::negated` flips
        // its kind as an exact complement.
        self.column_test(
            value,
            Self::admits_boolean_test,
            |column| BoundNode::BooleanTest { kind, column },
            PredicatePlan::Exact,
        )
    }

    fn is_null(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        self.column_test(
            value,
            Self::admits_null_test,
            BoundNode::IsNull,
            PredicatePlan::Exact,
        )
    }

    fn is_not_null(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        self.column_test(
            value,
            Self::admits_null_test,
            BoundNode::IsNotNull,
            PredicatePlan::Exact,
        )
    }

    fn is_nan(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        // Both NaN tests exclude NULL rows, so `BoundNode::negated` swaps
        // them as exact SQL complements.
        self.column_test(
            value,
            Self::admits_nan_test,
            BoundNode::IsNan,
            PredicatePlan::Exact,
        )
    }

    fn is_not_nan(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        self.column_test(
            value,
            Self::admits_nan_test,
            BoundNode::IsNotNan,
            PredicatePlan::Exact,
        )
    }

    fn starts_with(
        &self,
        value: &RuntimePredicateScalar<'predicate>,
        prefix: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        let RuntimePredicateScalar::Value(RuntimePredicateValue::String(prefix)) =
            prefix
        else {
            return Ok(PredicatePlan::Unsupported);
        };
        self.column_test(
            value,
            |data_type| matches!(data_type, DataType::Utf8),
            |column| BoundNode::StartsWith {
                column,
                prefix: BoundValue::String(prefix.as_ref().into()),
            },
            PredicatePlan::ExactNoComplement,
        )
    }

    // Query offload keeps `IN` in the query engine, as for Iceberg.
    fn in_set(
        &self,
        _operator: &RuntimeComparisonOperator,
        _value: &RuntimePredicateScalar<'predicate>,
        _set: &RuntimePredicateScalar<'predicate>,
    ) -> Result<PredicatePlan<BoundNode>, Self::Error> {
        Ok(PredicatePlan::Unsupported)
    }

    fn conjunction(&self, children: Vec<BoundNode>) -> BoundNode {
        BoundNode::And(children.into_boxed_slice())
    }

    fn disjunction(&self, children: Vec<BoundNode>) -> BoundNode {
        BoundNode::Or(children.into_boxed_slice())
    }

    fn negate(&self, predicate: BoundNode) -> BoundNode {
        predicate.negated()
    }
}
