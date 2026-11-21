//! Runtime value binding of planned Parquet predicates.
//!
//! Negation is carried to the leaves. A strict comparison with NULL is
//! UNKNOWN, whose truth set stays empty under NOT, so NULL folds to
//! `NeverTrue` only after the leaf has absorbed any enclosing negation.

use lagodb_core::datum::ArrayDatumLayout;
use lagodb_core::expr::{RuntimeValue, RuntimeValueBindings};

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::bound::BoundNode;
use super::operator::ComparisonOperator;
use super::plan::{PlannedColumn, PlannedNode};
use super::value::{BoundValue, DecodedValue, ValueType};

pub(super) struct ParquetFilterBinder<'a> {
    values: RuntimeValueBindings<'a>,
}

impl<'a> ParquetFilterBinder<'a> {
    pub(super) const fn new(values: RuntimeValueBindings<'a>) -> Self {
        Self { values }
    }

    pub(super) fn bind(
        &self,
        node: &PlannedNode,
        negated: bool,
    ) -> Result<BoundNode, ConnectorError> {
        Ok(match node {
            PlannedNode::AlwaysTrue if negated => BoundNode::NeverTrue,
            PlannedNode::AlwaysTrue => BoundNode::AlwaysTrue,
            PlannedNode::AlwaysFalse if negated => BoundNode::AlwaysTrue,
            PlannedNode::AlwaysFalse => BoundNode::NeverTrue,
            PlannedNode::Comparison {
                operator,
                column,
                value,
                value_type,
            } => {
                let operator = if negated {
                    operator.negated()
                } else {
                    *operator
                };
                Self::bind_comparison(
                    operator,
                    column,
                    *value_type,
                    self.values.value(*value),
                )?
            }
            PlannedNode::BooleanTest { kind, column } => BoundNode::BooleanTest {
                kind: if negated { kind.negated() } else { *kind },
                column: column.name.clone(),
            },
            PlannedNode::IsNull(column) | PlannedNode::IsNotNull(column) => {
                if matches!(node, PlannedNode::IsNull(_)) != negated {
                    BoundNode::IsNull(column.name.clone())
                } else {
                    BoundNode::IsNotNull(column.name.clone())
                }
            }
            PlannedNode::IsNan(column) | PlannedNode::IsNotNan(column) => {
                if matches!(node, PlannedNode::IsNan(_)) != negated {
                    BoundNode::IsNan(column.name.clone())
                } else {
                    BoundNode::IsNotNan(column.name.clone())
                }
            }
            // Prefix tests and sets plan without a complement, so planning
            // never places them below NOT.
            PlannedNode::StartsWith { .. } | PlannedNode::InSet { .. } if negated => {
                return Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet));
            }
            PlannedNode::StartsWith { column, prefix } => {
                let prefix = self.values.value(*prefix);
                if prefix.is_null() {
                    BoundNode::NeverTrue
                } else {
                    match unsafe { ValueType::String.decode(prefix) }? {
                        DecodedValue::Value(prefix @ BoundValue::String(_)) => {
                            BoundNode::StartsWith {
                                column: column.name.clone(),
                                prefix,
                            }
                        }
                        _ => {
                            return Err(ConnectorError::invalid_filter_plan(
                                FormatKind::Parquet,
                            ));
                        }
                    }
                }
            }
            PlannedNode::InSet {
                column,
                set,
                value_type,
            } => Self::bind_in_set(column, *value_type, self.values.value(*set))?,
            PlannedNode::And(children) => {
                let children = self.bind_children(children, negated)?;
                if negated {
                    BoundNode::Or(children)
                } else {
                    BoundNode::And(children)
                }
            }
            PlannedNode::Or(children) => {
                let children = self.bind_children(children, negated)?;
                if negated {
                    BoundNode::And(children)
                } else {
                    BoundNode::Or(children)
                }
            }
            PlannedNode::Not(child) => self.bind(child, !negated)?,
        })
    }

    fn bind_children(
        &self,
        children: &[PlannedNode],
        negated: bool,
    ) -> Result<Box<[BoundNode]>, ConnectorError> {
        children
            .iter()
            .map(|child| self.bind(child, negated))
            .collect()
    }

    /// `operator` already carries mirroring and any enclosing negation.
    fn bind_comparison(
        operator: ComparisonOperator,
        column: &PlannedColumn,
        value_type: ValueType,
        value: RuntimeValue,
    ) -> Result<BoundNode, ConnectorError> {
        if value.is_null() {
            return Ok(BoundNode::NeverTrue);
        }
        let column = &column.name;
        let matches_non_null = |matches: bool| {
            if matches {
                BoundNode::IsNotNull(column.clone())
            } else {
                BoundNode::NeverTrue
            }
        };
        // SAFETY: the planner admitted this value type for the slot's
        // metadata, and the binding callback keeps the datum live.
        Ok(match unsafe { value_type.decode(value) }? {
            DecodedValue::Value(value) => BoundNode::Comparison {
                operator,
                column: column.clone(),
                value,
            },
            DecodedValue::Float(value) => value.bind(column, operator),
            DecodedValue::OutsideFinite(value) => matches_non_null(
                value
                    .matches_finite_column(operator.kind())
                    .expect("outside-finite Decimal128 value"),
            ),
            DecodedValue::OutsideInteger(outside) => {
                matches_non_null(outside.comparison_matches_non_null(operator))
            }
            // Only `=` / `<>` plan for `character(n)`.
            DecodedValue::OutsideWidth => {
                matches_non_null(operator == ComparisonOperator::NotEq)
            }
            // Temporal comparisons plan Conservatively and are never negated,
            // so widening the unrepresentable leaf keeps the tree safe.
            DecodedValue::Unrepresentable => BoundNode::AlwaysTrue,
        })
    }

    fn bind_in_set(
        column: &PlannedColumn,
        value_type: ValueType,
        set: RuntimeValue,
    ) -> Result<BoundNode, ConnectorError> {
        // `x = ANY (NULL)` is UNKNOWN for every row.
        if set.is_null() {
            return Ok(BoundNode::NeverTrue);
        }
        let element = set.metadata().array_element().ok_or_else(|| {
            ConnectorError::invalid_filter_plan(FormatKind::Parquet)
        })?;
        // SAFETY: the planner admitted an array of this element type, and the
        // bound value outlives this call.
        let elements = unsafe {
            ArrayDatumLayout::bind(element.value_type.type_oid).values(set.datum())
        };
        let mut values = Vec::new();
        // A NULL element only turns a non-match into UNKNOWN, and sets are
        // never negated.
        for datum in elements.flatten() {
            // SAFETY: the iterator retains the array for this loop, and
            // decoding copies the element.
            let value = unsafe { RuntimeValue::from_raw(datum, false, element) };
            match unsafe { value_type.decode(value) }? {
                DecodedValue::Value(value) => values.push(value),
                // A value outside the column's domain matches no row.
                DecodedValue::OutsideInteger(_) | DecodedValue::OutsideWidth => {}
                // One unrepresentable element leaves the set to the residual.
                _ => return Ok(BoundNode::AlwaysTrue),
            }
        }
        Ok(BoundNode::InSet {
            column: column.name.clone(),
            values: values.into_boxed_slice(),
        })
    }
}
