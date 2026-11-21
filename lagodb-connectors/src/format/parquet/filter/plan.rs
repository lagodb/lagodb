//! Plan-persisted Parquet predicate trees.

use std::convert::Infallible;

use lagodb_core::expr::pushdown::{FilterEvaluation, PredicatePlan};
use lagodb_core::expr::{BooleanTestKind, RuntimeValueId};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::operator::ComparisonOperator;
use super::value::ValueType;

const NODE_COMPARISON: i32 = 0;
const NODE_IS_NULL: i32 = 1;
const NODE_IS_NOT_NULL: i32 = 2;
const NODE_AND: i32 = 3;
const NODE_OR: i32 = 4;
const NODE_NOT: i32 = 5;
const NODE_ALWAYS_TRUE: i32 = 6;
const NODE_ALWAYS_FALSE: i32 = 7;
const NODE_BOOLEAN_TEST: i32 = 8;
const NODE_IS_NAN: i32 = 9;
const NODE_IS_NOT_NAN: i32 = 10;
const NODE_STARTS_WITH: i32 = 11;
const NODE_IN_SET: i32 = 12;

/// A relation column; Parquet columns are matched by name.
#[derive(Clone)]
pub(super) struct PlannedColumn {
    pub(super) attno: pg_sys::AttrNumber,
    pub(super) name: Box<str>,
}

impl PlannedColumn {
    fn encode(&self, writer: &mut PlanDataWriter) {
        writer
            .append_i32(i32::from(self.attno))
            .append_str(&self.name);
    }

    fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, ConnectorError> {
        let attno = reader.read_i32()?;
        if attno <= 0 || attno > pg_sys::MaxHeapAttributeNumber as i32 {
            return Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet));
        }
        Ok(Self {
            attno: attno as pg_sys::AttrNumber,
            name: reader.read_str()?.into(),
        })
    }
}

pub(super) enum PlannedNode {
    AlwaysTrue,
    AlwaysFalse,
    Comparison {
        operator: ComparisonOperator,
        column: PlannedColumn,
        value: RuntimeValueId,
        value_type: ValueType,
    },
    BooleanTest {
        kind: BooleanTestKind,
        column: PlannedColumn,
    },
    IsNull(PlannedColumn),
    IsNotNull(PlannedColumn),
    IsNan(PlannedColumn),
    IsNotNan(PlannedColumn),
    StartsWith {
        column: PlannedColumn,
        prefix: RuntimeValueId,
    },
    /// `column = ANY (set)`, used only to prune row groups.
    InSet {
        column: PlannedColumn,
        set: RuntimeValueId,
        value_type: ValueType,
    },
    And(Box<[Self]>),
    Or(Box<[Self]>),
    Not(Box<Self>),
}

impl PlannedNode {
    /// What of this filter the reader evaluates per row: all of it, the part
    /// [`Self::row_filter`] keeps, or nothing. Only sets stay out.
    pub(super) fn evaluation(&self) -> FilterEvaluation {
        let Ok(plan) = self.row_filter::<(), Infallible>(
            &|_| Ok(PredicatePlan::Exact(())),
            |_| (),
            |_| (),
        );
        match plan {
            PredicatePlan::Unsupported => FilterEvaluation::PruningOnly,
            PredicatePlan::Partial(()) => FilterEvaluation::Projected,
            PredicatePlan::Exact(())
            | PredicatePlan::ExactNoComplement(())
            | PredicatePlan::Conservative(()) => FilterEvaluation::PerRow,
        }
    }

    /// The tree the reader evaluates on each row, with `leaf` planning every
    /// leaf but a set, by the shared AND-widening and complete-OR rules. Only
    /// AND and OR can hold a set: negation refuses Conservative plans. The
    /// bound tree projects identically (`ParquetNode::row_filter`).
    pub(super) fn row_filter<T, E>(
        &self,
        leaf: &impl Fn(&Self) -> Result<PredicatePlan<T>, E>,
        conjunction: fn(Vec<T>) -> T,
        disjunction: fn(Vec<T>) -> T,
    ) -> Result<PredicatePlan<T>, E> {
        let children = |nodes: &[Self]| {
            nodes
                .iter()
                .map(|child| child.row_filter(leaf, conjunction, disjunction))
                .collect::<Result<Vec<_>, E>>()
        };
        Ok(match self {
            Self::InSet { .. } => PredicatePlan::Unsupported,
            Self::And(nodes) => {
                PredicatePlan::conjunction(children(nodes)?, conjunction)
            }
            Self::Or(nodes) => {
                PredicatePlan::disjunction(children(nodes)?, disjunction)
            }
            node => leaf(node)?,
        })
    }

    pub(super) fn encode(&self, writer: &mut PlanDataWriter) {
        match self {
            Self::AlwaysTrue => {
                writer.append_i32(NODE_ALWAYS_TRUE);
            }
            Self::AlwaysFalse => {
                writer.append_i32(NODE_ALWAYS_FALSE);
            }
            Self::Comparison {
                operator,
                column,
                value,
                value_type,
            } => {
                writer
                    .append_i32(NODE_COMPARISON)
                    .append_i32(operator.tag());
                column.encode(writer);
                writer.append_count(value.index());
                value_type.encode(writer);
            }
            Self::BooleanTest { kind, column } => {
                writer
                    .append_i32(NODE_BOOLEAN_TEST)
                    .append_i32(kind.wire_id());
                column.encode(writer);
            }
            Self::IsNull(column) => Self::encode_column(writer, NODE_IS_NULL, column),
            Self::IsNotNull(column) => {
                Self::encode_column(writer, NODE_IS_NOT_NULL, column)
            }
            Self::IsNan(column) => Self::encode_column(writer, NODE_IS_NAN, column),
            Self::IsNotNan(column) => {
                Self::encode_column(writer, NODE_IS_NOT_NAN, column)
            }
            Self::StartsWith { column, prefix } => {
                writer.append_i32(NODE_STARTS_WITH);
                column.encode(writer);
                writer.append_count(prefix.index());
            }
            Self::InSet {
                column,
                set,
                value_type,
            } => {
                writer.append_i32(NODE_IN_SET);
                column.encode(writer);
                writer.append_count(set.index());
                value_type.encode(writer);
            }
            Self::And(children) => Self::encode_children(writer, NODE_AND, children),
            Self::Or(children) => Self::encode_children(writer, NODE_OR, children),
            Self::Not(child) => {
                writer
                    .append_i32(NODE_NOT)
                    .append_nested(|nested| child.encode(nested));
            }
        }
    }

    fn encode_column(writer: &mut PlanDataWriter, tag: i32, column: &PlannedColumn) {
        writer.append_i32(tag);
        column.encode(writer);
    }

    fn encode_children(writer: &mut PlanDataWriter, tag: i32, children: &[Self]) {
        writer.append_i32(tag).append_count(children.len());
        for child in children {
            writer.append_nested(|nested| child.encode(nested));
        }
    }

    pub(super) fn decode(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self, ConnectorError> {
        let invalid = || ConnectorError::invalid_filter_plan(FormatKind::Parquet);
        let value_id =
            |reader: &mut PlanDataReader<'_>| -> Result<_, ConnectorError> {
                let index = reader.read_count()?;
                RuntimeValueId::from_plan_data(index, binding_count)
                    .ok_or_else(invalid)
            };
        Ok(match reader.read_i32()? {
            NODE_ALWAYS_TRUE => Self::AlwaysTrue,
            NODE_ALWAYS_FALSE => Self::AlwaysFalse,
            NODE_COMPARISON => {
                let operator = ComparisonOperator::from_tag(reader.read_i32()?)?;
                let column = PlannedColumn::decode(reader)?;
                let value = value_id(reader)?;
                let value_type = ValueType::decode_plan(reader)?;
                if !value_type.accepts_operator(operator) {
                    return Err(invalid());
                }
                Self::Comparison {
                    operator,
                    column,
                    value,
                    value_type,
                }
            }
            NODE_BOOLEAN_TEST => Self::BooleanTest {
                kind: BooleanTestKind::from_wire_id(reader.read_i32()?)
                    .ok_or_else(invalid)?,
                column: PlannedColumn::decode(reader)?,
            },
            NODE_IS_NULL => Self::IsNull(PlannedColumn::decode(reader)?),
            NODE_IS_NOT_NULL => Self::IsNotNull(PlannedColumn::decode(reader)?),
            NODE_IS_NAN => Self::IsNan(PlannedColumn::decode(reader)?),
            NODE_IS_NOT_NAN => Self::IsNotNan(PlannedColumn::decode(reader)?),
            NODE_STARTS_WITH => Self::StartsWith {
                column: PlannedColumn::decode(reader)?,
                prefix: value_id(reader)?,
            },
            NODE_IN_SET => Self::InSet {
                column: PlannedColumn::decode(reader)?,
                set: value_id(reader)?,
                value_type: ValueType::decode_plan(reader)?,
            },
            NODE_AND => Self::And(Self::decode_children(reader, binding_count)?),
            NODE_OR => Self::Or(Self::decode_children(reader, binding_count)?),
            NODE_NOT => Self::Not(Box::new(
                reader.read_nested(|nested| Self::decode(nested, binding_count))?,
            )),
            _ => return Err(invalid()),
        })
    }

    fn decode_children(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Box<[Self]>, ConnectorError> {
        let count = reader.read_count()?;
        if count == 0 {
            return Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet));
        }
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            children.push(
                reader.read_nested(|nested| Self::decode(nested, binding_count))?,
            );
        }
        Ok(children.into_boxed_slice())
    }
}
