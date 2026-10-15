//! Owned, copyObject-serializable Iceberg predicate plans.

use lagodb_core::expr::{BooleanTestKind, PgComparisonKind, RuntimeValueId};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};

mod value;

pub(crate) use value::PlannedValueType;

use super::error::IcebergFilterError;

const NODE_COMPARISON: i32 = 0;
const NODE_IS_NULL: i32 = 1;
const NODE_IS_NOT_NULL: i32 = 2;
const NODE_AND: i32 = 3;
const NODE_OR: i32 = 4;
const NODE_NOT: i32 = 5;
const NODE_ALWAYS_TRUE: i32 = 6;
const NODE_ALWAYS_FALSE: i32 = 7;
const NODE_IS_NAN: i32 = 8;
const NODE_IS_NOT_NAN: i32 = 9;
const NODE_STARTS_WITH: i32 = 10;
const NODE_BOOLEAN_TEST: i32 = 11;

const OP_EQ: i32 = 0;
const OP_NOT_EQ: i32 = 1;
const OP_LT: i32 = 2;
const OP_LE: i32 = 3;
const OP_GT: i32 = 4;
const OP_GE: i32 = 5;

#[derive(Debug)]
pub(crate) struct PlannedIcebergPredicate {
    schema_id: i32,
    root: PlannedIcebergNode,
}

impl PlannedIcebergPredicate {
    pub(crate) fn new(schema_id: i32, root: PlannedIcebergNode) -> Self {
        Self { schema_id, root }
    }

    #[inline]
    pub(crate) fn schema_id(&self) -> i32 {
        self.schema_id
    }

    #[inline]
    pub(crate) fn root(&self) -> &PlannedIcebergNode {
        &self.root
    }

    pub(crate) fn encode(&self, writer: &mut PlanDataWriter) {
        writer.append_i32(self.schema_id).append_nested(|root| {
            self.root.encode(root);
        });
    }

    pub(crate) fn decode(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self, IcebergFilterError> {
        let schema_id = reader.read_i32()?;
        let root_node = reader
            .read_nested(|root| PlannedIcebergNode::decode(root, binding_count))?;
        Ok(Self::new(schema_id, root_node))
    }
}

#[derive(Debug)]
pub(crate) enum PlannedIcebergNode {
    AlwaysTrue,
    AlwaysFalse,
    Comparison {
        operator: PlannedComparisonOperator,
        column: PlannedIcebergColumn,
        value: RuntimeValueId,
        value_type: PlannedValueType,
    },
    BooleanTest {
        kind: BooleanTestKind,
        column: PlannedIcebergColumn,
    },
    IsNull(PlannedIcebergColumn),
    IsNotNull(PlannedIcebergColumn),
    IsNan(PlannedIcebergColumn),
    IsNotNan(PlannedIcebergColumn),
    StartsWith {
        column: PlannedIcebergColumn,
        prefix: RuntimeValueId,
    },
    And(Box<[Self]>),
    Or(Box<[Self]>),
    Not(Box<Self>),
}

impl PlannedIcebergNode {
    fn encode(&self, writer: &mut PlanDataWriter) {
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
            Self::IsNull(column) => {
                writer.append_i32(NODE_IS_NULL);
                column.encode(writer);
            }
            Self::IsNotNull(column) => {
                writer.append_i32(NODE_IS_NOT_NULL);
                column.encode(writer);
            }
            Self::IsNan(column) => {
                writer.append_i32(NODE_IS_NAN);
                column.encode(writer);
            }
            Self::IsNotNan(column) => {
                writer.append_i32(NODE_IS_NOT_NAN);
                column.encode(writer);
            }
            Self::StartsWith { column, prefix } => {
                writer.append_i32(NODE_STARTS_WITH);
                column.encode(writer);
                writer.append_count(prefix.index());
            }
            Self::And(children) => {
                Self::encode_children(writer, NODE_AND, children);
            }
            Self::Or(children) => {
                Self::encode_children(writer, NODE_OR, children);
            }
            Self::Not(child) => {
                writer.append_i32(NODE_NOT).append_nested(|encoded| {
                    child.encode(encoded);
                });
            }
        }
    }

    fn encode_children(writer: &mut PlanDataWriter, tag: i32, children: &[Self]) {
        writer.append_i32(tag).append_count(children.len());
        for child in children {
            writer.append_nested(|encoded| child.encode(encoded));
        }
    }

    fn decode(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self, IcebergFilterError> {
        match reader.read_i32()? {
            NODE_ALWAYS_TRUE => Ok(Self::AlwaysTrue),
            NODE_ALWAYS_FALSE => Ok(Self::AlwaysFalse),
            NODE_COMPARISON => {
                let operator =
                    PlannedComparisonOperator::from_tag(reader.read_i32()?)?;
                let column = PlannedIcebergColumn::decode(reader)?;
                let index = reader.read_count()?;
                let value = RuntimeValueId::from_plan_data(index, binding_count)
                    .ok_or(IcebergFilterError::BindingSlotOutOfBounds {
                        index,
                        binding_count,
                    })?;
                let value_type = PlannedValueType::decode(reader)?;
                Ok(Self::Comparison {
                    operator,
                    column,
                    value,
                    value_type,
                })
            }
            NODE_BOOLEAN_TEST => {
                let tag = reader.read_i32()?;
                let kind = BooleanTestKind::from_wire_id(tag)
                    .ok_or(IcebergFilterError::UnknownBooleanTest(tag))?;
                Ok(Self::BooleanTest {
                    kind,
                    column: PlannedIcebergColumn::decode(reader)?,
                })
            }
            NODE_IS_NULL => Ok(Self::IsNull(PlannedIcebergColumn::decode(reader)?)),
            NODE_IS_NOT_NULL => {
                Ok(Self::IsNotNull(PlannedIcebergColumn::decode(reader)?))
            }
            NODE_IS_NAN => Ok(Self::IsNan(PlannedIcebergColumn::decode(reader)?)),
            NODE_IS_NOT_NAN => {
                Ok(Self::IsNotNan(PlannedIcebergColumn::decode(reader)?))
            }
            NODE_STARTS_WITH => {
                let column = PlannedIcebergColumn::decode(reader)?;
                let index = reader.read_count()?;
                let prefix = RuntimeValueId::from_plan_data(index, binding_count)
                    .ok_or(IcebergFilterError::BindingSlotOutOfBounds {
                        index,
                        binding_count,
                    })?;
                Ok(Self::StartsWith { column, prefix })
            }
            NODE_AND => Ok(Self::And(Self::decode_children(
                reader,
                binding_count,
                "AND",
            )?)),
            NODE_OR => Ok(Self::Or(Self::decode_children(
                reader,
                binding_count,
                "OR",
            )?)),
            NODE_NOT => {
                let node =
                    reader.read_nested(|child| Self::decode(child, binding_count))?;
                Ok(Self::Not(Box::new(node)))
            }
            tag => Err(IcebergFilterError::UnknownNodeTag(tag)),
        }
    }

    fn decode_children(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
        kind: &'static str,
    ) -> Result<Box<[Self]>, IcebergFilterError> {
        let count = reader.read_count()?;
        if count == 0 {
            return Err(IcebergFilterError::EmptyLogicalNode { kind });
        }
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            children.push(
                reader.read_nested(|child| Self::decode(child, binding_count))?,
            );
        }
        Ok(children.into_boxed_slice())
    }
}

#[derive(Debug)]
pub(crate) struct PlannedIcebergColumn {
    pub(crate) field_id: i32,
    pub(crate) debug_name: String,
}

impl PlannedIcebergColumn {
    fn encode(&self, writer: &mut PlanDataWriter) {
        writer
            .append_i32(self.field_id)
            .append_str(&self.debug_name);
    }

    fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, IcebergFilterError> {
        Ok(Self {
            field_id: reader.read_i32()?,
            debug_name: reader.read_str()?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PlannedComparisonOperator {
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl PlannedComparisonOperator {
    #[inline]
    pub(crate) const fn kind(self) -> PgComparisonKind {
        match self {
            Self::Eq => PgComparisonKind::Equal,
            Self::NotEq => PgComparisonKind::NotEqual,
            Self::Lt => PgComparisonKind::Less,
            Self::Le => PgComparisonKind::LessEqual,
            Self::Gt => PgComparisonKind::Greater,
            Self::Ge => PgComparisonKind::GreaterEqual,
        }
    }

    pub(crate) const fn explain_symbol(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        }
    }

    #[inline]
    pub(crate) const fn mirrored(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::Le => Self::Ge,
            Self::Gt => Self::Lt,
            Self::Ge => Self::Le,
            Self::Eq | Self::NotEq => self,
        }
    }

    #[inline]
    pub(crate) const fn negated(self) -> Self {
        match self {
            Self::Eq => Self::NotEq,
            Self::NotEq => Self::Eq,
            Self::Lt => Self::Ge,
            Self::Le => Self::Gt,
            Self::Gt => Self::Le,
            Self::Ge => Self::Lt,
        }
    }

    const fn tag(self) -> i32 {
        match self {
            Self::Eq => OP_EQ,
            Self::NotEq => OP_NOT_EQ,
            Self::Lt => OP_LT,
            Self::Le => OP_LE,
            Self::Gt => OP_GT,
            Self::Ge => OP_GE,
        }
    }

    fn from_tag(tag: i32) -> Result<Self, IcebergFilterError> {
        match tag {
            OP_EQ => Ok(Self::Eq),
            OP_NOT_EQ => Ok(Self::NotEq),
            OP_LT => Ok(Self::Lt),
            OP_LE => Ok(Self::Le),
            OP_GT => Ok(Self::Gt),
            OP_GE => Ok(Self::Ge),
            tag => Err(IcebergFilterError::UnknownOperatorTag(tag)),
        }
    }
}
