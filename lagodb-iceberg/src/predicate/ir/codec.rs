//! Wire serialization (`copyObject` plan data) for planned Iceberg predicates.

use lagodb_core::datum::Decimal128Semantics;
use lagodb_core::expr::{
    BooleanTestKind, PgBpcharSemantics, PgComparisonKind, RuntimeValueId,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};

use crate::predicate::error::IcebergFilterError;
use crate::predicate::semantics::TimestampPrecision;

use super::node::{PlannedColumn, PlannedNode, PlannedValueType};

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
const NODE_IN_SET: i32 = 12;

const OP_EQ: i32 = 0;
const OP_NOT_EQ: i32 = 1;
const OP_LT: i32 = 2;
const OP_LE: i32 = 3;
const OP_GT: i32 = 4;
const OP_GE: i32 = 5;

const VALUE_INT2: i32 = 0;
const VALUE_INT4: i32 = 1;
const VALUE_INT8: i32 = 2;
const VALUE_DATE: i32 = 3;
const VALUE_TIMESTAMP: i32 = 4;
const VALUE_TIMESTAMPTZ: i32 = 5;
const VALUE_STRING: i32 = 6;
const VALUE_DECIMAL128: i32 = 7;
const VALUE_INT8_TO_INT: i32 = 8;
const VALUE_TIMESTAMP_NS: i32 = 9;
const VALUE_TIMESTAMPTZ_NS: i32 = 10;
const VALUE_BOOLEAN: i32 = 11;
const VALUE_UUID: i32 = 12;
const VALUE_BINARY: i32 = 13;
const VALUE_FIXED_BINARY: i32 = 14;
const VALUE_INT2_TO_LONG: i32 = 15;
const VALUE_INT4_TO_LONG: i32 = 16;
const VALUE_FLOAT: i32 = 17;
const VALUE_DOUBLE: i32 = 18;
const VALUE_FLOAT_TO_DOUBLE: i32 = 19;
const VALUE_DOUBLE_TO_FLOAT: i32 = 20;
const VALUE_BPCHAR: i32 = 21;

impl PlannedNode {
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
                    .append_i32(encode_operator(*operator));
                column.encode(writer);
                writer.append_count(value.index());
                value_type.encode_plan(writer);
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
            Self::InSet {
                column,
                set,
                value_type,
            } => {
                writer.append_i32(NODE_IN_SET);
                column.encode(writer);
                writer.append_count(set.index());
                value_type.encode_plan(writer);
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

    pub(super) fn decode(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self, IcebergFilterError> {
        match reader.read_i32()? {
            NODE_ALWAYS_TRUE => Ok(Self::AlwaysTrue),
            NODE_ALWAYS_FALSE => Ok(Self::AlwaysFalse),
            NODE_COMPARISON => {
                let operator = decode_operator(reader.read_i32()?)?;
                let column = PlannedColumn::decode(reader)?;
                let value = decode_slot(reader, binding_count)?;
                let value_type = PlannedValueType::decode_plan(reader)?;
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
                    column: PlannedColumn::decode(reader)?,
                })
            }
            NODE_IS_NULL => Ok(Self::IsNull(PlannedColumn::decode(reader)?)),
            NODE_IS_NOT_NULL => Ok(Self::IsNotNull(PlannedColumn::decode(reader)?)),
            NODE_IS_NAN => Ok(Self::IsNan(PlannedColumn::decode(reader)?)),
            NODE_IS_NOT_NAN => Ok(Self::IsNotNan(PlannedColumn::decode(reader)?)),
            NODE_STARTS_WITH => {
                let column = PlannedColumn::decode(reader)?;
                let prefix = decode_slot(reader, binding_count)?;
                Ok(Self::StartsWith { column, prefix })
            }
            NODE_IN_SET => {
                let column = PlannedColumn::decode(reader)?;
                let set = decode_slot(reader, binding_count)?;
                let value_type = PlannedValueType::decode_plan(reader)?;
                Ok(Self::InSet {
                    column,
                    set,
                    value_type,
                })
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

impl PlannedColumn {
    fn encode(&self, writer: &mut PlanDataWriter) {
        writer
            .append_i32(self.field_id)
            .append_str(&self.display_name);
    }

    fn decode(reader: &mut PlanDataReader<'_>) -> Result<Self, IcebergFilterError> {
        Ok(Self {
            field_id: reader.read_i32()?,
            display_name: reader.read_str()?,
        })
    }
}

impl PlannedValueType {
    fn encode_plan(self, writer: &mut PlanDataWriter) {
        match self {
            Self::Boolean => writer.append_i32(VALUE_BOOLEAN),
            Self::Uuid => writer.append_i32(VALUE_UUID),
            Self::Binary => writer.append_i32(VALUE_BINARY),
            Self::FixedBinary(width) => writer
                .append_i32(VALUE_FIXED_BINARY)
                .append_count(width as usize),
            Self::Int2ToLong => writer.append_i32(VALUE_INT2_TO_LONG),
            Self::Int4ToLong => writer.append_i32(VALUE_INT4_TO_LONG),
            Self::Int2 => writer.append_i32(VALUE_INT2),
            Self::Int4 => writer.append_i32(VALUE_INT4),
            Self::Int8 => writer.append_i32(VALUE_INT8),
            Self::Date => writer.append_i32(VALUE_DATE),
            Self::Timestamp(TimestampPrecision::Microseconds) => {
                writer.append_i32(VALUE_TIMESTAMP)
            }
            Self::Timestamp(TimestampPrecision::Nanoseconds) => {
                writer.append_i32(VALUE_TIMESTAMP_NS)
            }
            Self::Timestamptz(TimestampPrecision::Microseconds) => {
                writer.append_i32(VALUE_TIMESTAMPTZ)
            }
            Self::Timestamptz(TimestampPrecision::Nanoseconds) => {
                writer.append_i32(VALUE_TIMESTAMPTZ_NS)
            }
            Self::String => writer.append_i32(VALUE_STRING),
            Self::Bpchar(semantics) => writer
                .append_i32(VALUE_BPCHAR)
                .append_i32(semantics.typmod()),
            Self::Decimal128(semantics) => writer
                .append_i32(VALUE_DECIMAL128)
                .append_i32(i32::from(semantics.precision()))
                .append_i32(i32::from(semantics.scale())),
            Self::Int8ToInt => writer.append_i32(VALUE_INT8_TO_INT),
            Self::Float => writer.append_i32(VALUE_FLOAT),
            Self::Double => writer.append_i32(VALUE_DOUBLE),
            Self::FloatToDouble => writer.append_i32(VALUE_FLOAT_TO_DOUBLE),
            Self::DoubleToFloat => writer.append_i32(VALUE_DOUBLE_TO_FLOAT),
        };
    }

    fn decode_plan(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self, IcebergFilterError> {
        match reader.read_i32()? {
            VALUE_BOOLEAN => Ok(Self::Boolean),
            VALUE_UUID => Ok(Self::Uuid),
            VALUE_BINARY => Ok(Self::Binary),
            VALUE_FIXED_BINARY => Ok(Self::FixedBinary(reader.read_count()? as u64)),
            VALUE_INT2_TO_LONG => Ok(Self::Int2ToLong),
            VALUE_INT4_TO_LONG => Ok(Self::Int4ToLong),
            VALUE_INT2 => Ok(Self::Int2),
            VALUE_INT4 => Ok(Self::Int4),
            VALUE_INT8 => Ok(Self::Int8),
            VALUE_DATE => Ok(Self::Date),
            VALUE_TIMESTAMP => Ok(Self::Timestamp(TimestampPrecision::Microseconds)),
            VALUE_TIMESTAMP_NS => {
                Ok(Self::Timestamp(TimestampPrecision::Nanoseconds))
            }
            VALUE_TIMESTAMPTZ => {
                Ok(Self::Timestamptz(TimestampPrecision::Microseconds))
            }
            VALUE_TIMESTAMPTZ_NS => {
                Ok(Self::Timestamptz(TimestampPrecision::Nanoseconds))
            }
            VALUE_STRING => Ok(Self::String),
            VALUE_BPCHAR => {
                let typmod = reader.read_i32()?;
                PgBpcharSemantics::for_typmod(typmod)
                    .map(Self::Bpchar)
                    .ok_or(IcebergFilterError::InvalidBpcharTypmod(typmod))
            }
            VALUE_DECIMAL128 => {
                let encoded_precision = reader.read_i32()?;
                let encoded_scale = reader.read_i32()?;
                let semantics = u8::try_from(encoded_precision)
                    .ok()
                    .and_then(|precision| {
                        let scale = i8::try_from(encoded_scale).ok()?;
                        Decimal128Semantics::new(precision, scale)
                    })
                    .ok_or(IcebergFilterError::InvalidDecimalShape)?;
                Ok(Self::Decimal128(semantics))
            }
            VALUE_INT8_TO_INT => Ok(Self::Int8ToInt),
            VALUE_FLOAT => Ok(Self::Float),
            VALUE_DOUBLE => Ok(Self::Double),
            VALUE_FLOAT_TO_DOUBLE => Ok(Self::FloatToDouble),
            VALUE_DOUBLE_TO_FLOAT => Ok(Self::DoubleToFloat),
            tag => Err(IcebergFilterError::UnknownValueTypeTag(tag)),
        }
    }
}

fn decode_slot(
    reader: &mut PlanDataReader<'_>,
    binding_count: usize,
) -> Result<RuntimeValueId, IcebergFilterError> {
    let index = reader.read_count()?;
    RuntimeValueId::from_plan_data(index, binding_count).ok_or(
        IcebergFilterError::BindingSlotOutOfBounds {
            index,
            binding_count,
        },
    )
}

const fn encode_operator(operator: PgComparisonKind) -> i32 {
    match operator {
        PgComparisonKind::Equal => OP_EQ,
        PgComparisonKind::NotEqual => OP_NOT_EQ,
        PgComparisonKind::Less => OP_LT,
        PgComparisonKind::LessEqual => OP_LE,
        PgComparisonKind::Greater => OP_GT,
        PgComparisonKind::GreaterEqual => OP_GE,
    }
}

fn decode_operator(tag: i32) -> Result<PgComparisonKind, IcebergFilterError> {
    match tag {
        OP_EQ => Ok(PgComparisonKind::Equal),
        OP_NOT_EQ => Ok(PgComparisonKind::NotEqual),
        OP_LT => Ok(PgComparisonKind::Less),
        OP_LE => Ok(PgComparisonKind::LessEqual),
        OP_GT => Ok(PgComparisonKind::Greater),
        OP_GE => Ok(PgComparisonKind::GreaterEqual),
        tag => Err(IcebergFilterError::UnknownOperatorTag(tag)),
    }
}
