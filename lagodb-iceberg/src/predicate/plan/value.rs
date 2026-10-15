//! Persisted runtime-value conversions, including source timestamp precision.

use iceberg_lite::spec::{PrimitiveType, Type};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::tuple::Decimal128Semantics;

use super::super::error::IcebergFilterError;
use super::super::temporal::TimestampPrecision;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlannedValueType {
    Boolean,
    Uuid,
    Binary,
    FixedBinary(u64),
    Int2ToLong,
    Int4ToLong,
    Int2,
    Int4,
    Int8,
    Date,
    Timestamp(TimestampPrecision),
    Timestamptz(TimestampPrecision),
    String,
    Decimal128(Decimal128Semantics),
    Int8ToInt,
}

impl PlannedValueType {
    /// Resolve storage precision once, while the source schema is available.
    pub(crate) fn with_source_type(self, source: &Type) -> Option<Self> {
        match (self, source) {
            (Self::Binary, Type::Primitive(PrimitiveType::Fixed(width))) => {
                Some(Self::FixedBinary(*width))
            }
            (Self::Binary, Type::Primitive(PrimitiveType::Binary)) => Some(self),
            (Self::Uuid, Type::Primitive(PrimitiveType::Uuid)) => Some(self),
            (Self::Binary | Self::Uuid, _) => None,
            (Self::Timestamp(_), Type::Primitive(PrimitiveType::Timestamp)) => {
                Some(Self::Timestamp(TimestampPrecision::Microseconds))
            }
            (Self::Timestamp(_), Type::Primitive(PrimitiveType::TimestampNs)) => {
                Some(Self::Timestamp(TimestampPrecision::Nanoseconds))
            }
            (Self::Timestamptz(_), Type::Primitive(PrimitiveType::Timestamptz)) => {
                Some(Self::Timestamptz(TimestampPrecision::Microseconds))
            }
            (Self::Timestamptz(_), Type::Primitive(PrimitiveType::TimestamptzNs)) => {
                Some(Self::Timestamptz(TimestampPrecision::Nanoseconds))
            }
            (Self::Timestamp(_) | Self::Timestamptz(_), _) => None,
            _ => Some(self),
        }
    }

    pub(super) fn encode(self, writer: &mut PlanDataWriter) {
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
            Self::Decimal128(semantics) => writer
                .append_i32(VALUE_DECIMAL128)
                .append_i32(i32::from(semantics.precision()))
                .append_i32(i32::from(semantics.scale())),
            Self::Int8ToInt => writer.append_i32(VALUE_INT8_TO_INT),
        };
    }

    pub(super) fn decode(
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
            tag => Err(IcebergFilterError::UnknownValueTypeTag(tag)),
        }
    }
}
