//! Source binding and materialization for native scalar and primitive-array codecs.

use arrow_schema::FieldRef;
use pgrx::pg_sys;

use super::native_list::{
    BoundBoolArrayEncoder, BoundBpcharArrayEncoder, BoundFloat4ArrayEncoder,
    BoundFloat8ArrayEncoder, BoundInt2ArrayEncoder, BoundInt4ArrayEncoder,
    BoundInt8ArrayEncoder, BoundJsonArrayEncoder, BoundNameArrayEncoder,
    BoundShortArrayEncoder, BoundTextArrayEncoder, BoundVarcharArrayEncoder,
};
use crate::column_encoder::binary::{BinaryEncoder, FixedBinaryEncoder, UuidEncoder};
use crate::column_encoder::decimal::Decimal128Encoder;
use crate::column_encoder::primitive::{BoolEncoder, PrimitiveEncoder};
use crate::column_encoder::string::Utf8Encoder;
use crate::column_encoder::timestamp::{
    BoundTimestampMicrosEncoder, BoundTimestampNanosEncoder,
};
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::rule::{CellRule, ListElementRule};

use super::DatumEncoder;

/// A validated relation-bound source plan without Arrow builder state.
///
/// This is the single source of truth for the `(CellRule, PostgreSQL OID)`
/// compatibility relation. The runtime encoder is materialized only after
/// relation-wide capabilities, such as server encoding, have been checked.
pub(crate) enum NativeEncoderPlan {
    Bool,
    I16,
    I32Int2,
    I32Int4,
    I64Int8,
    F32,
    F64Float8,
    Text,
    Name,
    Bytea,
    FixedBytea { len: usize },
    Uuid,
    Numeric { precision: u32, scale: u32 },
    Date,
    Time,
    Timestamp { nanos: bool },
    Timestamptz { nanos: bool },
    BoolArray { field: FieldRef },
    ShortArray { field: FieldRef },
    Int2Array { field: FieldRef },
    Int4Array { field: FieldRef },
    Int8Array { field: FieldRef },
    Float4Array { field: FieldRef },
    Float8Array { field: FieldRef },
    TextArray { field: FieldRef },
    VarcharArray { field: FieldRef },
    BpcharArray { field: FieldRef },
    NameArray { field: FieldRef },
    JsonArray { field: FieldRef },
}

impl NativeEncoderPlan {
    /// Resolve one PostgreSQL source OID against the already-resolved output
    /// rule. A successful plan contains all information needed by the runtime
    /// encoder and no longer retains the general `CellRule`.
    pub(crate) fn bind(
        rule: CellRule,
        oid: pg_sys::Oid,
    ) -> ArrowConversionResult<Self> {
        let plan = match (rule, oid) {
            (CellRule::Bool, pg_sys::BOOLOID) => Self::Bool,
            (CellRule::I16, pg_sys::INT2OID) => Self::I16,
            (CellRule::I32, pg_sys::INT2OID) => Self::I32Int2,
            (CellRule::I32, pg_sys::INT4OID) => Self::I32Int4,
            (CellRule::I64, pg_sys::INT8OID) => Self::I64Int8,
            (CellRule::F32, pg_sys::FLOAT4OID) => Self::F32,
            (CellRule::F64, pg_sys::FLOAT8OID) => Self::F64Float8,
            (CellRule::Utf8, oid)
                if oid == pg_sys::TEXTOID
                    || oid == pg_sys::VARCHAROID
                    || oid == pg_sys::BPCHAROID
                    || oid == pg_sys::JSONOID =>
            {
                Self::Text
            }
            (CellRule::Utf8, pg_sys::NAMEOID) => Self::Name,
            (CellRule::Binary, pg_sys::BYTEAOID) => Self::Bytea,
            (CellRule::FixedBinary { len }, pg_sys::BYTEAOID) => {
                Self::FixedBytea { len }
            }
            (CellRule::Uuid, pg_sys::UUIDOID) => Self::Uuid,
            (CellRule::Decimal128 { precision, scale }, pg_sys::NUMERICOID) => {
                Self::Numeric { precision, scale }
            }
            (CellRule::Date32, pg_sys::DATEOID) => Self::Date,
            (CellRule::Time64Micros, pg_sys::TIMEOID) => Self::Time,
            (CellRule::Timestamp { nanos, tz: false }, pg_sys::TIMESTAMPOID) => {
                Self::Timestamp { nanos }
            }
            (CellRule::Timestamp { nanos, tz: true }, pg_sys::TIMESTAMPTZOID) => {
                Self::Timestamptz { nanos }
            }
            (
                CellRule::List {
                    element: ListElementRule::Bool,
                    field,
                    ..
                },
                pg_sys::BOOLARRAYOID,
            ) => Self::BoolArray { field },
            (
                CellRule::List {
                    element: ListElementRule::Short,
                    field,
                    ..
                },
                pg_sys::INT2ARRAYOID,
            ) => Self::ShortArray { field },
            (
                CellRule::List {
                    element: ListElementRule::Int,
                    field,
                    ..
                },
                pg_sys::INT2ARRAYOID,
            ) => Self::Int2Array { field },
            (
                CellRule::List {
                    element: ListElementRule::Int,
                    field,
                    ..
                },
                pg_sys::INT4ARRAYOID,
            ) => Self::Int4Array { field },
            (
                CellRule::List {
                    element: ListElementRule::Long,
                    field,
                    ..
                },
                pg_sys::INT8ARRAYOID,
            ) => Self::Int8Array { field },
            (
                CellRule::List {
                    element: ListElementRule::Float,
                    field,
                    ..
                },
                pg_sys::FLOAT4ARRAYOID,
            ) => Self::Float4Array { field },
            (
                CellRule::List {
                    element: ListElementRule::Double,
                    field,
                    ..
                },
                pg_sys::FLOAT8ARRAYOID,
            ) => Self::Float8Array { field },
            (
                CellRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::TEXTARRAYOID,
            ) => Self::TextArray { field },
            (
                CellRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::VARCHARARRAYOID,
            ) => Self::VarcharArray { field },
            (
                CellRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::BPCHARARRAYOID,
            ) => Self::BpcharArray { field },
            (
                CellRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::NAMEARRAYOID,
            ) => Self::NameArray { field },
            (
                CellRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::JSONARRAYOID,
            ) => Self::JsonArray { field },
            _ => {
                return Err(ArrowConversionError::InvariantViolated(
                    "bound write source type does not match its column rule",
                ));
            }
        };
        Ok(plan)
    }

    pub(crate) fn materialize(
        self,
        capacity: usize,
    ) -> ArrowConversionResult<DatumEncoder> {
        Ok(match self {
            Self::Bool => DatumEncoder::Bool(BoolEncoder::with_capacity(capacity)),
            Self::I16 => DatumEncoder::I16(PrimitiveEncoder::with_capacity(capacity)),
            Self::I32Int2 => {
                DatumEncoder::I32Int2(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::I32Int4 => {
                DatumEncoder::I32Int4(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::I64Int8 => {
                DatumEncoder::I64Int8(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::F32 => DatumEncoder::F32(PrimitiveEncoder::with_capacity(capacity)),
            Self::F64Float8 => {
                DatumEncoder::F64Float8(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::Text => DatumEncoder::Text(Utf8Encoder::with_capacity(capacity)),
            Self::Name => DatumEncoder::Name(Utf8Encoder::with_capacity(capacity)),
            Self::Bytea => {
                DatumEncoder::Bytea(BinaryEncoder::with_capacity(capacity))
            }
            Self::FixedBytea { len } => DatumEncoder::FixedBytea(
                FixedBinaryEncoder::with_capacity(capacity, len),
            ),
            Self::Uuid => DatumEncoder::Uuid(UuidEncoder::with_capacity(capacity)),
            Self::Numeric { precision, scale } => DatumEncoder::Numeric(
                Decimal128Encoder::with_capacity(capacity, precision, scale)?,
            ),
            Self::Date => {
                DatumEncoder::Date(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::Time => {
                DatumEncoder::Time(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::Timestamp { nanos: false } => DatumEncoder::TimestampMicros(
                BoundTimestampMicrosEncoder::with_capacity(capacity),
            ),
            Self::Timestamp { nanos: true } => DatumEncoder::TimestampNanos(
                BoundTimestampNanosEncoder::with_capacity(capacity),
            ),
            Self::Timestamptz { nanos: false } => DatumEncoder::TimestamptzMicros(
                BoundTimestampMicrosEncoder::with_capacity(capacity),
            ),
            Self::Timestamptz { nanos: true } => DatumEncoder::TimestamptzNanos(
                BoundTimestampNanosEncoder::with_capacity(capacity),
            ),
            Self::BoolArray { field } => DatumEncoder::BoolArray(
                BoundBoolArrayEncoder::with_capacity(capacity, field),
            ),
            Self::ShortArray { field } => DatumEncoder::ShortArray(
                BoundShortArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Int2Array { field } => DatumEncoder::Int2Array(
                BoundInt2ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Int4Array { field } => DatumEncoder::Int4Array(
                BoundInt4ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Int8Array { field } => DatumEncoder::Int8Array(
                BoundInt8ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Float4Array { field } => DatumEncoder::Float4Array(
                BoundFloat4ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Float8Array { field } => DatumEncoder::Float8Array(
                BoundFloat8ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::TextArray { field } => DatumEncoder::TextArray(
                BoundTextArrayEncoder::with_capacity(capacity, field),
            ),
            Self::VarcharArray { field } => DatumEncoder::VarcharArray(
                BoundVarcharArrayEncoder::with_capacity(capacity, field),
            ),
            Self::BpcharArray { field } => DatumEncoder::BpcharArray(
                BoundBpcharArrayEncoder::with_capacity(capacity, field),
            ),
            Self::NameArray { field } => DatumEncoder::NameArray(
                BoundNameArrayEncoder::with_capacity(capacity, field),
            ),
            Self::JsonArray { field } => DatumEncoder::JsonArray(
                BoundJsonArrayEncoder::with_capacity(capacity, field),
            ),
        })
    }
}
