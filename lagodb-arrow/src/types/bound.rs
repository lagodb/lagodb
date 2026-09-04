//! Relation-bound PostgreSQL datum encoders.
//!
//! The source codec and Arrow builder are selected together while a bound write
//! plan is constructed.  The row path therefore performs one enum dispatch to
//! a concrete encoder and never re-matches a source OID or source family.

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use pgrx::pg_sys;

use super::ColumnAppend;
use super::binary::{BinaryEncoder, BinaryKind, FixedBinaryEncoder, UuidEncoder};
use super::bound_list::{
    BoundBoolArrayEncoder, BoundBpcharArrayEncoder, BoundFloat4ArrayEncoder,
    BoundFloat8ArrayEncoder, BoundInt2ArrayEncoder, BoundInt4ArrayEncoder,
    BoundInt8ArrayEncoder, BoundJsonArrayEncoder, BoundNameArrayEncoder,
    BoundTextArrayEncoder, BoundVarcharArrayEncoder,
};
use super::decimal::Decimal128Encoder;
use super::primitive::{
    BoolEncoder, F32Conv, F64Conv, I32Conv, I64Conv, PrimitiveEncoder,
};
use super::string::Utf8Encoder;
use super::temporal::{Date32Conv, Time64Conv};
use super::timestamp::{BoundTimestampMicrosEncoder, BoundTimestampNanosEncoder};
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::rule::{ColumnRule, ListElementRule};

/// A validated relation-bound source plan without Arrow builder state.
///
/// This is the single source of truth for the `(ColumnRule, PostgreSQL OID)`
/// compatibility relation. The runtime encoder is materialized only after
/// relation-wide capabilities, such as server encoding, have been checked.
pub(crate) enum BoundEncoderPlan {
    Bool,
    I32Int2,
    I32Int4,
    I32Char,
    I64Int2,
    I64Int4,
    I64Int8,
    F32,
    F64Float4,
    F64Float8,
    Text,
    Name,
    Bytea,
    Jsonb,
    FixedBytea { len: usize },
    Uuid,
    Numeric { precision: u32, scale: u32 },
    Date,
    Time,
    Timestamp { nanos: bool },
    Timestamptz { nanos: bool },
    BoolArray { field: FieldRef },
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

impl BoundEncoderPlan {
    /// Resolve one PostgreSQL source OID against the already-resolved output
    /// rule. A successful plan contains all information needed by the runtime
    /// encoder and no longer retains the general `ColumnRule`.
    pub(crate) fn bind(
        rule: ColumnRule,
        oid: pg_sys::Oid,
    ) -> ArrowConversionResult<Self> {
        let plan = match (rule, oid) {
            (ColumnRule::Bool, pg_sys::BOOLOID) => Self::Bool,
            (ColumnRule::I32, pg_sys::INT2OID) => Self::I32Int2,
            (ColumnRule::I32, pg_sys::INT4OID) => Self::I32Int4,
            (ColumnRule::I32, pg_sys::CHAROID) => Self::I32Char,
            (ColumnRule::I64, pg_sys::INT2OID) => Self::I64Int2,
            (ColumnRule::I64, pg_sys::INT4OID) => Self::I64Int4,
            (ColumnRule::I64, pg_sys::INT8OID) => Self::I64Int8,
            (ColumnRule::F32, pg_sys::FLOAT4OID) => Self::F32,
            (ColumnRule::F64, pg_sys::FLOAT4OID) => Self::F64Float4,
            (ColumnRule::F64, pg_sys::FLOAT8OID) => Self::F64Float8,
            (ColumnRule::Utf8, oid)
                if oid == pg_sys::TEXTOID
                    || oid == pg_sys::VARCHAROID
                    || oid == pg_sys::BPCHAROID
                    || oid == pg_sys::JSONOID =>
            {
                Self::Text
            }
            (ColumnRule::Utf8, pg_sys::NAMEOID) => Self::Name,
            (ColumnRule::Binary, pg_sys::BYTEAOID) => Self::Bytea,
            (ColumnRule::PostgresJsonbVarlena, pg_sys::JSONBOID) => Self::Jsonb,
            (ColumnRule::FixedBinary { len }, pg_sys::BYTEAOID) => {
                Self::FixedBytea { len }
            }
            (ColumnRule::Uuid, pg_sys::UUIDOID) => Self::Uuid,
            (ColumnRule::Decimal128 { precision, scale }, pg_sys::NUMERICOID) => {
                Self::Numeric { precision, scale }
            }
            (ColumnRule::Date32, pg_sys::DATEOID) => Self::Date,
            (ColumnRule::Time64Micros, pg_sys::TIMEOID) => Self::Time,
            (ColumnRule::Timestamp { nanos, tz: false }, pg_sys::TIMESTAMPOID) => {
                Self::Timestamp { nanos }
            }
            (ColumnRule::Timestamp { nanos, tz: true }, pg_sys::TIMESTAMPTZOID) => {
                Self::Timestamptz { nanos }
            }
            (
                ColumnRule::List {
                    element: ListElementRule::Bool,
                    field,
                    ..
                },
                pg_sys::BOOLARRAYOID,
            ) => Self::BoolArray { field },
            (
                ColumnRule::List {
                    element: ListElementRule::Int,
                    field,
                    ..
                },
                pg_sys::INT2ARRAYOID,
            ) => Self::Int2Array { field },
            (
                ColumnRule::List {
                    element: ListElementRule::Int,
                    field,
                    ..
                },
                pg_sys::INT4ARRAYOID,
            ) => Self::Int4Array { field },
            (
                ColumnRule::List {
                    element: ListElementRule::Long,
                    field,
                    ..
                },
                pg_sys::INT8ARRAYOID,
            ) => Self::Int8Array { field },
            (
                ColumnRule::List {
                    element: ListElementRule::Float,
                    field,
                    ..
                },
                pg_sys::FLOAT4ARRAYOID,
            ) => Self::Float4Array { field },
            (
                ColumnRule::List {
                    element: ListElementRule::Double,
                    field,
                    ..
                },
                pg_sys::FLOAT8ARRAYOID,
            ) => Self::Float8Array { field },
            (
                ColumnRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::TEXTARRAYOID,
            ) => Self::TextArray { field },
            (
                ColumnRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::VARCHARARRAYOID,
            ) => Self::VarcharArray { field },
            (
                ColumnRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::BPCHARARRAYOID,
            ) => Self::BpcharArray { field },
            (
                ColumnRule::List {
                    element: ListElementRule::String,
                    field,
                    ..
                },
                pg_sys::NAMEARRAYOID,
            ) => Self::NameArray { field },
            (
                ColumnRule::List {
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
    ) -> ArrowConversionResult<BoundColumnEncoder> {
        Ok(match self {
            Self::Bool => {
                BoundColumnEncoder::Bool(BoolEncoder::with_capacity(capacity))
            }
            Self::I32Int2 => {
                BoundColumnEncoder::I32Int2(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::I32Int4 => {
                BoundColumnEncoder::I32Int4(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::I32Char => {
                BoundColumnEncoder::I32Char(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::I64Int2 => {
                BoundColumnEncoder::I64Int2(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::I64Int4 => {
                BoundColumnEncoder::I64Int4(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::I64Int8 => {
                BoundColumnEncoder::I64Int8(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::F32 => {
                BoundColumnEncoder::F32(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::F64Float4 => BoundColumnEncoder::F64Float4(
                PrimitiveEncoder::with_capacity(capacity),
            ),
            Self::F64Float8 => BoundColumnEncoder::F64Float8(
                PrimitiveEncoder::with_capacity(capacity),
            ),
            Self::Text => {
                BoundColumnEncoder::Text(Utf8Encoder::with_capacity(capacity))
            }
            Self::Name => {
                BoundColumnEncoder::Name(Utf8Encoder::with_capacity(capacity))
            }
            Self::Bytea => BoundColumnEncoder::Bytea(BinaryEncoder::with_capacity(
                capacity,
                BinaryKind::Bytea,
            )),
            Self::Jsonb => BoundColumnEncoder::Jsonb(BinaryEncoder::with_capacity(
                capacity,
                BinaryKind::PostgresJsonbVarlena,
            )),
            Self::FixedBytea { len } => BoundColumnEncoder::FixedBytea(
                FixedBinaryEncoder::with_capacity(capacity, len),
            ),
            Self::Uuid => {
                BoundColumnEncoder::Uuid(UuidEncoder::with_capacity(capacity))
            }
            Self::Numeric { precision, scale } => BoundColumnEncoder::Numeric(
                Decimal128Encoder::with_capacity(capacity, precision, scale)?,
            ),
            Self::Date => {
                BoundColumnEncoder::Date(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::Time => {
                BoundColumnEncoder::Time(PrimitiveEncoder::with_capacity(capacity))
            }
            Self::Timestamp { nanos: false } => BoundColumnEncoder::TimestampMicros(
                BoundTimestampMicrosEncoder::with_capacity(capacity),
            ),
            Self::Timestamp { nanos: true } => BoundColumnEncoder::TimestampNanos(
                BoundTimestampNanosEncoder::with_capacity(capacity),
            ),
            Self::Timestamptz { nanos: false } => {
                BoundColumnEncoder::TimestamptzMicros(
                    BoundTimestampMicrosEncoder::with_capacity(capacity),
                )
            }
            Self::Timestamptz { nanos: true } => {
                BoundColumnEncoder::TimestamptzNanos(
                    BoundTimestampNanosEncoder::with_capacity(capacity),
                )
            }
            Self::BoolArray { field } => BoundColumnEncoder::BoolArray(
                BoundBoolArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Int2Array { field } => BoundColumnEncoder::Int2Array(
                BoundInt2ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Int4Array { field } => BoundColumnEncoder::Int4Array(
                BoundInt4ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Int8Array { field } => BoundColumnEncoder::Int8Array(
                BoundInt8ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Float4Array { field } => BoundColumnEncoder::Float4Array(
                BoundFloat4ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::Float8Array { field } => BoundColumnEncoder::Float8Array(
                BoundFloat8ArrayEncoder::with_capacity(capacity, field),
            ),
            Self::TextArray { field } => BoundColumnEncoder::TextArray(
                BoundTextArrayEncoder::with_capacity(capacity, field),
            ),
            Self::VarcharArray { field } => BoundColumnEncoder::VarcharArray(
                BoundVarcharArrayEncoder::with_capacity(capacity, field),
            ),
            Self::BpcharArray { field } => BoundColumnEncoder::BpcharArray(
                BoundBpcharArrayEncoder::with_capacity(capacity, field),
            ),
            Self::NameArray { field } => BoundColumnEncoder::NameArray(
                BoundNameArrayEncoder::with_capacity(capacity, field),
            ),
            Self::JsonArray { field } => BoundColumnEncoder::JsonArray(
                BoundJsonArrayEncoder::with_capacity(capacity, field),
            ),
        })
    }
}

/// A relation-bound encoder whose source representation is part of the enum
/// variant. It is only materialized from a validated [`BoundEncoderPlan`]; row
/// appends therefore receive only the raw non-NULL datum.
pub(crate) enum BoundColumnEncoder {
    Bool(BoolEncoder),
    I32Int2(PrimitiveEncoder<I32Conv>),
    I32Int4(PrimitiveEncoder<I32Conv>),
    I32Char(PrimitiveEncoder<I32Conv>),
    I64Int2(PrimitiveEncoder<I64Conv>),
    I64Int4(PrimitiveEncoder<I64Conv>),
    I64Int8(PrimitiveEncoder<I64Conv>),
    F32(PrimitiveEncoder<F32Conv>),
    F64Float4(PrimitiveEncoder<F64Conv>),
    F64Float8(PrimitiveEncoder<F64Conv>),
    Text(Utf8Encoder),
    Name(Utf8Encoder),
    Bytea(BinaryEncoder),
    Jsonb(BinaryEncoder),
    FixedBytea(FixedBinaryEncoder),
    Uuid(UuidEncoder),
    Numeric(Decimal128Encoder),
    Date(PrimitiveEncoder<Date32Conv>),
    Time(PrimitiveEncoder<Time64Conv>),
    TimestampMicros(BoundTimestampMicrosEncoder),
    TimestampNanos(BoundTimestampNanosEncoder),
    TimestamptzMicros(BoundTimestampMicrosEncoder),
    TimestamptzNanos(BoundTimestampNanosEncoder),
    BoolArray(BoundBoolArrayEncoder),
    Int2Array(BoundInt2ArrayEncoder),
    Int4Array(BoundInt4ArrayEncoder),
    Int8Array(BoundInt8ArrayEncoder),
    Float4Array(BoundFloat4ArrayEncoder),
    Float8Array(BoundFloat8ArrayEncoder),
    TextArray(BoundTextArrayEncoder),
    VarcharArray(BoundVarcharArrayEncoder),
    BpcharArray(BoundBpcharArrayEncoder),
    NameArray(BoundNameArrayEncoder),
    JsonArray(BoundJsonArrayEncoder),
}

/// Expand a lifecycle operation once per source-bound encoder variant. This is
/// deliberately separate from the row-world encoder dispatch: the bound enum
/// has one variant for each valid source codec, so source selection is not
/// repeated for each datum. Timestamp metadata is selected by the outer
/// timestamp/timestamptz variant at finish time rather than stored in the
/// inner encoder.
macro_rules! dispatch_bound_encoder {
    ($self:expr, $e:ident => $body:expr) => {
        dispatch_bound_encoder!(
            $self,
            $e => $body,
            timestamp => $body,
            timestamptz => $body
        )
    };
    (
        $self:expr,
        $e:ident => $body:expr,
        timestamp => $timestamp_body:expr,
        timestamptz => $timestamptz_body:expr $(,)?
    ) => {
        match $self {
            BoundColumnEncoder::Bool($e) => $body,
            BoundColumnEncoder::I32Int2($e) => $body,
            BoundColumnEncoder::I32Int4($e) => $body,
            BoundColumnEncoder::I32Char($e) => $body,
            BoundColumnEncoder::I64Int2($e) => $body,
            BoundColumnEncoder::I64Int4($e) => $body,
            BoundColumnEncoder::I64Int8($e) => $body,
            BoundColumnEncoder::F32($e) => $body,
            BoundColumnEncoder::F64Float4($e) => $body,
            BoundColumnEncoder::F64Float8($e) => $body,
            BoundColumnEncoder::Text($e) => $body,
            BoundColumnEncoder::Name($e) => $body,
            BoundColumnEncoder::Bytea($e) => $body,
            BoundColumnEncoder::Jsonb($e) => $body,
            BoundColumnEncoder::FixedBytea($e) => $body,
            BoundColumnEncoder::Uuid($e) => $body,
            BoundColumnEncoder::Numeric($e) => $body,
            BoundColumnEncoder::Date($e) => $body,
            BoundColumnEncoder::Time($e) => $body,
            BoundColumnEncoder::TimestampMicros($e) => $timestamp_body,
            BoundColumnEncoder::TimestampNanos($e) => $timestamp_body,
            BoundColumnEncoder::TimestamptzMicros($e) => $timestamptz_body,
            BoundColumnEncoder::TimestamptzNanos($e) => $timestamptz_body,
            BoundColumnEncoder::BoolArray($e) => $body,
            BoundColumnEncoder::Int2Array($e) => $body,
            BoundColumnEncoder::Int4Array($e) => $body,
            BoundColumnEncoder::Int8Array($e) => $body,
            BoundColumnEncoder::Float4Array($e) => $body,
            BoundColumnEncoder::Float8Array($e) => $body,
            BoundColumnEncoder::TextArray($e) => $body,
            BoundColumnEncoder::VarcharArray($e) => $body,
            BoundColumnEncoder::BpcharArray($e) => $body,
            BoundColumnEncoder::NameArray($e) => $body,
            BoundColumnEncoder::JsonArray($e) => $body,
        }
    };
}

impl BoundColumnEncoder {
    /// Append a non-NULL datum through the source codec selected at binding.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-NULL PostgreSQL datum for this bound
    /// encoder variant, and any relation-bound UTF-8 invariant must hold.
    pub(crate) unsafe fn append(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        match self {
            Self::Bool(encoder) => unsafe { encoder.append_bound(datum) },
            Self::I32Int2(encoder) => unsafe {
                encoder.append_bound_value(I32Conv::from_int2(datum))
            },
            Self::I32Int4(encoder) => unsafe {
                encoder.append_bound_value(I32Conv::from_int4(datum))
            },
            Self::I32Char(encoder) => unsafe {
                encoder.append_bound_value(I32Conv::from_char(datum))
            },
            Self::I64Int2(encoder) => unsafe {
                encoder.append_bound_value(I64Conv::from_int2(datum))
            },
            Self::I64Int4(encoder) => unsafe {
                encoder.append_bound_value(I64Conv::from_int4(datum))
            },
            Self::I64Int8(encoder) => unsafe {
                encoder.append_bound_value(I64Conv::from_int8(datum))
            },
            Self::F32(encoder) => unsafe {
                encoder.append_bound_value(F32Conv::from_float4(datum))
            },
            Self::F64Float4(encoder) => unsafe {
                encoder.append_bound_value(F64Conv::from_float4(datum))
            },
            Self::F64Float8(encoder) => unsafe {
                encoder.append_bound_value(F64Conv::from_float8(datum))
            },
            Self::Text(encoder) => unsafe { encoder.append_text(datum) },
            Self::Name(encoder) => unsafe { encoder.append_name(datum) },
            Self::Bytea(encoder) => unsafe { encoder.append_bytea(datum) },
            Self::Jsonb(encoder) => unsafe { encoder.append_jsonb(datum) },
            Self::FixedBytea(encoder) => unsafe { encoder.append_bound(datum) },
            Self::Uuid(encoder) => unsafe { encoder.append_bound(datum) },
            Self::Numeric(encoder) => unsafe { encoder.append_bound(datum) },
            Self::Date(encoder) => unsafe {
                encoder.append_bound_value(Date32Conv::from_date(datum))
            },
            Self::Time(encoder) => unsafe {
                encoder.append_bound_value(Time64Conv::from_time(datum))
            },
            Self::TimestampMicros(encoder) => unsafe {
                encoder.append_timestamp(datum)
            },
            Self::TimestampNanos(encoder) => unsafe {
                encoder.append_timestamp(datum)
            },
            Self::TimestamptzMicros(encoder) => unsafe {
                encoder.append_timestamptz(datum)
            },
            Self::TimestamptzNanos(encoder) => unsafe {
                encoder.append_timestamptz(datum)
            },
            Self::BoolArray(encoder) => unsafe { encoder.append(datum) },
            Self::Int2Array(encoder) => unsafe { encoder.append(datum) },
            Self::Int4Array(encoder) => unsafe { encoder.append(datum) },
            Self::Int8Array(encoder) => unsafe { encoder.append(datum) },
            Self::Float4Array(encoder) => unsafe { encoder.append(datum) },
            Self::Float8Array(encoder) => unsafe { encoder.append(datum) },
            Self::TextArray(encoder) => unsafe { encoder.append(datum) },
            Self::VarcharArray(encoder) => unsafe { encoder.append(datum) },
            Self::BpcharArray(encoder) => unsafe { encoder.append(datum) },
            Self::NameArray(encoder) => unsafe { encoder.append(datum) },
            Self::JsonArray(encoder) => unsafe { encoder.append(datum) },
        }
    }

    pub(crate) fn append_null(&mut self) {
        dispatch_bound_encoder!(self, encoder => encoder.append_null())
    }

    pub(crate) fn finish(&mut self) -> ArrowConversionResult<ArrayRef> {
        dispatch_bound_encoder!(
            self,
            encoder => encoder.finish(),
            timestamp => encoder.finish(None),
            timestamptz => encoder.finish(Some("+00:00"))
        )
    }

    pub(crate) fn clear(&mut self) {
        let _ = self.finish();
    }

    pub(crate) fn len(&self) -> usize {
        dispatch_bound_encoder!(self, encoder => encoder.len())
    }
}
