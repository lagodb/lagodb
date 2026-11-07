//! Source-bound encoder binding, dispatch and lifecycle.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Time64MicrosecondType,
};
use arrow_schema::FieldRef;
use pgrx::pg_sys;

use super::RecursiveDatumEncoder;
use super::native_list::{
    BoundBoolArrayEncoder, BoundBpcharArrayEncoder, BoundFloat4ArrayEncoder,
    BoundFloat8ArrayEncoder, BoundInt2ArrayEncoder, BoundInt4ArrayEncoder,
    BoundInt8ArrayEncoder, BoundJsonArrayEncoder, BoundNameArrayEncoder,
    BoundShortArrayEncoder, BoundTextArrayEncoder, BoundVarcharArrayEncoder,
};
use crate::encoder::ColumnBuilder;
use crate::encoder::binary::{BinaryEncoder, FixedBinaryEncoder, UuidEncoder};
use crate::encoder::decimal::Decimal128Encoder;
use crate::encoder::primitive::{BoolEncoder, PrimitiveEncoder};
use crate::encoder::string::Utf8Encoder;
use crate::encoder::timestamp::{Micros, Nanos, TimestampEncoder};
use crate::error::{ArrowConversionError, ArrowConversionResult};
use crate::jsonb::NestedJsonbEncoder;
use crate::rule::{CellRule, ListElementRule};
use crate::temporal::{arrow_days_from_pg_date, arrow_time_from_pg_time};

const SOURCE_MISMATCH: &str =
    "bound write source type does not match its column rule";

/// A source-bound encoder whose PostgreSQL representation is part of the enum
/// variant. Native leaves bind through [`Self::bind`]; recursive containers
/// use the declared `BoundDatumPlan`. Row appends therefore receive only the
/// raw non-NULL datum.
pub(crate) enum DatumEncoder {
    Recursive(Box<RecursiveDatumEncoder>),
    Bool(BoolEncoder),
    I16(PrimitiveEncoder<Int16Type>),
    I32Int2(PrimitiveEncoder<Int32Type>),
    I32Int4(PrimitiveEncoder<Int32Type>),
    I64Int8(PrimitiveEncoder<Int64Type>),
    F32(PrimitiveEncoder<Float32Type>),
    F64Float8(PrimitiveEncoder<Float64Type>),
    Text(Utf8Encoder),
    Name(Utf8Encoder),
    Bytea(BinaryEncoder),
    NestedJsonb(Box<NestedJsonbEncoder>),
    FixedBytea(FixedBinaryEncoder),
    Uuid(UuidEncoder),
    Numeric(Decimal128Encoder),
    Date(PrimitiveEncoder<Date32Type>),
    Time(PrimitiveEncoder<Time64MicrosecondType>),
    TimestampMicros(TimestampEncoder<Micros>),
    TimestampNanos(TimestampEncoder<Nanos>),
    TimestamptzMicros(TimestampEncoder<Micros>),
    TimestamptzNanos(TimestampEncoder<Nanos>),
    BoolArray(BoundBoolArrayEncoder),
    Int2Array(BoundInt2ArrayEncoder),
    ShortArray(BoundShortArrayEncoder),
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

/// Expand a lifecycle operation once per source-bound encoder variant. The
/// bound enum has one variant for each valid source codec, so source
/// selection is not repeated for each datum. Timestamp metadata is selected by
/// the outer timestamp/timestamptz variant at finish time rather than stored
/// in the inner encoder.
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
            DatumEncoder::Recursive($e) => $body,
            DatumEncoder::Bool($e) => $body,
            DatumEncoder::I16($e) => $body,
            DatumEncoder::I32Int2($e) => $body,
            DatumEncoder::I32Int4($e) => $body,
            DatumEncoder::I64Int8($e) => $body,
            DatumEncoder::F32($e) => $body,
            DatumEncoder::F64Float8($e) => $body,
            DatumEncoder::Text($e) => $body,
            DatumEncoder::Name($e) => $body,
            DatumEncoder::Bytea($e) => $body,
            DatumEncoder::NestedJsonb($e) => $body,
            DatumEncoder::FixedBytea($e) => $body,
            DatumEncoder::Uuid($e) => $body,
            DatumEncoder::Numeric($e) => $body,
            DatumEncoder::Date($e) => $body,
            DatumEncoder::Time($e) => $body,
            DatumEncoder::TimestampMicros($e) => $timestamp_body,
            DatumEncoder::TimestampNanos($e) => $timestamp_body,
            DatumEncoder::TimestamptzMicros($e) => $timestamptz_body,
            DatumEncoder::TimestamptzNanos($e) => $timestamptz_body,
            DatumEncoder::BoolArray($e) => $body,
            DatumEncoder::Int2Array($e) => $body,
            DatumEncoder::ShortArray($e) => $body,
            DatumEncoder::Int4Array($e) => $body,
            DatumEncoder::Int8Array($e) => $body,
            DatumEncoder::Float4Array($e) => $body,
            DatumEncoder::Float8Array($e) => $body,
            DatumEncoder::TextArray($e) => $body,
            DatumEncoder::VarcharArray($e) => $body,
            DatumEncoder::BpcharArray($e) => $body,
            DatumEncoder::NameArray($e) => $body,
            DatumEncoder::JsonArray($e) => $body,
        }
    };
}

impl DatumEncoder {
    /// Bind one PostgreSQL source OID to an already-resolved output rule.
    ///
    /// This is the single source of truth for the native `(CellRule, source
    /// OID)` write relation. The pair is checked before any Arrow builder is
    /// allocated. Callers validate relation-wide capabilities, such as the
    /// UTF-8 server encoding, before binding any column.
    pub(crate) fn bind(
        rule: &CellRule,
        oid: pg_sys::Oid,
        capacity: usize,
    ) -> ArrowConversionResult<Self> {
        let encoder = match (rule, oid) {
            (CellRule::Bool, pg_sys::BOOLOID) => {
                Self::Bool(BoolEncoder::with_capacity(capacity))
            }
            (CellRule::I16, pg_sys::INT2OID) => {
                Self::I16(PrimitiveEncoder::with_capacity(capacity))
            }
            (CellRule::I32, pg_sys::INT2OID) => {
                Self::I32Int2(PrimitiveEncoder::with_capacity(capacity))
            }
            (CellRule::I32, pg_sys::INT4OID) => {
                Self::I32Int4(PrimitiveEncoder::with_capacity(capacity))
            }
            (CellRule::I64, pg_sys::INT8OID) => {
                Self::I64Int8(PrimitiveEncoder::with_capacity(capacity))
            }
            (CellRule::F32, pg_sys::FLOAT4OID) => {
                Self::F32(PrimitiveEncoder::with_capacity(capacity))
            }
            (CellRule::F64, pg_sys::FLOAT8OID) => {
                Self::F64Float8(PrimitiveEncoder::with_capacity(capacity))
            }
            (
                CellRule::Utf8,
                pg_sys::TEXTOID
                | pg_sys::VARCHAROID
                | pg_sys::BPCHAROID
                | pg_sys::JSONOID,
            ) => Self::Text(Utf8Encoder::with_capacity(capacity)),
            (CellRule::Utf8, pg_sys::NAMEOID) => {
                Self::Name(Utf8Encoder::with_capacity(capacity))
            }
            (CellRule::Binary, pg_sys::BYTEAOID) => {
                Self::Bytea(BinaryEncoder::with_capacity(capacity))
            }
            (CellRule::FixedBinary { len }, pg_sys::BYTEAOID) => {
                Self::FixedBytea(FixedBinaryEncoder::with_capacity(capacity, *len))
            }
            (CellRule::Uuid, pg_sys::UUIDOID) => {
                Self::Uuid(UuidEncoder::with_capacity(capacity))
            }
            (CellRule::Decimal128 { precision, scale }, pg_sys::NUMERICOID) => {
                Self::Numeric(Decimal128Encoder::with_capacity(
                    capacity, *precision, *scale,
                )?)
            }
            (CellRule::Date32, pg_sys::DATEOID) => {
                Self::Date(PrimitiveEncoder::with_capacity(capacity))
            }
            (CellRule::Time64Micros, pg_sys::TIMEOID) => {
                Self::Time(PrimitiveEncoder::with_capacity(capacity))
            }
            (
                CellRule::Timestamp {
                    nanos: false,
                    tz: false,
                },
                pg_sys::TIMESTAMPOID,
            ) => Self::TimestampMicros(TimestampEncoder::with_capacity(capacity)),
            (
                CellRule::Timestamp {
                    nanos: true,
                    tz: false,
                },
                pg_sys::TIMESTAMPOID,
            ) => Self::TimestampNanos(TimestampEncoder::with_capacity(capacity)),
            (
                CellRule::Timestamp {
                    nanos: false,
                    tz: true,
                },
                pg_sys::TIMESTAMPTZOID,
            ) => Self::TimestamptzMicros(TimestampEncoder::with_capacity(capacity)),
            (
                CellRule::Timestamp {
                    nanos: true,
                    tz: true,
                },
                pg_sys::TIMESTAMPTZOID,
            ) => Self::TimestamptzNanos(TimestampEncoder::with_capacity(capacity)),
            (CellRule::List { element, field, .. }, _) => {
                Self::bind_list(*element, field, oid, capacity)?
            }
            _ => {
                return Err(ArrowConversionError::InvariantViolated(SOURCE_MISMATCH));
            }
        };
        Ok(encoder)
    }

    /// Bind a native primitive-array encoder for a PostgreSQL array OID the
    /// element rule can encode.
    pub(super) fn bind_list(
        element: ListElementRule,
        field: &FieldRef,
        oid: pg_sys::Oid,
        capacity: usize,
    ) -> ArrowConversionResult<Self> {
        use ListElementRule as E;

        let field = Arc::clone(field);
        match (element, oid) {
            (E::Bool, pg_sys::BOOLARRAYOID) => Ok(Self::BoolArray(
                BoundBoolArrayEncoder::with_capacity(capacity, field),
            )),
            (E::Short, pg_sys::INT2ARRAYOID) => Ok(Self::ShortArray(
                BoundShortArrayEncoder::with_capacity(capacity, field),
            )),
            (E::Int, pg_sys::INT2ARRAYOID) => Ok(Self::Int2Array(
                BoundInt2ArrayEncoder::with_capacity(capacity, field),
            )),
            (E::Int, pg_sys::INT4ARRAYOID) => Ok(Self::Int4Array(
                BoundInt4ArrayEncoder::with_capacity(capacity, field),
            )),
            (E::Long, pg_sys::INT8ARRAYOID) => Ok(Self::Int8Array(
                BoundInt8ArrayEncoder::with_capacity(capacity, field),
            )),
            (E::Float, pg_sys::FLOAT4ARRAYOID) => Ok(Self::Float4Array(
                BoundFloat4ArrayEncoder::with_capacity(capacity, field),
            )),
            (E::Double, pg_sys::FLOAT8ARRAYOID) => Ok(Self::Float8Array(
                BoundFloat8ArrayEncoder::with_capacity(capacity, field),
            )),
            (E::String, pg_sys::TEXTARRAYOID) => Ok(Self::TextArray(
                BoundTextArrayEncoder::with_capacity(capacity, field),
            )),
            (E::String, pg_sys::VARCHARARRAYOID) => Ok(Self::VarcharArray(
                BoundVarcharArrayEncoder::with_capacity(capacity, field),
            )),
            (E::String, pg_sys::BPCHARARRAYOID) => Ok(Self::BpcharArray(
                BoundBpcharArrayEncoder::with_capacity(capacity, field),
            )),
            (E::String, pg_sys::NAMEARRAYOID) => Ok(Self::NameArray(
                BoundNameArrayEncoder::with_capacity(capacity, field),
            )),
            (E::String, pg_sys::JSONARRAYOID) => Ok(Self::JsonArray(
                BoundJsonArrayEncoder::with_capacity(capacity, field),
            )),
            _ => Err(ArrowConversionError::InvariantViolated(SOURCE_MISMATCH)),
        }
    }

    /// Append a non-NULL datum through the source codec selected at binding.
    /// Fixed-width sources are decoded with PostgreSQL's `DatumGetX`
    /// representation for the bound OID.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-NULL PostgreSQL datum for this bound
    /// encoder variant, and any bound UTF-8 invariant must hold.
    /// PG errors unwind to the provider/FFI boundary.
    pub(crate) unsafe fn append(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        let raw = datum.value();
        match self {
            Self::Recursive(encoder) => unsafe { encoder.append(datum) },
            Self::NestedJsonb(encoder) => unsafe { encoder.append(datum) },
            Self::Bool(encoder) => Ok(encoder.append_value(raw != 0)),
            Self::I16(encoder) => Ok(encoder.append_value(raw as i16)),
            Self::I32Int2(encoder) => Ok(encoder.append_value(i32::from(raw as i16))),
            Self::I32Int4(encoder) => Ok(encoder.append_value(raw as i32)),
            Self::I64Int8(encoder) => Ok(encoder.append_value(raw as i64)),
            Self::F32(encoder) => {
                Ok(encoder.append_value(f32::from_bits(raw as u32)))
            }
            Self::F64Float8(encoder) => {
                Ok(encoder.append_value(f64::from_bits(raw as u64)))
            }
            Self::Text(encoder) => Ok(unsafe { encoder.append_text(datum) }),
            Self::Name(encoder) => Ok(unsafe { encoder.append_name(datum) }),
            Self::Bytea(encoder) => unsafe { encoder.append_bytea(datum) },
            Self::FixedBytea(encoder) => unsafe { encoder.append_bytea(datum) },
            Self::Uuid(encoder) => unsafe { encoder.append_uuid(datum) },
            Self::Numeric(encoder) => unsafe { encoder.append_numeric(datum) },
            Self::Date(encoder) => {
                Ok(encoder.append_value(arrow_days_from_pg_date(raw as i32)?))
            }
            // Match DatumGetTimeADT. Time::from_datum would wrap 24:00:00 to
            // midnight before the Arrow range check could reject it.
            Self::Time(encoder) => {
                Ok(encoder.append_value(arrow_time_from_pg_time(raw as i64)?))
            }
            Self::TimestampMicros(encoder) | Self::TimestamptzMicros(encoder) => unsafe {
                encoder.append(datum)
            },
            Self::TimestampNanos(encoder) | Self::TimestamptzNanos(encoder) => unsafe {
                encoder.append(datum)
            },
            Self::BoolArray(encoder) => unsafe { encoder.append(datum) },
            Self::Int2Array(encoder) => unsafe { encoder.append(datum) },
            Self::ShortArray(encoder) => unsafe { encoder.append(datum) },
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
        match self {
            Self::Recursive(encoder) => encoder.clear(),
            Self::NestedJsonb(encoder) => encoder.clear(),
            _ => {
                let _ = self.finish();
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        dispatch_bound_encoder!(self, encoder => encoder.len())
    }
}
