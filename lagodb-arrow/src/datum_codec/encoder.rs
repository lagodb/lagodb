//! Datum-only encoder dispatch and lifecycle.

use arrow_array::ArrayRef;
use pgrx::pg_sys;

use super::native_list::{
    BoundBoolArrayEncoder, BoundBpcharArrayEncoder, BoundFloat4ArrayEncoder,
    BoundFloat8ArrayEncoder, BoundInt2ArrayEncoder, BoundInt4ArrayEncoder,
    BoundInt8ArrayEncoder, BoundJsonArrayEncoder, BoundNameArrayEncoder,
    BoundShortArrayEncoder, BoundTextArrayEncoder, BoundVarcharArrayEncoder,
};
use crate::column_encoder::ColumnAppend;
use crate::column_encoder::binary::{BinaryEncoder, FixedBinaryEncoder, UuidEncoder};
use crate::column_encoder::decimal::Decimal128Encoder;
use crate::column_encoder::primitive::{
    BoolEncoder, F32Conv, F64Conv, I16Conv, I32Conv, I64Conv, PrimitiveEncoder,
};
use crate::column_encoder::string::Utf8Encoder;
use crate::column_encoder::temporal::{Date32Conv, Time64Conv};
use crate::column_encoder::timestamp::{
    BoundTimestampMicrosEncoder, BoundTimestampNanosEncoder,
};
use crate::datum_codec::RecursiveDatumEncoder;
use crate::error::ArrowConversionResult;
use crate::jsonb::NestedJsonbEncoder;

/// A relation-bound encoder whose source representation is part of the enum
/// variant. Native leaves use `NativeEncoderPlan`; recursive containers use the
/// declared `BoundDatumPlan`. Row
/// appends therefore receive only the raw non-NULL datum.
pub(crate) enum DatumEncoder {
    Recursive(Box<RecursiveDatumEncoder>),
    Bool(BoolEncoder),
    I16(PrimitiveEncoder<I16Conv>),
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
    NestedJsonb(Box<NestedJsonbEncoder>),
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
            DatumEncoder::Recursive($e) => $body,
            DatumEncoder::Bool($e) => $body,
            DatumEncoder::I16($e) => $body,
            DatumEncoder::I32Int2($e) => $body,
            DatumEncoder::I32Int4($e) => $body,
            DatumEncoder::I32Char($e) => $body,
            DatumEncoder::I64Int2($e) => $body,
            DatumEncoder::I64Int4($e) => $body,
            DatumEncoder::I64Int8($e) => $body,
            DatumEncoder::F32($e) => $body,
            DatumEncoder::F64Float4($e) => $body,
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
    /// Append a non-NULL datum through the source codec selected at binding.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid, non-NULL PostgreSQL datum for this bound
    /// encoder variant, and any relation-bound UTF-8 invariant must hold.
    /// PG errors unwind to the provider/FFI boundary.
    pub(crate) unsafe fn append(
        &mut self,
        datum: pg_sys::Datum,
    ) -> ArrowConversionResult<usize> {
        match self {
            Self::Recursive(encoder) => unsafe { encoder.append(datum) },
            Self::NestedJsonb(encoder) => unsafe { encoder.append(datum) },
            Self::Bool(encoder) => unsafe { encoder.append_bound(datum) },
            Self::I16(encoder) => unsafe {
                encoder.append_bound_value(I16Conv::from_int2(datum))
            },
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
        if let Self::Recursive(encoder) = self {
            encoder.clear();
            return;
        }
        if let Self::NestedJsonb(encoder) = self {
            encoder.clear();
            return;
        }
        let _ = self.finish();
    }

    pub(crate) fn len(&self) -> usize {
        dispatch_bound_encoder!(self, encoder => encoder.len())
    }
}
