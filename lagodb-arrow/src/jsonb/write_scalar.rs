//! Concrete scalar builders for JSONB input. Binding selects codecs once.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    TimestampNanosecondBuilder,
};
use arrow_schema::DataType;
use lagodb_core::tuple::{Decimal128NumericCodec, JsonbDatetime, JsonbRef};
use uuid::Uuid;

use super::NestedJsonbDefault;
use super::scalar::ScalarPlan;
use crate::{ArrowConversionError, ArrowConversionResult};

pub(super) enum ScalarWriter {
    Bool(BooleanBuilder),
    Int(Int32Builder, Decimal128NumericCodec),
    Long(Int64Builder, Decimal128NumericCodec),
    Float(Float32Builder),
    Double(Float64Builder),
    String(StringBuilder),
    LargeString(LargeStringBuilder),
    Binary(BinaryBuilder, Vec<u8>),
    LargeBinary(LargeBinaryBuilder, Vec<u8>),
    Fixed(FixedSizeBinaryBuilder, Vec<u8>),
    Uuid(FixedSizeBinaryBuilder),
    Decimal(Decimal128Builder, Decimal128NumericCodec),
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
    Timestamp(TimestampMicrosecondBuilder, bool),
    TimestampNs(TimestampNanosecondBuilder, bool),
}

macro_rules! scalar_builder {
    ($self:expr, $b:ident => $body:expr) => {
        match $self {
            ScalarWriter::Bool($b) => $body,
            ScalarWriter::Int($b, _) => $body,
            ScalarWriter::Long($b, _) => $body,
            ScalarWriter::Float($b) => $body,
            ScalarWriter::Double($b) => $body,
            ScalarWriter::String($b) => $body,
            ScalarWriter::LargeString($b) => $body,
            ScalarWriter::Binary($b, _) => $body,
            ScalarWriter::LargeBinary($b, _) => $body,
            ScalarWriter::Fixed($b, _) => $body,
            ScalarWriter::Uuid($b) => $body,
            ScalarWriter::Decimal($b, _) => $body,
            ScalarWriter::Date($b) => $body,
            ScalarWriter::Time($b) => $body,
            ScalarWriter::Timestamp($b, _) => $body,
            ScalarWriter::TimestampNs($b, _) => $body,
        }
    };
}

impl ScalarWriter {
    pub(super) fn new(
        plan: &ScalarPlan,
        dt: &DataType,
        capacity: usize,
    ) -> ArrowConversionResult<Self> {
        Ok(match plan {
            ScalarPlan::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            ScalarPlan::I32 => Self::Int(
                Int32Builder::with_capacity(capacity),
                Decimal128NumericCodec::new(10, 0)?,
            ),
            ScalarPlan::I64 => Self::Long(
                Int64Builder::with_capacity(capacity),
                Decimal128NumericCodec::new(19, 0)?,
            ),
            ScalarPlan::F32 => Self::Float(Float32Builder::with_capacity(capacity)),
            ScalarPlan::F64 => Self::Double(Float64Builder::with_capacity(capacity)),
            ScalarPlan::String if matches!(dt, DataType::LargeUtf8) => {
                Self::LargeString(LargeStringBuilder::with_capacity(
                    capacity,
                    capacity * 16,
                ))
            }
            ScalarPlan::String => {
                Self::String(StringBuilder::with_capacity(capacity, capacity * 16))
            }
            ScalarPlan::Binary if matches!(dt, DataType::LargeBinary) => {
                Self::LargeBinary(
                    LargeBinaryBuilder::with_capacity(capacity, capacity * 16),
                    Vec::new(),
                )
            }
            ScalarPlan::Binary => Self::Binary(
                BinaryBuilder::with_capacity(capacity, capacity * 16),
                Vec::new(),
            ),
            ScalarPlan::FixedBinary(len) => Self::Fixed(
                FixedSizeBinaryBuilder::with_capacity(capacity, *len as i32),
                Vec::new(),
            ),
            ScalarPlan::Uuid => {
                Self::Uuid(FixedSizeBinaryBuilder::with_capacity(capacity, 16))
            }
            ScalarPlan::Decimal(codec) => Self::Decimal(
                Decimal128Builder::with_capacity(capacity).with_precision_and_scale(
                    codec.precision() as u8,
                    codec.scale() as i8,
                )?,
                *codec,
            ),
            ScalarPlan::Date => Self::Date(Date32Builder::with_capacity(capacity)),
            ScalarPlan::Time => {
                Self::Time(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            ScalarPlan::Timestamp { nanos: false, tz } => Self::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_data_type(dt.clone()),
                *tz,
            ),
            ScalarPlan::Timestamp { nanos: true, tz } => Self::TimestampNs(
                TimestampNanosecondBuilder::with_capacity(capacity)
                    .with_data_type(dt.clone()),
                *tz,
            ),
        })
    }

    /// Called only within JsonbReader's backend/error/lifetime boundary.
    pub(super) fn append(
        &mut self,
        value: JsonbRef<'_>,
    ) -> ArrowConversionResult<usize> {
        match (self, value) {
            (Self::Bool(b), JsonbRef::Boolean(v)) => {
                b.append_value(v);
                Ok(1)
            }
            (Self::Int(b, codec), JsonbRef::Numeric(v)) => {
                let n = i32::try_from(v.decimal(codec)?)
                    .map_err(|_| Self::range("int32"))?;
                b.append_value(n);
                Ok(4)
            }
            (Self::Long(b, codec), JsonbRef::Numeric(v)) => {
                let n = i64::try_from(v.decimal(codec)?)
                    .map_err(|_| Self::range("int64"))?;
                b.append_value(n);
                Ok(8)
            }
            (Self::Float(b), JsonbRef::Numeric(v)) => {
                b.append_value(v.float4());
                Ok(4)
            }
            (Self::Double(b), JsonbRef::Numeric(v)) => {
                b.append_value(v.float8());
                Ok(8)
            }
            (Self::Float(b), JsonbRef::String(v)) => {
                b.append_value(Self::special_float(v)? as f32);
                Ok(4)
            }
            (Self::Double(b), JsonbRef::String(v)) => {
                b.append_value(Self::special_float(v)?);
                Ok(8)
            }
            (Self::String(b), JsonbRef::String(v)) => {
                b.append_value(v);
                Ok(v.len() + 4)
            }
            (Self::LargeString(b), JsonbRef::String(v)) => {
                b.append_value(v);
                Ok(v.len() + 8)
            }
            (Self::Binary(b, scratch), JsonbRef::String(v)) => {
                Self::hex(v, scratch)?;
                b.append_value(&*scratch);
                Ok(scratch.len() + 4)
            }
            (Self::LargeBinary(b, scratch), JsonbRef::String(v)) => {
                Self::hex(v, scratch)?;
                b.append_value(&*scratch);
                Ok(scratch.len() + 8)
            }
            (Self::Fixed(b, scratch), JsonbRef::String(v)) => {
                Self::hex(v, scratch)?;
                b.append_value(&*scratch)
                    .map_err(|_| Self::range("fixed binary width"))?;
                Ok(scratch.len())
            }
            (Self::Uuid(b), JsonbRef::String(v)) => {
                b.append_value(Uuid::parse_str(v)?.as_bytes())?;
                Ok(16)
            }
            (Self::Decimal(b, codec), JsonbRef::Numeric(v)) => {
                b.append_value(v.decimal(codec)?);
                Ok(16)
            }
            (Self::Date(b), JsonbRef::String(v)) => {
                // SAFETY: caller is inside JsonbReader::read.
                b.append_value(unsafe { JsonbDatetime::Date.parse(v)? } as i32);
                Ok(4)
            }
            (Self::Time(b), JsonbRef::String(v)) => {
                // SAFETY: the complete append runs inside JsonbReader::read.
                b.append_value(unsafe { JsonbDatetime::Time.parse(v)? });
                Ok(8)
            }
            (Self::Timestamp(b, tz), JsonbRef::String(v)) => {
                // SAFETY: the complete append runs inside JsonbReader::read.
                b.append_value(unsafe {
                    JsonbDatetime::Timestamp {
                        nanos: false,
                        tz: *tz,
                    }
                    .parse(v)?
                });
                Ok(8)
            }
            (Self::TimestampNs(b, tz), JsonbRef::String(v)) => {
                // SAFETY: the complete append runs inside JsonbReader::read.
                b.append_value(unsafe {
                    JsonbDatetime::Timestamp {
                        nanos: true,
                        tz: *tz,
                    }
                    .parse(v)?
                });
                Ok(8)
            }
            _ => Err(ArrowConversionError::InvalidInput(
                "JSONB leaf does not match its target type".into(),
            )),
        }
    }

    pub(super) fn append_default(
        &mut self,
        value: &NestedJsonbDefault,
    ) -> ArrowConversionResult<usize> {
        use NestedJsonbDefault as D;
        match (self, value) {
            (Self::Bool(b), D::Boolean(v)) => b.append_value(*v),
            (Self::Int(b, _), D::Int(v)) => b.append_value(*v),
            (Self::Date(b), D::Int(v)) => b.append_value(*v),
            (Self::Long(b, _), D::Long(v)) => b.append_value(*v),
            (Self::Time(b), D::Long(v)) => b.append_value(*v),
            (Self::Timestamp(b, _), D::Long(v)) => b.append_value(*v),
            (Self::TimestampNs(b, _), D::Long(v)) => b.append_value(*v),
            (Self::Float(b), D::Float(v)) => b.append_value(*v),
            (Self::Double(b), D::Double(v)) => b.append_value(*v),
            (Self::String(b), D::String(v)) => {
                b.append_value(v);
                return Ok(v.len() + 4);
            }
            (Self::LargeString(b), D::String(v)) => {
                b.append_value(v);
                return Ok(v.len() + 8);
            }
            (Self::Binary(b, _), D::Binary(v)) => {
                b.append_value(v);
                return Ok(v.len() + 4);
            }
            (Self::LargeBinary(b, _), D::Binary(v)) => {
                b.append_value(v);
                return Ok(v.len() + 8);
            }
            (Self::Fixed(b, _), D::Binary(v)) | (Self::Uuid(b), D::Binary(v)) => {
                b.append_value(v)?;
                return Ok(v.len());
            }
            (Self::Decimal(b, _), D::Decimal(v)) => b.append_value(*v),
            _ => {
                return Err(ArrowConversionError::InvariantViolated(
                    "write default does not match its scalar plan",
                ));
            }
        }
        Ok(16)
    }

    fn range(target: &str) -> ArrowConversionError {
        ArrowConversionError::ValueOutOfRange(format!("JSONB value exceeds {target}"))
    }

    fn special_float(value: &str) -> ArrowConversionResult<f64> {
        // Paired with JsonbBuilder::float: PG Numeric cannot retain negative
        // zero. Only schema-bound float/double writers interpret these strings;
        // ordinary string fields keep their contents unchanged. Casting this
        // exact zero to float4 preserves its sign as well.
        match value {
            "-0.0" => Ok(-0.0),
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(ArrowConversionError::InvalidInput(
                "expected a JSON number or -0.0/NaN/Infinity/-Infinity string".into(),
            )),
        }
    }

    fn hex(value: &str, scratch: &mut Vec<u8>) -> ArrowConversionResult<()> {
        let invalid = || {
            ArrowConversionError::InvalidInput(
                "expected an even-length hexadecimal binary string".into(),
            )
        };
        if !value.len().is_multiple_of(2) {
            return Err(invalid());
        }
        scratch.clear();
        scratch.reserve(value.len() / 2);
        for pair in value.as_bytes().chunks_exact(2) {
            let high = char::from(pair[0]).to_digit(16).ok_or_else(invalid)?;
            let low = char::from(pair[1]).to_digit(16).ok_or_else(invalid)?;
            scratch.push((high * 16 + low) as u8);
        }
        Ok(())
    }

    pub(super) fn append_null(&mut self) {
        scalar_builder!(self, b => b.append_null())
    }
    pub(super) fn len(&self) -> usize {
        scalar_builder!(self, b => b.len())
    }
    pub(super) fn finish(&mut self) -> ArrayRef {
        scalar_builder!(self, b => Arc::new(b.finish()))
    }
}
