//! Planned comparison value types and their file-independent bound values.
//!
//! A [`ValueType`] is persisted with the plan and states how a PostgreSQL
//! value decodes against the column's declared Parquet contract. Decoding
//! yields a logical [`BoundValue`]; each file then lowers it to its own
//! physical representation (see `lower`).

use lagodb_arrow::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
use lagodb_core::datum::{
    Decimal128ComparisonValue, Decimal128Semantics, PackedDetoastedVarlena,
};
use lagodb_core::expr::{PgBpcharSemantics, PgBpcharValue, RuntimeValue};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use pgrx::datum::Uuid as PgUuid;
use pgrx::{FromDatum, pg_sys};
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::float::FloatComparison;
use super::integer::{IntegerOutOfRange, IntegerWidth};
use super::operator::ComparisonOperator;

const VALUE_BOOL: i32 = 0;
const VALUE_INTEGER: i32 = 1;
const VALUE_FLOAT4: i32 = 2;
const VALUE_FLOAT8: i32 = 3;
const VALUE_FLOAT4_ON_FLOAT8: i32 = 4;
const VALUE_FLOAT8_ON_FLOAT4: i32 = 5;
const VALUE_DECIMAL128: i32 = 6;
const VALUE_STRING: i32 = 7;
const VALUE_BPCHAR: i32 = 8;
const VALUE_BINARY: i32 = 9;
const VALUE_UUID: i32 = 10;
const VALUE_DATE: i32 = 11;
const VALUE_TIMESTAMP: i32 = 12;

/// How a PostgreSQL comparison value binds against a declared column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ValueType {
    Bool,
    /// Any built-in integer value against a column of this physical width.
    Integer(IntegerWidth),
    Float4,
    Float8,
    /// `float84` operators widen the real value exactly.
    Float4OnFloat8,
    /// `float48` operators widen the real column; binding rounds the value.
    Float8OnFloat4,
    Decimal128(Decimal128Semantics),
    String,
    /// A `bpchar` value padded at bind time to a `character(n)` column width.
    Bpchar(PgBpcharSemantics),
    Binary,
    Uuid,
    Date,
    /// `timestamp` or `timestamptz`, both microseconds since the Unix epoch.
    Timestamp,
}

/// File-independent comparison value.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum BoundValue {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal128 {
        coefficient: i128,
        #[serde(with = "decimal_semantics")]
        semantics: Decimal128Semantics,
    },
    String(Box<str>),
    Binary(Box<[u8]>),
    /// Days since the Unix epoch.
    Date32(i32),
    /// Microseconds since the Unix epoch.
    Timestamp(i64),
}

/// One decoded comparison value before the comparison is bound.
pub(super) enum DecodedValue {
    Value(BoundValue),
    Float(FloatComparison),
    OutsideFinite(Decimal128ComparisonValue),
    OutsideInteger(IntegerOutOfRange),
    /// A `bpchar` value wider than its column, which no stored value equals.
    OutsideWidth,
    /// A temporal value with no Unix-epoch value: an infinity, or one whose
    /// epoch shift overflows. Only Conservative plans carry temporal
    /// comparisons, so binding widens such a leaf to TRUE.
    Unrepresentable,
}

impl ValueType {
    /// Operators whose semantics this value type implements.
    pub(super) const fn accepts_operator(self, operator: ComparisonOperator) -> bool {
        match self {
            Self::Bool | Self::Bpchar(_) => {
                matches!(operator, ComparisonOperator::Eq | ComparisonOperator::NotEq)
            }
            _ => true,
        }
    }

    pub(super) fn encode(self, writer: &mut PlanDataWriter) {
        match self {
            Self::Bool => writer.append_i32(VALUE_BOOL),
            Self::Integer(width) => {
                writer.append_i32(VALUE_INTEGER).append_i32(width.tag())
            }
            Self::Float4 => writer.append_i32(VALUE_FLOAT4),
            Self::Float8 => writer.append_i32(VALUE_FLOAT8),
            Self::Float4OnFloat8 => writer.append_i32(VALUE_FLOAT4_ON_FLOAT8),
            Self::Float8OnFloat4 => writer.append_i32(VALUE_FLOAT8_ON_FLOAT4),
            Self::Decimal128(semantics) => writer
                .append_i32(VALUE_DECIMAL128)
                .append_i32(i32::from(semantics.precision()))
                .append_i32(i32::from(semantics.scale())),
            Self::String => writer.append_i32(VALUE_STRING),
            Self::Bpchar(semantics) => writer
                .append_i32(VALUE_BPCHAR)
                .append_i32(semantics.typmod()),
            Self::Binary => writer.append_i32(VALUE_BINARY),
            Self::Uuid => writer.append_i32(VALUE_UUID),
            Self::Date => writer.append_i32(VALUE_DATE),
            Self::Timestamp => writer.append_i32(VALUE_TIMESTAMP),
        };
    }

    pub(super) fn decode_plan(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self, ConnectorError> {
        let invalid = || ConnectorError::invalid_filter_plan(FormatKind::Parquet);
        Ok(match reader.read_i32()? {
            VALUE_BOOL => Self::Bool,
            VALUE_INTEGER => Self::Integer(
                IntegerWidth::from_tag(reader.read_i32()?).ok_or_else(invalid)?,
            ),
            VALUE_FLOAT4 => Self::Float4,
            VALUE_FLOAT8 => Self::Float8,
            VALUE_FLOAT4_ON_FLOAT8 => Self::Float4OnFloat8,
            VALUE_FLOAT8_ON_FLOAT4 => Self::Float8OnFloat4,
            VALUE_DECIMAL128 => {
                let precision = reader.read_i32()?;
                let scale = reader.read_i32()?;
                let semantics = u8::try_from(precision)
                    .ok()
                    .zip(i8::try_from(scale).ok())
                    .and_then(|(precision, scale)| {
                        Decimal128Semantics::new(precision, scale)
                    })
                    .ok_or_else(invalid)?;
                Self::Decimal128(semantics)
            }
            VALUE_STRING => Self::String,
            VALUE_BPCHAR => Self::Bpchar(
                PgBpcharSemantics::for_typmod(reader.read_i32()?)
                    .ok_or_else(invalid)?,
            ),
            VALUE_BINARY => Self::Binary,
            VALUE_UUID => Self::Uuid,
            VALUE_DATE => Self::Date,
            VALUE_TIMESTAMP => Self::Timestamp,
            _ => return Err(invalid()),
        })
    }

    /// # Safety
    ///
    /// `value` must be a live, non-NULL PostgreSQL datum of the planned value
    /// type for this call. Pass-by-reference values are copied into
    /// Rust-owned storage.
    pub(super) unsafe fn decode(
        self,
        value: RuntimeValue,
    ) -> Result<DecodedValue, ConnectorError> {
        let datum = unsafe { value.datum() };
        let type_oid = value.metadata().value_type.type_oid;
        let invalid = || ConnectorError::invalid_filter_datum(type_oid);
        Ok(match self {
            Self::Bool => DecodedValue::Value(BoundValue::Bool(
                unsafe { bool::from_datum(datum, false) }.ok_or_else(invalid)?,
            )),
            Self::Integer(width) => {
                let value = match type_oid {
                    pg_sys::INT2OID => {
                        unsafe { i16::from_datum(datum, false) }.map(i64::from)
                    }
                    pg_sys::INT4OID => {
                        unsafe { i32::from_datum(datum, false) }.map(i64::from)
                    }
                    pg_sys::INT8OID => unsafe { i64::from_datum(datum, false) },
                    _ => None,
                }
                .ok_or_else(invalid)?;
                match width.narrow(value) {
                    Ok(value) => DecodedValue::Value(value),
                    Err(outside) => DecodedValue::OutsideInteger(outside),
                }
            }
            Self::Float4 => DecodedValue::Float(FloatComparison::F32(
                unsafe { f32::from_datum(datum, false) }.ok_or_else(invalid)?,
            )),
            Self::Float8 => DecodedValue::Float(FloatComparison::F64(
                unsafe { f64::from_datum(datum, false) }.ok_or_else(invalid)?,
            )),
            Self::Float4OnFloat8 => {
                DecodedValue::Float(FloatComparison::F64(f64::from(
                    unsafe { f32::from_datum(datum, false) }.ok_or_else(invalid)?,
                )))
            }
            Self::Float8OnFloat4 => {
                DecodedValue::Float(FloatComparison::DoubleOnFloat(
                    unsafe { f64::from_datum(datum, false) }.ok_or_else(invalid)?,
                ))
            }
            Self::Decimal128(semantics) => {
                match unsafe { semantics.codec().encode_comparison_datum(datum) }? {
                    Decimal128ComparisonValue::Finite(coefficient) => {
                        DecodedValue::Value(BoundValue::Decimal128 {
                            coefficient,
                            semantics,
                        })
                    }
                    special => DecodedValue::OutsideFinite(special),
                }
            }
            // A persisted String or Bpchar plan proves that the planner held
            // Utf8ServerEncoding; binding does not repeat that check.
            Self::String => DecodedValue::Value(BoundValue::String(
                unsafe { String::from_datum(datum, false) }
                    .ok_or_else(invalid)?
                    .into_boxed_str(),
            )),
            Self::Bpchar(semantics) => match unsafe { semantics.canonicalize(datum) }
            {
                PgBpcharValue::Canonical(canonical) => {
                    DecodedValue::Value(BoundValue::String(
                        unsafe { String::from_datum(canonical.as_datum(), false) }
                            .ok_or_else(invalid)?
                            .into_boxed_str(),
                    ))
                }
                PgBpcharValue::OutsideWidth => DecodedValue::OutsideWidth,
            },
            Self::Binary => {
                let bytes = unsafe { PackedDetoastedVarlena::from_datum(datum) };
                DecodedValue::Value(BoundValue::Binary(bytes.bytes().into()))
            }
            Self::Uuid => {
                let uuid = unsafe { PgUuid::from_datum(datum, false) }
                    .ok_or_else(invalid)?;
                DecodedValue::Value(BoundValue::Binary(
                    uuid.as_bytes().as_slice().into(),
                ))
            }
            Self::Date => {
                let days =
                    unsafe { i32::from_datum(datum, false) }.ok_or_else(invalid)?;
                pg_epoch_days_to_unix_days(days)
                    .map_or(DecodedValue::Unrepresentable, |days| {
                        DecodedValue::Value(BoundValue::Date32(days))
                    })
            }
            Self::Timestamp => {
                let micros =
                    unsafe { i64::from_datum(datum, false) }.ok_or_else(invalid)?;
                pg_epoch_micros_to_unix_micros(micros)
                    .map_or(DecodedValue::Unrepresentable, |micros| {
                        DecodedValue::Value(BoundValue::Timestamp(micros))
                    })
            }
        })
    }
}

/// Worker transport of decimal semantics as their precision and scale.
mod decimal_semantics {
    use lagodb_core::datum::Decimal128Semantics;
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        semantics: &Decimal128Semantics,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        (semantics.precision(), semantics.scale()).serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Decimal128Semantics, D::Error> {
        let (precision, scale) = <(u8, i8)>::deserialize(deserializer)?;
        Decimal128Semantics::new(precision, scale)
            .ok_or_else(|| D::Error::custom("invalid Decimal128 precision or scale"))
    }
}
