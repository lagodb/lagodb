//! Leaf binding and JSONB scalar emission, with no per-value Arrow downcasts.

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use lagodb_core::datum::{Decimal128NumericCodec, JsonbBuilder, JsonbPosition};
use uuid::Uuid;

use crate::column_encoder::{downcast, temporal};
use crate::{ArrowConversionError, ArrowConversionResult, CellRule};

#[derive(Debug, Clone)]
pub(super) enum ScalarPlan {
    Bool,
    I32,
    I64,
    F32,
    F64,
    String,
    Binary,
    FixedBinary(usize),
    Uuid,
    Date,
    Time,
    Timestamp { nanos: bool, tz: bool },
    Decimal(Decimal128NumericCodec),
}

impl ScalarPlan {
    pub(super) fn new(rule: CellRule) -> ArrowConversionResult<Self> {
        Ok(match rule {
            CellRule::Bool => Self::Bool,
            CellRule::I32 => Self::I32,
            CellRule::I64 => Self::I64,
            CellRule::F32 => Self::F32,
            CellRule::F64 => Self::F64,
            CellRule::Utf8 => Self::String,
            CellRule::Binary => Self::Binary,
            CellRule::FixedBinary { len } => Self::FixedBinary(len),
            CellRule::Uuid => Self::Uuid,
            CellRule::Date32 => Self::Date,
            CellRule::Time64Micros => Self::Time,
            CellRule::Timestamp { nanos, tz } => Self::Timestamp { nanos, tz },
            CellRule::Decimal128 { precision, scale } => {
                Self::Decimal(Decimal128NumericCodec::new(precision, scale)?)
            }
            other => {
                return Err(ArrowConversionError::UnsupportedColumnType(format!(
                    "{other:?} is not a semantic JSONB scalar"
                )));
            }
        })
    }

    pub(super) fn accepts(&self, dt: &DataType) -> bool {
        match self {
            Self::Bool => matches!(dt, DataType::Boolean),
            Self::I32 => matches!(dt, DataType::Int32),
            Self::I64 => matches!(dt, DataType::Int64),
            Self::F32 => matches!(dt, DataType::Float32),
            Self::F64 => matches!(dt, DataType::Float64),
            Self::String => matches!(dt, DataType::Utf8 | DataType::LargeUtf8),
            Self::Binary => matches!(dt, DataType::Binary | DataType::LargeBinary),
            Self::FixedBinary(len) => {
                matches!(dt, DataType::FixedSizeBinary(n) if *n as usize == *len)
            }
            Self::Uuid => matches!(dt, DataType::FixedSizeBinary(16)),
            Self::Date => matches!(dt, DataType::Date32),
            Self::Time => matches!(dt, DataType::Time64(TimeUnit::Microsecond)),
            Self::Timestamp { nanos, tz } => {
                let unit = if *nanos {
                    TimeUnit::Nanosecond
                } else {
                    TimeUnit::Microsecond
                };
                matches!(dt, DataType::Timestamp(u, zone) if *u == unit && zone.is_some() == *tz)
            }
            Self::Decimal(codec) => matches!(dt, DataType::Decimal128(p, s)
                if u32::from(*p) == codec.precision() && i32::from(*s) == codec.scale() as i32),
        }
    }

    pub(super) fn bind(
        &self,
        array: &dyn Array,
    ) -> ArrowConversionResult<ScalarReader> {
        Ok(match self {
            Self::Bool => ScalarReader::Bool(
                downcast::<BooleanArray>(array, "Boolean")?.clone(),
            ),
            Self::I32 => {
                ScalarReader::I32(downcast::<Int32Array>(array, "Int32")?.clone())
            }
            Self::I64 => {
                ScalarReader::I64(downcast::<Int64Array>(array, "Int64")?.clone())
            }
            Self::F32 => {
                ScalarReader::F32(downcast::<Float32Array>(array, "Float32")?.clone())
            }
            Self::F64 => {
                ScalarReader::F64(downcast::<Float64Array>(array, "Float64")?.clone())
            }
            Self::String => match array.data_type() {
                DataType::Utf8 => {
                    ScalarReader::String(array.as_string::<i32>().clone())
                }
                _ => ScalarReader::LargeString(array.as_string::<i64>().clone()),
            },
            Self::Binary => match array.data_type() {
                DataType::Binary => {
                    ScalarReader::Binary(array.as_binary::<i32>().clone())
                }
                _ => ScalarReader::LargeBinary(array.as_binary::<i64>().clone()),
            },
            Self::FixedBinary(_) => ScalarReader::FixedBinary(
                downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary")?.clone(),
            ),
            Self::Uuid => ScalarReader::Uuid(
                downcast::<FixedSizeBinaryArray>(array, "UUID")?.clone(),
            ),
            Self::Date => {
                ScalarReader::Date(downcast::<Date32Array>(array, "Date32")?.clone())
            }
            Self::Time => ScalarReader::Time(
                downcast::<Time64MicrosecondArray>(array, "Time64")?.clone(),
            ),
            Self::Timestamp { nanos: false, tz } => ScalarReader::TimestampMicros(
                downcast::<TimestampMicrosecondArray>(array, "TimestampMicrosecond")?
                    .clone(),
                *tz,
            ),
            Self::Timestamp { nanos: true, tz } => ScalarReader::TimestampNanos(
                downcast::<TimestampNanosecondArray>(array, "TimestampNanosecond")?
                    .clone(),
                *tz,
            ),
            Self::Decimal(codec) => ScalarReader::Decimal(
                downcast::<Decimal128Array>(array, "Decimal128")?.clone(),
                *codec,
            ),
        })
    }
}

pub(super) enum ScalarReader {
    Bool(BooleanArray),
    I32(Int32Array),
    I64(Int64Array),
    F32(Float32Array),
    F64(Float64Array),
    String(StringArray),
    LargeString(LargeStringArray),
    Binary(BinaryArray),
    LargeBinary(LargeBinaryArray),
    FixedBinary(FixedSizeBinaryArray),
    Uuid(FixedSizeBinaryArray),
    Date(Date32Array),
    Time(Time64MicrosecondArray),
    TimestampMicros(TimestampMicrosecondArray, bool),
    TimestampNanos(TimestampNanosecondArray, bool),
    Decimal(Decimal128Array, Decimal128NumericCodec),
}

impl ScalarReader {
    /// # Safety
    /// `row` is in bounds and non-null, the builder's scalar token is valid,
    /// and a PG backend/current row memory context is active.
    pub(super) unsafe fn append<'a>(
        &'a self,
        row: usize,
        position: JsonbPosition,
        out: &mut JsonbBuilder<'a>,
    ) -> ArrowConversionResult<()> {
        // SAFETY: arrays were bound to concrete types and the parent reader
        // established row/null/token validity. Borrowed strings outlive out.
        unsafe {
            match self {
                Self::Bool(a) => out.boolean(position, a.value_unchecked(row)),
                Self::I32(a) => {
                    out.integer(position, i64::from(a.value_unchecked(row)))
                }
                Self::I64(a) => out.integer(position, a.value_unchecked(row)),
                Self::F32(a) => {
                    out.float(position, f64::from(a.value_unchecked(row)), true)
                }
                Self::F64(a) => out.float(position, a.value_unchecked(row), false),
                Self::String(a) => out.string(position, a.value_unchecked(row))?,
                Self::LargeString(a) => {
                    out.string(position, a.value_unchecked(row))?
                }
                Self::Binary(a) => {
                    out.hex_string(position, a.value_unchecked(row))?
                }
                Self::LargeBinary(a) => {
                    out.hex_string(position, a.value_unchecked(row))?
                }
                Self::FixedBinary(a) => {
                    out.hex_string(position, a.value_unchecked(row))?
                }
                Self::Uuid(a) => {
                    // FixedSizeBinary(16) was established while binding.
                    let bytes = a.value_unchecked(row).try_into().unwrap_unchecked();
                    out.formatted_string(
                        position,
                        format_args!("{}", Uuid::from_bytes(bytes)),
                    )?;
                }
                Self::Decimal(a, codec) => {
                    let datum = codec.decode_datum(a.value_unchecked(row));
                    out.numeric(position, datum);
                }
                Self::Date(a) => {
                    out.datetime(
                        position,
                        temporal::pg_date_from_arrow_days(a.value_unchecked(row))?,
                    );
                }
                Self::Time(a) => {
                    out.datetime(
                        position,
                        temporal::time_from_micros(a.value_unchecked(row))?,
                    );
                }
                Self::TimestampMicros(a, tz) => {
                    let value = a.value_unchecked(row);
                    if *tz {
                        out.datetime(
                            position,
                            temporal::timestamptz_from_unix_micros(value)?,
                        );
                    } else {
                        out.datetime(
                            position,
                            temporal::timestamp_from_unix_micros(value)?,
                        );
                    }
                }
                Self::TimestampNanos(a, tz) => {
                    out.timestamp_nanos(position, a.value_unchecked(row), *tz)?;
                }
            }
        }
        Ok(())
    }
}
