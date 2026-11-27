//! Decode PostgreSQL runtime values once when a planned predicate binds.

use core::str;

use iceberg_lite::spec::Datum;
use lagodb_arrow::pg_epoch_days_to_unix_days;
use lagodb_core::datum::{Decimal128ComparisonValue, PackedDetoastedVarlena};
use lagodb_core::expr::{PgBpcharValue, RuntimeValue};
use pgrx::{FromDatum, datum::Uuid as PgUuid, pg_sys};
use uuid::Uuid;

use crate::predicate::error::IcebergFilterError;
use crate::predicate::ir::PlannedValueType;
use crate::predicate::semantics::{
    FloatComparison, FloatValue, Int32OutOfRange, TimestampComparison,
};

pub(super) enum DecodedValue {
    Timestamp(TimestampComparison),
    Float(FloatComparison),
    Datum(Datum),
    OutsideFinite(Decimal128ComparisonValue),
    OutsideInteger(Int32OutOfRange),
    /// A `bpchar` value with more non-blank characters than its column width.
    OutsideWidth,
}

#[inline]
unsafe fn scalar_from_datum<T: FromDatum>(datum: pg_sys::Datum) -> T {
    unsafe { T::from_datum(datum, false) }
        .expect("validated non-NULL PostgreSQL scalar Datum")
}

impl PlannedValueType {
    /// Decode one live non-NULL PostgreSQL runtime value when a predicate binds
    /// or rebinds.
    ///
    /// # Safety
    ///
    /// The value metadata must describe its non-NULL PostgreSQL Datum, whose
    /// memory remains live for this binding call.
    pub(super) unsafe fn decode_datum(
        self,
        value: RuntimeValue,
    ) -> Result<Option<DecodedValue>, IcebergFilterError> {
        let datum = unsafe { value.datum() };
        let decoded = match self {
            Self::Boolean => Some(DecodedValue::Datum(Datum::bool(unsafe {
                scalar_from_datum::<bool>(datum)
            }))),
            Self::Uuid => {
                let value = unsafe { scalar_from_datum::<PgUuid>(datum) };
                Some(DecodedValue::Datum(Datum::uuid(Uuid::from_bytes(
                    *value.as_bytes(),
                ))))
            }
            Self::Binary | Self::FixedBinary(_) => {
                let guard = unsafe { PackedDetoastedVarlena::from_datum(datum) };
                let bytes = guard.bytes();
                let literal = match self {
                    Self::FixedBinary(width) => {
                        if bytes.len() as u64 != width {
                            return Ok(None);
                        }
                        Datum::fixed(bytes.iter().copied())
                    }
                    _ => Datum::binary(bytes.iter().copied()),
                };
                Some(DecodedValue::Datum(literal))
            }
            Self::Int2ToLong => Some(DecodedValue::Datum(Datum::long(i64::from(
                unsafe { scalar_from_datum::<i16>(datum) },
            )))),
            Self::Int4ToLong => Some(DecodedValue::Datum(Datum::long(i64::from(
                unsafe { scalar_from_datum::<i32>(datum) },
            )))),
            Self::Int2 => Some(DecodedValue::Datum(Datum::int(i32::from(unsafe {
                scalar_from_datum::<i16>(datum)
            })))),
            Self::Int4 => Some(DecodedValue::Datum(Datum::int(unsafe {
                scalar_from_datum::<i32>(datum)
            }))),
            Self::Int8 => Some(DecodedValue::Datum(Datum::long(unsafe {
                scalar_from_datum::<i64>(datum)
            }))),
            Self::Int8ToInt => {
                let value = unsafe { scalar_from_datum::<i64>(datum) };
                Some(match Int32OutOfRange::narrow(value) {
                    Ok(value) => DecodedValue::Datum(Datum::int(value)),
                    Err(boundary) => DecodedValue::OutsideInteger(boundary),
                })
            }
            Self::Float => Some(DecodedValue::Float(FloatComparison::Native(
                FloatValue::Float(unsafe { scalar_from_datum::<f32>(datum) }),
            ))),
            Self::Double => Some(DecodedValue::Float(FloatComparison::Native(
                FloatValue::Double(unsafe { scalar_from_datum::<f64>(datum) }),
            ))),
            Self::FloatToDouble => Some(DecodedValue::Float(
                FloatComparison::Native(FloatValue::Double(f64::from(unsafe {
                    scalar_from_datum::<f32>(datum)
                }))),
            )),
            Self::DoubleToFloat => Some(DecodedValue::Float(
                FloatComparison::DoubleOnFloat(unsafe {
                    scalar_from_datum::<f64>(datum)
                }),
            )),
            Self::Date => {
                let days = unsafe { scalar_from_datum::<i32>(datum) };
                if matches!(days, i32::MIN | i32::MAX) {
                    None
                } else {
                    pg_epoch_days_to_unix_days(days)
                        .map(Datum::date)
                        .map(DecodedValue::Datum)
                }
            }
            Self::Timestamp(precision) | Self::Timestamptz(precision) => {
                let micros = unsafe { scalar_from_datum::<i64>(datum) };
                TimestampComparison::from_pg_micros(
                    micros,
                    precision,
                    matches!(self, Self::Timestamptz(_)),
                )
                .map(DecodedValue::Timestamp)
            }
            Self::String => {
                // SAFETY: `datum` is a live non-NULL text/varchar varlena, and
                // the persisted String plan proves `Utf8ServerEncoding`.
                let guard = unsafe { PackedDetoastedVarlena::from_datum(datum) };
                let text = unsafe { str::from_utf8_unchecked(guard.bytes()) };
                Some(DecodedValue::Datum(Datum::string(text)))
            }
            Self::Bpchar(semantics) => {
                Some(match unsafe { semantics.canonicalize(datum) } {
                    PgBpcharValue::Canonical(canonical) => {
                        // SAFETY: `canonical` owns or borrows a live non-NULL
                        // bpchar datum, and the persisted Bpchar plan proves
                        // `Utf8ServerEncoding`.
                        let guard = unsafe {
                            PackedDetoastedVarlena::from_datum(canonical.as_datum())
                        };
                        let text =
                            unsafe { str::from_utf8_unchecked(guard.bytes()) };
                        DecodedValue::Datum(Datum::string(text))
                    }
                    PgBpcharValue::OutsideWidth => DecodedValue::OutsideWidth,
                })
            }
            Self::Decimal128(semantics) => Some(
                match unsafe { semantics.codec().encode_comparison_datum(datum) }? {
                    Decimal128ComparisonValue::Finite(coefficient) => {
                        DecodedValue::Datum(
                            Datum::decimal_from_unscaled(
                                coefficient,
                                u32::from(semantics.precision()),
                                u32::try_from(semantics.scale()).expect(
                                    "validated Decimal128 scale is non-negative",
                                ),
                            )
                            .expect(
                                "Decimal128NumericCodec bounds coefficient to precision",
                            ),
                        )
                    }
                    Decimal128ComparisonValue::NegativeInfinity => {
                        DecodedValue::OutsideFinite(
                            Decimal128ComparisonValue::NegativeInfinity,
                        )
                    }
                    special @ (Decimal128ComparisonValue::PositiveInfinity
                    | Decimal128ComparisonValue::NaN) => {
                        DecodedValue::OutsideFinite(special)
                    }
                },
            ),
        };
        Ok(decoded)
    }
}
