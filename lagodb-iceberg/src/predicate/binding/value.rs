//! Decode PostgreSQL values once when a predicate binds or rebinds.

use iceberg_lite::spec::Datum;
use lagodb_arrow::pg_epoch_days_to_unix_days;
use lagodb_core::expr::RuntimeValue;
use lagodb_core::tuple::{Decimal128ComparisonValue, DetoastedVarlena};
use pgrx::{FromDatum, datum::Uuid as PgUuid};
use uuid::Uuid;

use super::super::error::IcebergFilterError;
use super::super::plan::PlannedValueType;
use super::super::policy::Int32OutOfRange;
use super::super::temporal::TimestampComparison;
use super::{BoundComparisonValue, IcebergFilterBinder};
use crate::error::IcebergError;

impl IcebergFilterBinder<'_> {
    /// # Safety
    ///
    /// The value metadata must describe its non-NULL PostgreSQL Datum, whose
    /// memory remains live for this binding call.
    pub(super) unsafe fn decode_value(
        value_type: PlannedValueType,
        value: RuntimeValue,
    ) -> Result<Option<BoundComparisonValue>, IcebergFilterError> {
        let type_oid = value.metadata().value_type.type_oid;
        let datum = unsafe { value.datum() };
        let decoded = match value_type {
            PlannedValueType::Boolean => {
                Some(BoundComparisonValue::Datum(Datum::bool(
                    unsafe { bool::from_datum(datum, false) }
                        .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
                )))
            }
            PlannedValueType::Uuid => {
                let value = unsafe { PgUuid::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                Some(BoundComparisonValue::Datum(Datum::uuid(Uuid::from_bytes(
                    *value.as_bytes(),
                ))))
            }
            PlannedValueType::Binary | PlannedValueType::FixedBinary(_) => {
                let guard = unsafe { DetoastedVarlena::from_datum(datum) };
                let bytes = guard.bytes();
                let literal = match value_type {
                    PlannedValueType::FixedBinary(width) => {
                        if bytes.len() as u64 != width {
                            return Ok(None);
                        }
                        Datum::fixed(bytes.iter().copied())
                    }
                    _ => Datum::binary(bytes.iter().copied()),
                };
                Some(BoundComparisonValue::Datum(literal))
            }
            PlannedValueType::Int2ToLong => {
                Some(BoundComparisonValue::Datum(Datum::long(i64::from(
                    unsafe { i16::from_datum(datum, false) }
                        .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
                ))))
            }
            PlannedValueType::Int4ToLong => {
                Some(BoundComparisonValue::Datum(Datum::long(i64::from(
                    unsafe { i32::from_datum(datum, false) }
                        .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
                ))))
            }
            PlannedValueType::Int2 => Some(BoundComparisonValue::Datum(Datum::int(
                unsafe { i16::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?
                    as i32,
            ))),
            PlannedValueType::Int4 => Some(BoundComparisonValue::Datum(Datum::int(
                unsafe { i32::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
            ))),
            PlannedValueType::Int8 => Some(BoundComparisonValue::Datum(Datum::long(
                unsafe { i64::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
            ))),
            PlannedValueType::Int8ToInt => {
                let value = unsafe { i64::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                Some(match Int32OutOfRange::narrow(value) {
                    Ok(value) => BoundComparisonValue::Datum(Datum::int(value)),
                    Err(boundary) => BoundComparisonValue::OutsideInteger(boundary),
                })
            }
            PlannedValueType::Date => {
                let days = unsafe { i32::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                if matches!(days, i32::MIN | i32::MAX) {
                    None
                } else {
                    pg_epoch_days_to_unix_days(days)
                        .map(Datum::date)
                        .map(BoundComparisonValue::Datum)
                }
            }
            PlannedValueType::Timestamp(precision)
            | PlannedValueType::Timestamptz(precision) => {
                let micros = unsafe { i64::from_datum(datum, false) }
                    .ok_or(IcebergFilterError::DatumDecode { type_oid })?;
                TimestampComparison::from_pg_micros(
                    micros,
                    precision,
                    matches!(value_type, PlannedValueType::Timestamptz(_)),
                )
                .map(BoundComparisonValue::Timestamp)
            }
            PlannedValueType::String => {
                Some(BoundComparisonValue::Datum(Datum::string(
                    // The persisted String plan proves the planner resolved
                    // `Utf8ServerEncoding`; execution intentionally does not
                    // repeat that statement-invariant check.
                    unsafe { String::from_datum(datum, false) }
                        .ok_or(IcebergFilterError::DatumDecode { type_oid })?,
                )))
            }
            PlannedValueType::Decimal128(semantics) => Some(
                match unsafe { semantics.codec().encode_comparison_datum(datum) }? {
                    Decimal128ComparisonValue::Finite(coefficient) => {
                        BoundComparisonValue::Datum(
                            Datum::decimal_from_unscaled(
                                coefficient,
                                u32::from(semantics.precision()),
                                u32::try_from(semantics.scale()).expect(
                                    "validated Decimal128 scale is non-negative",
                                ),
                            )
                            .map_err(IcebergError::from)?,
                        )
                    }
                    Decimal128ComparisonValue::NegativeInfinity => {
                        BoundComparisonValue::OutsideFinite(
                            Decimal128ComparisonValue::NegativeInfinity,
                        )
                    }
                    special @ (Decimal128ComparisonValue::PositiveInfinity
                    | Decimal128ComparisonValue::NaN) => {
                        BoundComparisonValue::OutsideFinite(special)
                    }
                },
            ),
        };
        Ok(decoded)
    }
}
