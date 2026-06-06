//! [`IcebergDatumDecoder`]: decode a non-null PG `Datum` into an iceberg [`Datum`].

use iceberg_lite::spec::Datum;
use pgrx::prelude::{AnyNumeric, Date, Timestamp, TimestampWithTimeZone};
use pgrx::{FromDatum, PgBuiltInOids, PgOid, pg_sys};
use rust_decimal::Decimal;

use crate::access::conversion::{
    pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros,
};
use crate::customscan::FLOAT_PUSHDOWN_ENABLED;

use super::error::IcebergTranslationError;

/// Decode a non-null PG `Datum` into an iceberg [`Datum`].
///
/// Supports integers, numeric, date/timestamp types, optional floats, and text/varchar.
/// Collation admissibility is enforced in [`IcebergPredicateTranslator::comparison`].
///
/// [`IcebergPredicateTranslator::comparison`]: super::IcebergPredicateTranslator
pub(crate) struct IcebergDatumDecoder;

impl IcebergDatumDecoder {
    /// # Safety
    ///
    /// `type_oid` must accurately describe the PG type the `datum` represents.
    pub(crate) unsafe fn decode(
        type_oid: pg_sys::Oid,
        datum: pg_sys::Datum,
    ) -> Result<Datum, IcebergTranslationError> {
        let pg_oid = PgOid::from(type_oid);
        let result = match pg_oid {
            PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
                unsafe { i16::from_datum(datum, false) }
                    .map(|v| Datum::int(v as i32))
                    .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
            }
            PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
                unsafe { i32::from_datum(datum, false) }
                    .map(Datum::int)
                    .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
            }
            PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
                unsafe { i64::from_datum(datum, false) }
                    .map(Datum::long)
                    .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
            }
            PgOid::BuiltIn(PgBuiltInOids::NUMERICOID) => {
                unsafe { Self::decode_numeric(type_oid, datum) }?
            }
            PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
                unsafe { Self::decode_date(type_oid, datum) }?
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
                unsafe { Self::decode_timestamp(type_oid, datum) }?
            }
            PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
                unsafe { Self::decode_timestamptz(type_oid, datum) }?
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT4OID) if FLOAT_PUSHDOWN_ENABLED => {
                unsafe { f32::from_datum(datum, false) }
                    .map(Datum::float)
                    .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
            }
            PgOid::BuiltIn(PgBuiltInOids::FLOAT8OID) if FLOAT_PUSHDOWN_ENABLED => {
                unsafe { f64::from_datum(datum, false) }
                    .map(Datum::double)
                    .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
            }
            PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID) => {
                unsafe { String::from_datum(datum, false) }
                    .map(Datum::string)
                    .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
            }
            _ => {
                return Err(IcebergTranslationError::UnsupportedType { type_oid });
            }
        };
        Ok(result)
    }

    /// Decode PG `numeric` via canonical text into iceberg decimal [`Datum`].
    ///
    /// # Safety
    ///
    /// `datum` must be a valid non-null PG `numeric`.
    unsafe fn decode_numeric(
        type_oid: pg_sys::Oid,
        datum: pg_sys::Datum,
    ) -> Result<Datum, IcebergTranslationError> {
        let numeric = unsafe { AnyNumeric::from_datum(datum, false) }
            .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;

        // NaN / ±Infinity have no Iceberg ordering for pruning bounds.
        if numeric.is_nan() {
            return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
        }

        let decimal = Decimal::from_str_exact(numeric.normalize()).map_err(|_| {
            IcebergTranslationError::ValueNotRepresentable { type_oid }
        })?;

        Datum::decimal(decimal)
            .map_err(|_| IcebergTranslationError::ValueNotRepresentable { type_oid })
    }

    /// Decode PG `date` using shared PG→Unix day offset.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid non-null PG `date`.
    unsafe fn decode_date(
        type_oid: pg_sys::Oid,
        datum: pg_sys::Datum,
    ) -> Result<Datum, IcebergTranslationError> {
        let date = unsafe { Date::from_datum(datum, false) }
            .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;

        // ±infinity dates have no finite day count.
        if !date.is_finite() {
            return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
        }

        let unix_days = pg_epoch_days_to_unix_days(date.to_pg_epoch_days())
            .ok_or(IcebergTranslationError::ValueNotRepresentable { type_oid })?;
        Ok(Datum::date(unix_days))
    }

    /// Decode PG `timestamp` using shared PG→Unix microsecond offset.
    ///
    /// # Safety
    ///
    /// `datum` must be a valid non-null PG `timestamp`.
    unsafe fn decode_timestamp(
        type_oid: pg_sys::Oid,
        datum: pg_sys::Datum,
    ) -> Result<Datum, IcebergTranslationError> {
        let ts = unsafe { Timestamp::from_datum(datum, false) }
            .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;

        if !ts.is_finite() {
            return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
        }

        let pg_micros: i64 = ts.into();
        let unix_micros = pg_epoch_micros_to_unix_micros(pg_micros)
            .ok_or(IcebergTranslationError::ValueNotRepresentable { type_oid })?;
        Ok(Datum::timestamp_micros(unix_micros))
    }

    /// Decode PG `timestamptz` (PG stores UTC micros since PG epoch).
    ///
    /// # Safety
    ///
    /// `datum` must be a valid non-null PG `timestamptz`.
    unsafe fn decode_timestamptz(
        type_oid: pg_sys::Oid,
        datum: pg_sys::Datum,
    ) -> Result<Datum, IcebergTranslationError> {
        let ts = unsafe { TimestampWithTimeZone::from_datum(datum, false) }
            .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;

        if !ts.is_finite() {
            return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
        }

        let pg_micros: i64 = ts.into();
        let unix_micros = pg_epoch_micros_to_unix_micros(pg_micros)
            .ok_or(IcebergTranslationError::ValueNotRepresentable { type_oid })?;
        Ok(Datum::timestamptz_micros(unix_micros))
    }
}
