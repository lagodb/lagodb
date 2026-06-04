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

#[cfg(test)]
mod tests {
    use super::*;
    use pg_lakebase_core::tuple::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
    use pgrx::IntoDatum;

    fn date_datum_from_raw(pg_days: i32) -> pg_sys::Datum {
        pg_sys::Datum::from(pg_days)
    }

    fn ts_datum_from_raw(pg_micros: i64) -> pg_sys::Datum {
        pg_sys::Datum::from(pg_micros)
    }

    #[test]
    fn decode_date_applies_shared_epoch_offset() {
        let raw_pg_days: i32 = 1_000;
        let datum = date_datum_from_raw(raw_pg_days);

        let got = unsafe { IcebergDatumDecoder::decode(pg_sys::DATEOID, datum) }
            .expect("a finite, in-range date must decode");

        let expected = Datum::date(raw_pg_days + PG_EPOCH_DAYS_DIFF);
        assert_eq!(
            got, expected,
            "date must use the shared PG->Unix day offset"
        );
    }

    #[test]
    fn decode_date_unix_epoch_is_day_zero() {
        let datum = date_datum_from_raw(-PG_EPOCH_DAYS_DIFF);

        let got = unsafe { IcebergDatumDecoder::decode(pg_sys::DATEOID, datum) }
            .expect("epoch date must decode");

        assert_eq!(got, Datum::date(0));
    }

    #[test]
    fn decode_date_infinity_is_not_representable() {
        for raw in [i32::MAX, i32::MIN] {
            let datum = date_datum_from_raw(raw);
            assert!(
                matches!(
                    unsafe { IcebergDatumDecoder::decode(pg_sys::DATEOID, datum) },
                    Err(IcebergTranslationError::ValueNotRepresentable {
                        type_oid,
                    }) if type_oid == pg_sys::DATEOID
                ),
                "±infinity date (raw {raw}) must be ValueNotRepresentable",
            );
        }
    }

    #[test]
    fn decode_timestamp_applies_shared_epoch_offset() {
        let raw_pg_micros: i64 = 123_456_789;
        let datum = ts_datum_from_raw(raw_pg_micros);

        let got = unsafe { IcebergDatumDecoder::decode(pg_sys::TIMESTAMPOID, datum) }
            .expect("a finite, in-range timestamp must decode");

        let expected = Datum::timestamp_micros(raw_pg_micros + PG_EPOCH_USECS_DIFF);
        assert_eq!(got, expected);
    }

    #[test]
    fn decode_timestamp_unix_epoch_is_zero_micros() {
        let datum = ts_datum_from_raw(-PG_EPOCH_USECS_DIFF);
        let got = unsafe { IcebergDatumDecoder::decode(pg_sys::TIMESTAMPOID, datum) }
            .expect("epoch timestamp must decode");
        assert_eq!(got, Datum::timestamp_micros(0));
    }

    #[test]
    fn decode_timestamp_infinity_is_not_representable() {
        for raw in [i64::MAX, i64::MIN] {
            let datum = ts_datum_from_raw(raw);
            assert!(
                matches!(
                    unsafe { IcebergDatumDecoder::decode(pg_sys::TIMESTAMPOID, datum) },
                    Err(IcebergTranslationError::ValueNotRepresentable {
                        type_oid,
                    }) if type_oid == pg_sys::TIMESTAMPOID
                ),
                "±infinity timestamp (raw {raw}) must be ValueNotRepresentable",
            );
        }
    }

    #[test]
    fn decode_timestamptz_applies_shared_epoch_offset() {
        let raw_pg_micros: i64 = -987_654_321;
        let datum = ts_datum_from_raw(raw_pg_micros);

        let got =
            unsafe { IcebergDatumDecoder::decode(pg_sys::TIMESTAMPTZOID, datum) }
                .expect("a finite, in-range timestamptz must decode");

        let expected = Datum::timestamptz_micros(raw_pg_micros + PG_EPOCH_USECS_DIFF);
        assert_eq!(got, expected);
    }

    #[test]
    fn decode_timestamptz_infinity_is_not_representable() {
        for raw in [i64::MAX, i64::MIN] {
            let datum = ts_datum_from_raw(raw);
            assert!(
                matches!(
                    unsafe { IcebergDatumDecoder::decode(pg_sys::TIMESTAMPTZOID, datum) },
                    Err(IcebergTranslationError::ValueNotRepresentable {
                        type_oid,
                    }) if type_oid == pg_sys::TIMESTAMPTZOID
                ),
                "±infinity timestamptz (raw {raw}) must be ValueNotRepresentable",
            );
        }
    }

    #[test]
    fn decode_float4_builds_float_datum() {
        if !FLOAT_PUSHDOWN_ENABLED {
            // Float decode is gated behind the pushdown toggle; verify rejection.
            let datum = 1.5_f32.into_datum().expect("f32 into_datum");
            assert!(matches!(
                unsafe { IcebergDatumDecoder::decode(pg_sys::FLOAT4OID, datum) },
                Err(IcebergTranslationError::UnsupportedType { .. })
            ));
            return;
        }
        let datum = 1.5_f32.into_datum().expect("f32 into_datum");
        let got = unsafe { IcebergDatumDecoder::decode(pg_sys::FLOAT4OID, datum) }
            .expect("float4 must decode");
        assert_eq!(got, Datum::float(1.5_f32));
    }

    #[test]
    fn decode_float8_builds_double_datum() {
        if !FLOAT_PUSHDOWN_ENABLED {
            let datum = (-273.15_f64).into_datum().expect("f64 into_datum");
            assert!(matches!(
                unsafe { IcebergDatumDecoder::decode(pg_sys::FLOAT8OID, datum) },
                Err(IcebergTranslationError::UnsupportedType { .. })
            ));
            return;
        }
        let datum = (-273.15_f64).into_datum().expect("f64 into_datum");
        let got = unsafe { IcebergDatumDecoder::decode(pg_sys::FLOAT8OID, datum) }
            .expect("float8 must decode");
        assert_eq!(got, Datum::double(-273.15_f64));
    }

    /// When float pushdown is enabled, NaN decodes successfully (relied on
    /// residual for correctness). When disabled, float decode is rejected.
    #[test]
    fn decode_float8_nan_behavior() {
        let datum = f64::NAN.into_datum().expect("f64 NaN into_datum");
        if !FLOAT_PUSHDOWN_ENABLED {
            assert!(matches!(
                unsafe { IcebergDatumDecoder::decode(pg_sys::FLOAT8OID, datum) },
                Err(IcebergTranslationError::UnsupportedType { .. })
            ));
            return;
        }
        let got = unsafe { IcebergDatumDecoder::decode(pg_sys::FLOAT8OID, datum) }
            .expect("float NaN is representable when pushdown enabled");
        assert_eq!(got, Datum::double(f64::NAN));
    }

    #[test]
    fn decode_unsupported_type_is_rejected() {
        let datum = pg_sys::Datum::from(1usize);
        assert!(matches!(
            unsafe { IcebergDatumDecoder::decode(pg_sys::BOOLOID, datum) },
            Err(IcebergTranslationError::UnsupportedType { type_oid })
                if type_oid == pg_sys::BOOLOID
        ));
    }

    // text/varchar decode needs a live PG backend (`palloc`); see pg_test.rs.
}
