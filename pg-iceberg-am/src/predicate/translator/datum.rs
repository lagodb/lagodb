//! PostgreSQL Datum decoding for Iceberg predicate literals.

use iceberg_lite::spec::Datum;
use pg_arrow_conv::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
use pgrx::{FromDatum, PgBuiltInOids, PgOid, pg_sys};

use super::IcebergTranslationError;

// =============================================================================
// Datum decoding
//
// Decode a non-null PG `Datum` (literal/param value) into an iceberg `Datum`.
// This is the `unsafe` PG-FFI surface of the translator: each arm trusts that
// `type_oid` accurately describes the value behind `datum`. Temporal arms reuse
// the shared PG→Unix epoch offsets so pushed bounds match the storage write
// side (`pg_arrow_conv::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros}`).
// =============================================================================

/// Decode a non-null PG `Datum` into an iceberg [`Datum`].
///
/// Supports integers, date/timestamp types, and text/varchar. Collation
/// admissibility is enforced in
/// [`super::IcebergPredicateTranslator::comparison`].
///
/// # Safety
///
/// `type_oid` must accurately describe the PostgreSQL type represented by
/// `datum`, and `datum` must be non-NULL. For pass-by-reference values, the
/// owning PostgreSQL memory context must remain live for this call.
pub(crate) unsafe fn decode_datum(
    type_oid: pg_sys::Oid,
    datum: pg_sys::Datum,
) -> Result<Datum, IcebergTranslationError> {
    let pg_oid = PgOid::from(type_oid);
    let result = match pg_oid {
        PgOid::BuiltIn(PgBuiltInOids::INT2OID) => {
            // SAFETY: the function contract guarantees a non-NULL Datum, and
            // this match arm establishes that its PostgreSQL type is int2.
            unsafe { i16::from_datum(datum, false) }
                .map(|v| Datum::int(v as i32))
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        PgOid::BuiltIn(PgBuiltInOids::INT4OID) => {
            // SAFETY: the function contract guarantees a non-NULL Datum, and
            // this match arm establishes that its PostgreSQL type is int4.
            unsafe { i32::from_datum(datum, false) }
                .map(Datum::int)
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        PgOid::BuiltIn(PgBuiltInOids::INT8OID) => {
            // SAFETY: the function contract guarantees a non-NULL Datum, and
            // this match arm establishes that its PostgreSQL type is int8.
            unsafe { i64::from_datum(datum, false) }
                .map(Datum::long)
                .ok_or(IcebergTranslationError::DatumDecode { type_oid })?
        }
        PgOid::BuiltIn(PgBuiltInOids::DATEOID) => {
            // SAFETY: the outer contract guarantees non-NULL validity and this
            // match arm establishes the date representation required below.
            unsafe { decode_date(type_oid, datum) }?
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPOID) => {
            // SAFETY: the outer contract guarantees non-NULL validity and this
            // match arm establishes the timestamp representation required below.
            unsafe { decode_timestamp(type_oid, datum) }?
        }
        PgOid::BuiltIn(PgBuiltInOids::TIMESTAMPTZOID) => {
            // SAFETY: the outer contract guarantees non-NULL validity and this
            // arm establishes the timestamptz representation required below.
            unsafe { decode_timestamptz(type_oid, datum) }?
        }
        PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID) => {
            // SAFETY: the function contract guarantees a non-NULL text-compatible
            // Datum whose backing memory context remains live for this call.
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

/// Decode PG `date` using shared PG→Unix day offset.
///
/// # Safety
///
/// `datum` must be a valid non-null PG `date`.
unsafe fn decode_date(
    type_oid: pg_sys::Oid,
    datum: pg_sys::Datum,
) -> Result<Datum, IcebergTranslationError> {
    // SAFETY: the caller guarantees a valid non-NULL PostgreSQL date Datum.
    let pg_days = unsafe { i32::from_datum(datum, false) }
        .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;
    date_from_pg_epoch_days(type_oid, pg_days)
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
    // SAFETY: the caller guarantees a valid non-NULL PostgreSQL timestamp Datum.
    let pg_micros = unsafe { i64::from_datum(datum, false) }
        .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;
    timestamp_from_pg_epoch_micros(
        type_oid,
        pg_micros,
        TimestampKind::WithoutTimeZone,
    )
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
    // SAFETY: the caller guarantees a valid non-NULL PostgreSQL timestamptz Datum.
    let pg_micros = unsafe { i64::from_datum(datum, false) }
        .ok_or(IcebergTranslationError::DatumDecode { type_oid })?;
    timestamp_from_pg_epoch_micros(type_oid, pg_micros, TimestampKind::WithTimeZone)
}

fn date_from_pg_epoch_days(
    type_oid: pg_sys::Oid,
    pg_days: i32,
) -> Result<Datum, IcebergTranslationError> {
    if matches!(pg_days, i32::MIN | i32::MAX) {
        return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
    }
    pg_epoch_days_to_unix_days(pg_days)
        .map(Datum::date)
        .ok_or(IcebergTranslationError::ValueNotRepresentable { type_oid })
}

#[derive(Clone, Copy)]
enum TimestampKind {
    WithoutTimeZone,
    WithTimeZone,
}

fn timestamp_from_pg_epoch_micros(
    type_oid: pg_sys::Oid,
    pg_micros: i64,
    kind: TimestampKind,
) -> Result<Datum, IcebergTranslationError> {
    if matches!(pg_micros, i64::MIN | i64::MAX) {
        return Err(IcebergTranslationError::ValueNotRepresentable { type_oid });
    }
    let unix_micros = pg_epoch_micros_to_unix_micros(pg_micros)
        .ok_or(IcebergTranslationError::ValueNotRepresentable { type_oid })?;
    Ok(match kind {
        TimestampKind::WithoutTimeZone => Datum::timestamp_micros(unix_micros),
        TimestampKind::WithTimeZone => Datum::timestamptz_micros(unix_micros),
    })
}

#[cfg(test)]
mod tests {
    use pg_lakebase_core::tuple::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
    use proptest::prelude::*;

    use super::*;

    const DATE_GUARD: i32 = 20_000;
    const MIN_PG_TS_USEC: i64 = -211_813_488_000_000_000;
    const MAX_PG_TS_USEC: i64 = 9_223_371_331_199_999_999;
    const TS_OFFSET_SAFE_MAX: i64 = i64::MAX - PG_EPOCH_USECS_DIFF;
    const TS_GEN_MAX: i64 = if MAX_PG_TS_USEC < TS_OFFSET_SAFE_MAX {
        MAX_PG_TS_USEC
    } else {
        TS_OFFSET_SAFE_MAX
    };

    #[test]
    fn temporal_conversions_align_at_unix_epoch() {
        assert_eq!(
            date_from_pg_epoch_days(pg_sys::DATEOID, -PG_EPOCH_DAYS_DIFF),
            Ok(Datum::date(0)),
        );
        assert_eq!(
            timestamp_from_pg_epoch_micros(
                pg_sys::TIMESTAMPOID,
                -PG_EPOCH_USECS_DIFF,
                TimestampKind::WithoutTimeZone,
            ),
            Ok(Datum::timestamp_micros(0)),
        );
        assert_eq!(
            timestamp_from_pg_epoch_micros(
                pg_sys::TIMESTAMPTZOID,
                -PG_EPOCH_USECS_DIFF,
                TimestampKind::WithTimeZone,
            ),
            Ok(Datum::timestamptz_micros(0)),
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn pushed_date_bound_matches_write_side_offset(
            pg_days in (i32::MIN + DATE_GUARD)..=(i32::MAX - DATE_GUARD),
        ) {
            let expected = pg_epoch_days_to_unix_days(pg_days).expect("guarded offset");
            prop_assert_eq!(
                date_from_pg_epoch_days(pg_sys::DATEOID, pg_days),
                Ok(Datum::date(expected)),
            );
        }

        #[test]
        fn pushed_timestamp_bounds_match_write_side_offset(
            pg_micros in MIN_PG_TS_USEC..=TS_GEN_MAX,
        ) {
            let expected = pg_epoch_micros_to_unix_micros(pg_micros)
                .expect("guarded offset");
            prop_assert_eq!(
                timestamp_from_pg_epoch_micros(
                    pg_sys::TIMESTAMPOID,
                    pg_micros,
                    TimestampKind::WithoutTimeZone,
                ),
                Ok(Datum::timestamp_micros(expected)),
            );
            prop_assert_eq!(
                timestamp_from_pg_epoch_micros(
                    pg_sys::TIMESTAMPTZOID,
                    pg_micros,
                    TimestampKind::WithTimeZone,
                ),
                Ok(Datum::timestamptz_micros(expected)),
            );
        }
    }

    #[test]
    fn temporal_infinities_are_not_representable() {
        for raw in [i32::MIN, i32::MAX] {
            assert!(matches!(
                date_from_pg_epoch_days(pg_sys::DATEOID, raw),
                Err(IcebergTranslationError::ValueNotRepresentable { .. })
            ));
        }
        for (type_oid, kind) in [
            (pg_sys::TIMESTAMPOID, TimestampKind::WithoutTimeZone),
            (pg_sys::TIMESTAMPTZOID, TimestampKind::WithTimeZone),
        ] {
            for raw in [i64::MIN, i64::MAX] {
                assert!(matches!(
                    timestamp_from_pg_epoch_micros(type_oid, raw, kind),
                    Err(IcebergTranslationError::ValueNotRepresentable { .. })
                ));
            }
        }
    }
}
