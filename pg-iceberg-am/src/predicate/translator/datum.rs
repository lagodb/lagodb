//! PostgreSQL Datum decoding for Iceberg predicate literals.

use iceberg_lite::spec::Datum;
use pg_arrow_conv::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
use pgrx::prelude::{Date, Timestamp, TimestampWithTimeZone};
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
    // SAFETY: the caller guarantees a valid non-NULL PostgreSQL timestamp Datum.
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
    // SAFETY: the caller guarantees a valid non-NULL PostgreSQL timestamptz Datum.
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
