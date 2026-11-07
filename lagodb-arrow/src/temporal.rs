//! PostgreSQL ⇄ Arrow temporal value math.
//!
//! These are stateless conversions shared by the datum readers, the encoders
//! and nested JSONB. The two epoch helpers are public because the consumer's
//! predicate translator reuses them to keep pushed bounds aligned with stored
//! manifest bounds; the rest are crate-internal.

use lagodb_core::datum::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
use pgrx::pg_sys;
use pgrx::prelude::{Date, Time, Timestamp, TimestampWithTimeZone};

use crate::error::{ArrowConversionError, ArrowConversionResult};

/// Convert PostgreSQL-epoch days (since 2000-01-01) to Unix-epoch days (since
/// 1970-01-01). Shared by the write side and the predicate translator so a
/// pushed `date` bound uses the same offset as the stored manifest bounds.
/// Returns `None` on `i32` overflow.
pub fn pg_epoch_days_to_unix_days(pg_days: i32) -> Option<i32> {
    pg_days.checked_add(PG_EPOCH_DAYS_DIFF)
}

/// Convert PostgreSQL-epoch microseconds (since 2000-01-01) to Unix-epoch
/// microseconds (since 1970-01-01). Shared by the write side and the predicate
/// translator. Returns `None` on `i64` overflow.
pub fn pg_epoch_micros_to_unix_micros(pg_micros: i64) -> Option<i64> {
    pg_micros.checked_add(PG_EPOCH_USECS_DIFF)
}

fn invalid_datum(message: impl Into<String>) -> ArrowConversionError {
    ArrowConversionError::ValueOutOfRange(message.into())
}

pub(crate) fn pg_date_from_arrow_days(
    arrow_days: i32,
) -> ArrowConversionResult<Date> {
    let pg_days = arrow_days.checked_sub(PG_EPOCH_DAYS_DIFF).ok_or_else(|| {
        invalid_datum(format!(
            "date value {arrow_days} days overflows PostgreSQL epoch"
        ))
    })?;

    Date::try_from(pg_days).map_err(|_| {
        invalid_datum(format!(
            "date value {arrow_days} days is outside PostgreSQL date range"
        ))
    })
}

pub(crate) fn arrow_days_from_pg_date(
    pg_days: pg_sys::DateADT,
) -> ArrowConversionResult<i32> {
    pg_epoch_days_to_unix_days(pg_days).ok_or_else(|| {
        invalid_datum(format!(
            "PostgreSQL date value {pg_days} days overflows Unix epoch"
        ))
    })
}

pub(crate) fn time_from_micros(micros: i64) -> ArrowConversionResult<Time> {
    Time::try_from(micros).map_err(|_| {
        invalid_datum(format!(
            "time value {micros} microseconds is outside PostgreSQL time range"
        ))
    })
}

/// PG includes 24:00:00; Arrow Time excludes the end-of-day boundary.
/// All other PG time values already satisfy the Arrow range.
pub(crate) fn arrow_time_from_pg_time(
    micros: pg_sys::TimeADT,
) -> ArrowConversionResult<i64> {
    if micros == 86_400_000_000 {
        return Err(invalid_datum(
            "PostgreSQL time 24:00:00 is outside the Arrow time range",
        ));
    }
    Ok(micros)
}

pub(crate) fn timestamp_from_unix_micros(
    unix_micros: i64,
) -> ArrowConversionResult<Timestamp> {
    let pg_micros = unix_micros_to_pg_micros(unix_micros)?;
    Timestamp::try_from(pg_micros).map_err(|_| {
        invalid_datum(format!(
            "timestamp value {unix_micros} microseconds is outside PostgreSQL range"
        ))
    })
}

pub(crate) fn timestamptz_from_unix_micros(
    unix_micros: i64,
) -> ArrowConversionResult<TimestampWithTimeZone> {
    let pg_micros = unix_micros_to_pg_micros(unix_micros)?;
    TimestampWithTimeZone::try_from(pg_micros).map_err(|_| {
        invalid_datum(format!(
            "timestamptz value {unix_micros} microseconds is outside PostgreSQL range"
        ))
    })
}

pub(crate) fn unix_micros_from_timestamp(
    pg_micros: i64,
) -> ArrowConversionResult<i64> {
    pg_epoch_micros_to_unix_micros(pg_micros).ok_or_else(|| {
        invalid_datum(format!(
            "PostgreSQL timestamp value {pg_micros} microseconds overflows Unix epoch"
        ))
    })
}

/// Convert PG-epoch microseconds to Unix-epoch nanoseconds. PostgreSQL only
/// stores microsecond resolution, so the result is always a multiple of 1000 —
/// no precision is lost.
pub(crate) fn unix_nanos_from_timestamp(
    pg_micros: i64,
) -> ArrowConversionResult<i64> {
    let unix_micros = unix_micros_from_timestamp(pg_micros)?;
    unix_micros.checked_mul(1_000).ok_or_else(|| {
        invalid_datum(format!(
            "PostgreSQL timestamp value {pg_micros} microseconds overflows the i64 \
             nanosecond range"
        ))
    })
}

/// Convert Unix-epoch nanoseconds to microseconds with floor division, so
/// chronological order is preserved for negative (pre-epoch) timestamps and
/// `read(write(x)) == x` holds.
pub(crate) fn unix_micros_from_nanos(unix_nanos: i64) -> i64 {
    unix_nanos.div_euclid(1_000)
}

/// Convert a Unix-epoch microsecond value to PostgreSQL-epoch microseconds.
/// Returns `None`-equivalent error on underflow past the PG epoch.
pub(crate) fn unix_micros_to_pg_micros(
    unix_micros: i64,
) -> ArrowConversionResult<i64> {
    unix_micros.checked_sub(PG_EPOCH_USECS_DIFF).ok_or_else(|| {
        invalid_datum(format!(
            "timestamp value {unix_micros} microseconds overflows PostgreSQL epoch"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nanos_to_micros_truncates_positive_toward_zero() {
        assert_eq!(unix_micros_from_nanos(0), 0);
        assert_eq!(unix_micros_from_nanos(999), 0);
        assert_eq!(unix_micros_from_nanos(1_000), 1);
        assert_eq!(unix_micros_from_nanos(1_999), 1);
        assert_eq!(unix_micros_from_nanos(2_000), 2);
    }

    #[test]
    fn nanos_to_micros_floors_negative_away_from_zero() {
        assert_eq!(unix_micros_from_nanos(-1), -1);
        assert_eq!(unix_micros_from_nanos(-1_000), -1);
        assert_eq!(unix_micros_from_nanos(-1_001), -2);
        assert_eq!(unix_micros_from_nanos(-2_000), -2);
    }

    #[test]
    fn nanos_to_micros_matches_div_euclid() {
        for nanos in [
            i64::MIN / 2,
            -1_000_000,
            -1_500,
            -1,
            0,
            1,
            1_500,
            i64::MAX / 2,
        ] {
            assert_eq!(unix_micros_from_nanos(nanos), nanos.div_euclid(1_000));
        }
    }

    // PostgreSQL stores microsecond resolution, so the nanosecond branch is
    // exactly the microsecond value scaled by 1000 with no precision loss.
    #[test]
    fn timestamp_nanos_are_micros_times_1000() {
        for pg_micros in [0i64, 1, 999_999, -1, -1_000_000, 1_700_000_000_000_000] {
            let micros = unix_micros_from_timestamp(pg_micros).expect("micros");
            let nanos = unix_nanos_from_timestamp(pg_micros).expect("nanos");
            assert_eq!(nanos, micros * 1_000, "pg_micros={pg_micros}");
        }
    }
}
