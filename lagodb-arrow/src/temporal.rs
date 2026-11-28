//! PostgreSQL ⇄ Arrow temporal value math.
//!
//! These are stateless conversions shared by the datum readers, the encoders
//! and nested JSONB. The two epoch helpers are public so every PostgreSQL to
//! Unix-epoch conversion outside this crate (predicate pushdown, Parquet
//! filters, the Avro writer and offloaded query constants) uses the same
//! shift and the same infinity rejection as stored values; the rest are
//! crate-internal.

use lagodb_core::datum::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
use pgrx::datum::USECS_PER_DAY;
use pgrx::pg_sys;
use pgrx::prelude::{Date, Time, Timestamp, TimestampWithTimeZone};

use crate::error::{ArrowConversionError, ArrowConversionResult};

// PostgreSQL's `-infinity`/`infinity` sentinels are the extremes of their
// typedefs (`DT_NOBEGIN`/`DT_NOEND` in `datatype/timestamp.h`,
// `DATEVAL_NOBEGIN`/`DATEVAL_NOEND` in `utils/date.h`). They are spelled as
// `pg_sys::Timestamp::MIN`/`MAX` and `pg_sys::DateADT::MIN`/`MAX` because
// bindgen types those macro constants per platform (`DT_NOEND` is `u64` on
// macOS, `i64` on Linux).
//
// Arrow has no infinity, so reads reject the one Arrow value that the epoch
// shift maps onto the `-infinity` sentinel, as the Avro reader does; pgrx's
// `try_from` alone would accept it. The shift cannot reach `infinity`.

/// Convert PostgreSQL-epoch days (since 2000-01-01) to Unix-epoch days (since
/// 1970-01-01). Shared by the write side and the predicate translators so a
/// pushed `date` bound uses the same offset as the stored manifest bounds.
/// Returns `None` for PostgreSQL's `-infinity` and `infinity`
/// (`DATE_NOT_FINITE` in `utils/date.h`), which have no Unix-epoch day, and
/// on `i32` overflow: `infinity` overflows the shift, and `-infinity` would
/// otherwise become a finite day about 5.9 million years BC.
pub fn pg_epoch_days_to_unix_days(pg_days: i32) -> Option<i32> {
    if pg_days == pg_sys::DateADT::MIN {
        return None;
    }
    pg_days.checked_add(PG_EPOCH_DAYS_DIFF)
}

/// Convert PostgreSQL-epoch microseconds (since 2000-01-01) to Unix-epoch
/// microseconds (since 1970-01-01). Shared by the write side and the predicate
/// translators. Returns `None` for PostgreSQL's `-infinity` and `infinity`
/// (`TIMESTAMP_NOT_FINITE` in `datatype/timestamp.h`), which have no
/// Unix-epoch instant, and on `i64` overflow: `infinity` overflows the shift,
/// and `-infinity` would otherwise become an instant about 290,000 years BC.
pub fn pg_epoch_micros_to_unix_micros(pg_micros: i64) -> Option<i64> {
    if pg_micros == pg_sys::Timestamp::MIN {
        return None;
    }
    pg_micros.checked_add(PG_EPOCH_USECS_DIFF)
}

pub(crate) fn pg_date_from_arrow_days(
    arrow_days: i32,
) -> ArrowConversionResult<Date> {
    arrow_days
        .checked_sub(PG_EPOCH_DAYS_DIFF)
        .filter(|&pg_days| pg_days != pg_sys::DateADT::MIN)
        .and_then(|pg_days| Date::try_from(pg_days).ok())
        .ok_or_else(|| {
            ArrowConversionError::DatetimeValueOutOfRange(format!(
                "date value {arrow_days} days is outside PostgreSQL date range"
            ))
        })
}

/// PostgreSQL's `-infinity` and `infinity` dates have no Arrow date, as at
/// the query and predicate boundaries ([`pg_epoch_days_to_unix_days`]). Every
/// finite PostgreSQL date fits the shifted `i32`, so a valid `DateADT` fails
/// only as an infinity.
pub(crate) fn arrow_days_from_pg_date(
    pg_days: pg_sys::DateADT,
) -> ArrowConversionResult<i32> {
    pg_epoch_days_to_unix_days(pg_days).ok_or_else(|| {
        ArrowConversionError::DatetimeValueOutOfRange(
            "PostgreSQL date infinity has no Arrow date value".to_owned(),
        )
    })
}

pub(crate) fn time_from_micros(micros: i64) -> ArrowConversionResult<Time> {
    Time::try_from(micros).map_err(|_| {
        ArrowConversionError::DatetimeValueOutOfRange(format!(
            "time value {micros} microseconds is outside PostgreSQL time range"
        ))
    })
}

/// PG includes 24:00:00; Arrow Time excludes the end-of-day boundary.
/// All other PG time values already satisfy the Arrow range.
pub(crate) fn arrow_time_from_pg_time(
    micros: pg_sys::TimeADT,
) -> ArrowConversionResult<i64> {
    if micros == USECS_PER_DAY {
        return Err(ArrowConversionError::DatetimeValueOutOfRange(
            "PostgreSQL time 24:00:00 is outside the Arrow time range".to_owned(),
        ));
    }
    Ok(micros)
}

pub(crate) fn timestamp_from_unix_micros(
    unix_micros: i64,
) -> ArrowConversionResult<Timestamp> {
    unix_micros_to_pg_micros(unix_micros)
        .and_then(|pg_micros| Timestamp::try_from(pg_micros).ok())
        .ok_or_else(|| {
            ArrowConversionError::DatetimeValueOutOfRange(format!(
                "timestamp value {unix_micros} microseconds is outside PostgreSQL range"
            ))
        })
}

pub(crate) fn timestamptz_from_unix_micros(
    unix_micros: i64,
) -> ArrowConversionResult<TimestampWithTimeZone> {
    unix_micros_to_pg_micros(unix_micros)
        .and_then(|pg_micros| TimestampWithTimeZone::try_from(pg_micros).ok())
        .ok_or_else(|| {
            ArrowConversionError::DatetimeValueOutOfRange(format!(
                "timestamptz value {unix_micros} microseconds is outside PostgreSQL range"
            ))
        })
}

/// PostgreSQL's `-infinity` and `infinity` timestamps have no Arrow
/// timestamp, as at the query and predicate boundaries and in the Avro writer
/// ([`pg_epoch_micros_to_unix_micros`]). The nanosecond conversion goes
/// through this check.
pub(crate) fn unix_micros_from_timestamp(
    pg_micros: i64,
) -> ArrowConversionResult<i64> {
    pg_epoch_micros_to_unix_micros(pg_micros).ok_or_else(|| {
        ArrowConversionError::DatetimeValueOutOfRange(
            if matches!(pg_micros, pg_sys::Timestamp::MIN | pg_sys::Timestamp::MAX) {
                "PostgreSQL timestamp infinity has no Arrow timestamp value".to_owned()
            } else {
                format!(
                    "PostgreSQL timestamp value {pg_micros} microseconds overflows Unix epoch"
                )
            },
        )
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
        ArrowConversionError::DatetimeValueOutOfRange(format!(
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

/// Convert a Unix-epoch microsecond value to PostgreSQL-epoch microseconds,
/// shared by `timestamp` and `timestamptz` reads. Returns `None` when the
/// shift underflows `i64` or lands on the `-infinity` sentinel.
fn unix_micros_to_pg_micros(unix_micros: i64) -> Option<i64> {
    unix_micros
        .checked_sub(PG_EPOCH_USECS_DIFF)
        .filter(|&pg_micros| pg_micros != pg_sys::Timestamp::MIN)
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
