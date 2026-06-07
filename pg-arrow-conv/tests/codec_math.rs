//! Host tests for the public PG↔Unix epoch helpers. Pure integer arithmetic,
//! no PostgreSQL backend required.

use pg_arrow_conv::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
use pg_lakebase_core::tuple::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};

// --- pg_epoch_days_to_unix_days ----------------------------------------------

#[test]
fn days_helper_applies_the_shared_offset() {
    // PG epoch (2000-01-01) day 0 maps to the Unix-epoch day offset.
    assert_eq!(pg_epoch_days_to_unix_days(0), Some(PG_EPOCH_DAYS_DIFF));
    // A PG day equal to the negated offset lands exactly on the Unix epoch.
    assert_eq!(pg_epoch_days_to_unix_days(-PG_EPOCH_DAYS_DIFF), Some(0));
}

#[test]
fn days_helper_round_trips_through_the_inverse_offset() {
    // Forward (PG -> Unix) then reverse (subtract the offset) is the identity
    // for every value that does not overflow `i32`.
    for pg_days in [
        i32::MIN + PG_EPOCH_DAYS_DIFF,
        -1_000_000,
        -PG_EPOCH_DAYS_DIFF,
        -1,
        0,
        1,
        PG_EPOCH_DAYS_DIFF,
        1_000_000,
        i32::MAX - PG_EPOCH_DAYS_DIFF,
    ] {
        let unix_days = pg_epoch_days_to_unix_days(pg_days)
            .expect("value chosen to not overflow i32");
        assert_eq!(
            unix_days - PG_EPOCH_DAYS_DIFF,
            pg_days,
            "round-trip failed for pg_days={pg_days}"
        );
    }
}

#[test]
fn days_helper_returns_none_on_overflow() {
    // PG_EPOCH_DAYS_DIFF is positive, so the maximum representable PG day plus
    // the offset overflows `i32`.
    assert_eq!(pg_epoch_days_to_unix_days(i32::MAX), None);
}

// --- pg_epoch_micros_to_unix_micros ------------------------------------------

#[test]
fn micros_helper_applies_the_shared_offset() {
    assert_eq!(pg_epoch_micros_to_unix_micros(0), Some(PG_EPOCH_USECS_DIFF));
    assert_eq!(
        pg_epoch_micros_to_unix_micros(-PG_EPOCH_USECS_DIFF),
        Some(0)
    );
}

#[test]
fn micros_helper_round_trips_through_the_inverse_offset() {
    for pg_micros in [
        i64::MIN + PG_EPOCH_USECS_DIFF,
        -1_000_000_000,
        -PG_EPOCH_USECS_DIFF,
        -1,
        0,
        1,
        PG_EPOCH_USECS_DIFF,
        1_000_000_000,
        i64::MAX - PG_EPOCH_USECS_DIFF,
    ] {
        let unix_micros = pg_epoch_micros_to_unix_micros(pg_micros)
            .expect("value chosen to not overflow i64");
        assert_eq!(
            unix_micros - PG_EPOCH_USECS_DIFF,
            pg_micros,
            "round-trip failed for pg_micros={pg_micros}"
        );
    }
}

#[test]
fn micros_helper_returns_none_on_overflow() {
    assert_eq!(pg_epoch_micros_to_unix_micros(i64::MAX), None);
}
