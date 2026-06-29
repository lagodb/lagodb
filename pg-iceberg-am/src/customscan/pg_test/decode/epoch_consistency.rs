//! Cross-crate epoch-consistency backend tests.
//!
//! Each test drives the SAME raw PG `Datum` through BOTH ends and asserts the
//! translator's pushed `Datum` equals the value produced by the write-side
//! epoch conversion — proving the two share one offset without needing a full
//! end-to-end table write+scan.
//!
//! They stay in `pg-iceberg-am` (they reference `decode_datum`, which is
//! AM-private) as `#[pgrx::pg_test]` — the translator side calls `decode_datum`,
//! whose text arm references PG backend symbols,
//! `pg_detoast_datum`, `palloc`), so the whole decode path requires a live
//! backend even though these tests only drive its temporal arms.
//!
//! The write-side offset is now asserted through the **public** epoch helpers
//! re-exported from `pg_arrow_conv` (`pg_epoch_days_to_unix_days` /
//! `pg_epoch_micros_to_unix_micros`). These are exactly the conversions the
//! write-side `TemporalCodec` applies; `TemporalCodec` itself is crate-private
//! in `pg_arrow_conv`, so the public helpers are the cross-crate surface that
//! proves the shared offset (see `pg-arrow-conv-extraction` design, Req 13.3).

#[pgrx::pg_schema]
mod tests {}

use iceberg_lite::spec::Datum;
use pg_arrow_conv::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
use pg_lakebase_core::tuple::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};
use pgrx::prelude::{Date, Timestamp, TimestampWithTimeZone};
use pgrx::{FromDatum, pg_sys};
use proptest::prelude::*;
use proptest::test_runner::TestRunner;

use crate::predicate::translator::decode_datum;

/// Buffer (in PG-epoch days) kept away from both ends of the `i32` range so
/// that (a) we never generate the `±infinity` date sentinels
/// (`i32::MIN` / `i32::MAX`) and (b) adding `PG_EPOCH_DAYS_DIFF` (10957)
/// can never overflow `i32`. Any value in the generated range is therefore
/// a finite, representable date at BOTH ends.
const DATE_GUARD: i32 = 20_000;

/// PostgreSQL's minimum valid finite `timestamp` / `timestamptz` value in
/// PG-epoch microseconds (`pgrx`'s `MIN_TIMESTAMP_USEC`, i.e. 4714-11-24 BC).
/// `Timestamp` / `TimestampWithTimeZone::from_datum` reject (panic on) any
/// value below this, so the generator must stay at or above it to model
/// only *representable* timestamps (the task's domain).
const MIN_PG_TS_USEC: i64 = -211_813_488_000_000_000;

/// PostgreSQL's maximum valid finite `timestamp` / `timestamptz` value in
/// PG-epoch microseconds (`pgrx`'s `MAX_TIMESTAMP_USEC`, i.e. 294276 AD).
const MAX_PG_TS_USEC: i64 = 9_223_371_331_199_999_999;

/// The largest PG-epoch micros value for which adding `PG_EPOCH_USECS_DIFF`
/// stays within `i64` (so BOTH ends produce a `Datum` rather than the
/// shared overflow → not-representable result).
const TS_OFFSET_SAFE_MAX: i64 = i64::MAX - PG_EPOCH_USECS_DIFF;

/// Upper bound of the timestamp generator: the tighter of PG's max valid
/// value and the offset-overflow-safe max.
const TS_GEN_MAX: i64 = if MAX_PG_TS_USEC < TS_OFFSET_SAFE_MAX {
    MAX_PG_TS_USEC
} else {
    TS_OFFSET_SAFE_MAX
};

/// Build a PG `date` `Datum` directly from a raw `DateADT` (PG-epoch days).
fn date_datum(pg_days: i32) -> pg_sys::Datum {
    pg_sys::Datum::from(pg_days)
}

/// Build a PG `timestamp` / `timestamptz` `Datum` from raw PG-epoch micros.
fn ts_datum(pg_micros: i64) -> pg_sys::Datum {
    pg_sys::Datum::from(pg_micros)
}

/// 256 cases per run (matching the repo's other PBTs, above the >=100
/// floor); no on-disk regression persistence inside the backend harness.
fn proptest_config() -> ProptestConfig {
    ProptestConfig {
        cases: 256,
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// At the Unix epoch the translator and the write side agree: PG-epoch day
/// `-PG_EPOCH_DAYS_DIFF` (1970-01-01) is iceberg day 0 on BOTH ends.
#[pgrx::pg_test(schema = "tests")]
fn date_epoch_consistency_at_unix_epoch() {
    let pg_days = -PG_EPOCH_DAYS_DIFF;
    let datum = date_datum(pg_days);

    let pushed = unsafe { decode_datum(pg_sys::DATEOID, datum) }
        .expect("epoch date must decode on the translator side");

    let date = unsafe { Date::from_datum(datum, false) }
        .expect("epoch date must decode into a pgrx Date");
    let write_arrow_days = pg_epoch_days_to_unix_days(date.to_pg_epoch_days())
        .expect("epoch date must encode on the write side");

    assert_eq!(write_arrow_days, 0, "Unix epoch must be iceberg day 0");
    assert_eq!(
        pushed,
        Datum::date(write_arrow_days),
        "pushed date bound must equal the write side's stored bound",
    );
}

/// At the Unix epoch the timestamp translator and write side agree on micros
/// 0 (PG-epoch micros `-PG_EPOCH_USECS_DIFF`).
#[pgrx::pg_test(schema = "tests")]
fn timestamp_epoch_consistency_at_unix_epoch() {
    let pg_micros = -PG_EPOCH_USECS_DIFF;
    let datum = ts_datum(pg_micros);

    let pushed = unsafe { decode_datum(pg_sys::TIMESTAMPOID, datum) }
        .expect("epoch timestamp must decode on the translator side");

    let ts = unsafe { Timestamp::from_datum(datum, false) }
        .expect("epoch timestamp must decode into a pgrx Timestamp");
    let write_unix_micros = pg_epoch_micros_to_unix_micros(ts.into())
        .expect("epoch timestamp must encode on the write side");

    assert_eq!(write_unix_micros, 0, "Unix epoch must be 0 micros");
    assert_eq!(
        pushed,
        Datum::timestamp_micros(write_unix_micros),
        "pushed timestamp bound must equal the write side's stored bound",
    );
}

#[pgrx::pg_test(schema = "tests")]
fn pushed_date_bound_matches_write_side_offset() {
    let mut runner = TestRunner::new(proptest_config());
    runner
        .run(
            &((i32::MIN + DATE_GUARD)..=(i32::MAX - DATE_GUARD)),
            |pg_days| {
                let datum = date_datum(pg_days);

                let pushed = unsafe { decode_datum(pg_sys::DATEOID, datum) }.expect(
                    "a representable date must decode on the translator side",
                );

                let date = unsafe { Date::from_datum(datum, false) }
                    .expect("a representable date must decode into a pgrx Date");
                let write_arrow_days =
                    pg_epoch_days_to_unix_days(date.to_pg_epoch_days())
                        .expect("a representable date must encode on the write side");

                prop_assert_eq!(
                    write_arrow_days,
                    pg_epoch_days_to_unix_days(pg_days)
                        .expect("offset must not overflow for a guarded value"),
                );
                prop_assert_eq!(pushed, Datum::date(write_arrow_days));
                Ok(())
            },
        )
        .expect("pushed `date` bound must match the write-side offset");
}

#[pgrx::pg_test(schema = "tests")]
fn pushed_timestamp_bound_matches_write_side_offset() {
    let mut runner = TestRunner::new(proptest_config());
    runner
        .run(&(MIN_PG_TS_USEC..=TS_GEN_MAX), |pg_micros| {
            let datum = ts_datum(pg_micros);

            let pushed = unsafe { decode_datum(pg_sys::TIMESTAMPOID, datum) }.expect(
                "a representable timestamp must decode on the translator side",
            );

            let ts = unsafe { Timestamp::from_datum(datum, false) }.expect(
                "a representable timestamp must decode into a pgrx Timestamp",
            );
            let write_unix_micros = pg_epoch_micros_to_unix_micros(ts.into())
                .expect("a representable timestamp must encode on the write side");

            prop_assert_eq!(
                write_unix_micros,
                pg_epoch_micros_to_unix_micros(pg_micros)
                    .expect("offset must not overflow for a guarded value"),
            );
            prop_assert_eq!(pushed, Datum::timestamp_micros(write_unix_micros));
            Ok(())
        })
        .expect("pushed `timestamp` bound must match the write-side offset");
}

#[pgrx::pg_test(schema = "tests")]
fn pushed_timestamptz_bound_matches_write_side_offset() {
    let mut runner = TestRunner::new(proptest_config());
    runner
        .run(&(MIN_PG_TS_USEC..=TS_GEN_MAX), |pg_micros| {
            let datum = ts_datum(pg_micros);

            let pushed = unsafe { decode_datum(pg_sys::TIMESTAMPTZOID, datum) }
                .expect(
                    "a representable timestamptz must decode on the translator side",
                );

            let ts = unsafe { TimestampWithTimeZone::from_datum(datum, false) }
                .expect("a representable timestamptz must decode into a pgrx value");
            let write_unix_micros = pg_epoch_micros_to_unix_micros(ts.into())
                .expect("a representable timestamptz must encode on the write side");

            prop_assert_eq!(
                write_unix_micros,
                pg_epoch_micros_to_unix_micros(pg_micros)
                    .expect("offset must not overflow for a guarded value"),
            );
            prop_assert_eq!(pushed, Datum::timestamptz_micros(write_unix_micros));
            Ok(())
        })
        .expect("pushed `timestamptz` bound must match the write-side offset");
}
