use std::time::Duration;

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::state::{MAX_RECONCILERS, MAX_WORKERS};

static MAX_REGISTRATIONS: GucSetting<i32> = GucSetting::<i32>::new(256);
static LAUNCHER_NAPTIME_MS: GucSetting<i32> = GucSetting::<i32>::new(1_000);
static RECONCILE_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static STOP_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(10_000);
static MAX_DATABASE_RECONCILERS: GucSetting<i32> = GucSetting::<i32>::new(4);

pub(crate) fn init() {
    GucRegistry::define_int_guc(
        c"pg_lakebase.max_worker_registrations",
        c"Maximum database-local worker registrations tracked by Lakebase",
        c"Bounds shared scheduling state; restart required.",
        &MAX_REGISTRATIONS,
        1,
        MAX_WORKERS as i32,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_lakebase.max_database_reconcilers",
        c"Maximum concurrent Lakebase database reconcilers",
        c"Bounds transient worker usage while databases are discovered.",
        &MAX_DATABASE_RECONCILERS,
        1,
        MAX_RECONCILERS as i32,
        GucContext::Sighup,
        GucFlags::default(),
    );
    define_ms(
        c"pg_lakebase.worker_launcher_naptime_ms",
        c"Lakebase worker launcher idle polling interval",
        &LAUNCHER_NAPTIME_MS,
        10,
        60_000,
        GucContext::Sighup,
    );
    define_ms(
        c"pg_lakebase.worker_reconcile_interval_ms",
        c"Lakebase worker full reconciliation safety interval",
        &RECONCILE_INTERVAL_MS,
        100,
        3_600_000,
        GucContext::Sighup,
    );
    define_ms(
        c"pg_lakebase.worker_stop_timeout_ms",
        c"Maximum wait for Lakebase workers to stop before DDL continues",
        &STOP_TIMEOUT_MS,
        100,
        300_000,
        GucContext::Sighup,
    );
}

fn define_ms(
    name: &'static std::ffi::CStr,
    description: &'static std::ffi::CStr,
    setting: &'static GucSetting<i32>,
    min: i32,
    max: i32,
    context: GucContext,
) {
    GucRegistry::define_int_guc(
        name,
        description,
        c"Value is measured in milliseconds.",
        setting,
        min,
        max,
        context,
        GucFlags::default(),
    );
}

pub(crate) fn max_registrations() -> usize {
    MAX_REGISTRATIONS.get() as usize
}

pub(crate) fn launcher_naptime() -> Duration {
    Duration::from_millis(LAUNCHER_NAPTIME_MS.get() as u64)
}

pub(crate) fn reconcile_interval() -> Duration {
    Duration::from_millis(RECONCILE_INTERVAL_MS.get() as u64)
}

pub(crate) fn stop_timeout() -> Duration {
    Duration::from_millis(STOP_TIMEOUT_MS.get() as u64)
}

pub(crate) fn max_database_reconcilers() -> usize {
    MAX_DATABASE_RECONCILERS.get() as usize
}
