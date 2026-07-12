//! GUCs for the independent Lakebase maintenance worker.

use std::ffi::CString;
use std::time::Duration;

use pg_lakebase_storage::protocol::{
    MAX_BULK_DELETE_OBJECT_KEYS, MAX_LIST_PAGE_SIZE,
};
use pg_lakebase_storage::{
    DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS, LIST_CURSOR_IDLE_TTL_MS,
};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static ACTOR_THREADS: GucSetting<i32> = GucSetting::<i32>::new(4);
static NAPTIME_MS: GucSetting<i32> = GucSetting::<i32>::new(1_000);
static BATCH_ITEMS: GucSetting<i32> = GucSetting::<i32>::new(128);
static RETRY_BASE_MS: GucSetting<i32> =
    GucSetting::<i32>::new(MIN_MAINTENANCE_RETRY_DELAY_MS);
static RETRY_MAX_MS: GucSetting<i32> = GucSetting::<i32>::new(300_000);
static RETRY_MAX_ATTEMPTS: GucSetting<i32> = GucSetting::<i32>::new(32);
static REQUEST_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static SHUTDOWN_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);

const MAX_MAINTENANCE_BATCH_ITEMS: i32 =
    if MAX_BULK_DELETE_OBJECT_KEYS < MAX_LIST_PAGE_SIZE as usize {
        MAX_BULK_DELETE_OBJECT_KEYS as i32
    } else {
        MAX_LIST_PAGE_SIZE as i32
    };

// Leave margin below the storage list cursor idle TTL so a slow page delete
// cannot expire the cursor before the next list-page request.
const MAX_MAINTENANCE_REQUEST_TIMEOUT_MS: i32 = LIST_CURSOR_IDLE_TTL_MS * 4 / 5;

const STORAGE_CONNECTION_DRAIN_TIMEOUT_MS: i32 = DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS;
const MIN_MAINTENANCE_RETRY_DELAY_MS: i32 =
    STORAGE_CONNECTION_DRAIN_TIMEOUT_MS + 1_000;

pub(crate) fn init() {
    GucRegistry::define_bool_guc(
        c"pg_lakebase.maintenance_worker_enabled",
        c"Start the Lakebase maintenance background worker",
        c"Runs the format-neutral durable physical maintenance queue consumer.",
        &ENABLED,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_lakebase.maintenance_database",
        c"Database containing the Lakebase maintenance queue",
        c"Defaults to postgres when unset.",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_lakebase.maintenance_actor_threads",
        c"Number of maintenance storage actors",
        c"Each actor owns one independent storage connection; restart required.",
        &ACTOR_THREADS,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    define_ms(
        c"pg_lakebase.maintenance_naptime_ms",
        c"Maintenance worker idle polling interval",
        &NAPTIME_MS,
        10,
        3_600_000,
    );
    GucRegistry::define_int_guc(
        c"pg_lakebase.maintenance_batch_items",
        c"Maximum physical objects processed in one maintenance round",
        c"Bounds list-page memory, delete work, and producer batch size.",
        &BATCH_ITEMS,
        1,
        MAX_MAINTENANCE_BATCH_ITEMS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    define_ms(
        c"pg_lakebase.maintenance_retry_base_ms",
        c"Initial maintenance retry delay; must exceed storage drain timeout",
        &RETRY_BASE_MS,
        MIN_MAINTENANCE_RETRY_DELAY_MS,
        3_600_000,
    );
    GucRegistry::define_int_guc(
        c"pg_lakebase.maintenance_retry_max_attempts",
        c"Maximum attempts before a maintenance item is failed",
        c"A failed item requires an explicit operator retry.",
        &RETRY_MAX_ATTEMPTS,
        1,
        1_000_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    define_ms(
        c"pg_lakebase.maintenance_retry_max_ms",
        c"Maximum maintenance retry delay",
        &RETRY_MAX_MS,
        MIN_MAINTENANCE_RETRY_DELAY_MS,
        86_400_000,
    );
    define_ms(
        c"pg_lakebase.maintenance_request_timeout_ms",
        c"Storage RPC timeout for maintenance work",
        &REQUEST_TIMEOUT_MS,
        100,
        MAX_MAINTENANCE_REQUEST_TIMEOUT_MS,
    );
    define_ms(
        c"pg_lakebase.maintenance_shutdown_timeout_ms",
        c"Graceful maintenance actor shutdown deadline",
        &SHUTDOWN_TIMEOUT_MS,
        100,
        3_600_000,
    );
}

fn define_ms(
    name: &'static std::ffi::CStr,
    description: &'static std::ffi::CStr,
    setting: &'static GucSetting<i32>,
    min: i32,
    max: i32,
) {
    GucRegistry::define_int_guc(
        name,
        description,
        c"Value is measured in milliseconds.",
        setting,
        min,
        max,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

pub(crate) fn enabled() -> bool {
    ENABLED.get()
}

pub(crate) fn database() -> String {
    DATABASE
        .get()
        .map(|value| value.to_string_lossy().into_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "postgres".to_owned())
}

pub(crate) fn naptime() -> Duration {
    Duration::from_millis(NAPTIME_MS.get() as u64)
}

pub(crate) fn actor_threads() -> usize {
    ACTOR_THREADS.get() as usize
}

pub(crate) fn batch_items() -> usize {
    BATCH_ITEMS.get() as usize
}

pub(crate) fn retry_base_ms() -> u64 {
    RETRY_BASE_MS.get() as u64
}

pub(crate) fn retry_max_ms() -> u64 {
    RETRY_MAX_MS.get() as u64
}

pub(crate) fn retry_max_attempts() -> i32 {
    RETRY_MAX_ATTEMPTS.get()
}

pub(crate) fn request_timeout() -> Duration {
    Duration::from_millis(REQUEST_TIMEOUT_MS.get() as u64)
}

pub(crate) fn shutdown_timeout() -> Duration {
    Duration::from_millis(SHUTDOWN_TIMEOUT_MS.get() as u64)
}
