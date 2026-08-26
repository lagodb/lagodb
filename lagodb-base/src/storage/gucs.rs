//! GUC definitions for the pg-lakebase storage background worker.
//!
//! Most GUCs use `GucContext::Postmaster` (require a PostgreSQL restart).
//! Runtime-tunable parameters use `GucContext::Sighup` and take effect after
//! `pg_reload_conf()` or a SIGHUP signal.

use std::ffi::CString;

use pg_lakebase_storage::{
    DEFAULT_CACHE_CLEANUP_BATCH_BYTES, DEFAULT_CACHE_CLEANUP_BATCH_ITEMS,
    DEFAULT_CACHE_CLEANUP_INTERVAL, DEFAULT_CACHE_CLEANUP_START_PERCENT,
    DEFAULT_CACHE_CLEANUP_TARGET_PERCENT, DEFAULT_CACHE_TOUCH_GRANULARITY,
};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);

static SOCKET_PATH: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

static CACHE_DIR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

static WORKER_THREADS: GucSetting<i32> = GucSetting::<i32>::new(4);

static SHUTDOWN_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);

static LOG_CHANNEL_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(4096);

static MAX_CONNECTIONS: GucSetting<i32> = GucSetting::<i32>::new(1024);

static BACKEND_MAX_IDLE_CONNECTIONS: GucSetting<i32> = GucSetting::<i32>::new(8);

static MAX_READ_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1024 * 1024);

/// Grace period retained after a tablespace-backed volume is dropped.
static STORAGE_VOLUME_RETIREMENT_GRACE_PERIOD_SECONDS: GucSetting<i32> =
    GucSetting::<i32>::new(604_800);

// --- Cache runtime GUCs (Sighup-reloadable) ---

static CACHE_TOUCH_GRANULARITY_MS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_CACHE_TOUCH_GRANULARITY.as_millis() as i32);
static CACHE_MAX_MB: GucSetting<i32> = GucSetting::<i32>::new(0);
static CACHE_CLEANUP_START_PERCENT: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_CACHE_CLEANUP_START_PERCENT as i32);
static CACHE_CLEANUP_TARGET_PERCENT: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_CACHE_CLEANUP_TARGET_PERCENT as i32);
static CACHE_CLEANUP_INTERVAL_MS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_CACHE_CLEANUP_INTERVAL.as_millis() as i32);
static CACHE_CLEANUP_BATCH_ITEMS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_CACHE_CLEANUP_BATCH_ITEMS as i32);
static CACHE_CLEANUP_BATCH_MB: GucSetting<i32> = GucSetting::<i32>::new(
    (DEFAULT_CACHE_CLEANUP_BATCH_BYTES / (1024 * 1024)) as i32,
);

pub fn init() {
    GucRegistry::define_bool_guc(
        c"lagodb.storage_server_enabled",
        c"Start pg-lakebase-storage background worker",
        c"When true, a background worker running the local storage service is started at postmaster startup.",
        &ENABLED,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"lagodb.storage_server_socket_path",
        c"Unix socket path for pg-lakebase-storage",
        c"Absolute path to the Unix socket. Empty or unset means derive from DataDir.",
        &SOCKET_PATH,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"lagodb.storage_server_cache_dir",
        c"Cache directory for pg-lakebase-storage",
        c"Absolute path to the local cache directory. Empty or unset means derive from DataDir.",
        &CACHE_DIR,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_worker_threads",
        c"Number of Tokio worker threads for the storage server",
        c"Controls the size of the Tokio multi-thread runtime thread pool.",
        &WORKER_THREADS,
        1,
        256,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_shutdown_timeout_ms",
        c"Shutdown timeout in milliseconds for the storage worker",
        c"Maximum time to wait for in-flight connections during shutdown.",
        &SHUTDOWN_TIMEOUT_MS,
        100,
        60000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_log_channel_capacity",
        c"Bounded log channel capacity for the storage worker",
        c"Number of log events buffered between Tokio threads and the PG log bridge.",
        &LOG_CHANNEL_CAPACITY,
        64,
        1_000_000,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_max_connections",
        c"Maximum concurrent connections to the storage server",
        c"Limits the number of simultaneously connected backends.",
        &MAX_CONNECTIONS,
        1,
        100_000,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_max_read_size",
        c"Maximum read size in bytes per storage request",
        c"Upper bound on data returned in a single storage read response.",
        &MAX_READ_SIZE,
        1,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_backend_max_idle_connections",
        c"Maximum idle storage connections cached by each PostgreSQL backend",
        c"Bounds reusable per-context sockets without closing active file handles.",
        &BACKEND_MAX_IDLE_CONNECTIONS,
        1,
        1024,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_volume_retirement_grace_period_seconds",
        c"Grace period for retired storage volumes",
        c"How long a dropped tablespace volume remains available to maintenance before its configuration is purged.",
        &STORAGE_VOLUME_RETIREMENT_GRACE_PERIOD_SECONDS,
        1,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );

    // --- Cache runtime GUCs (Sighup-reloadable) ---

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_cache_touch_granularity_ms",
        c"Minimum interval between cache access-time updates for a single object",
        c"Prevents excessive write I/O from frequent access-time touches. Set to 0 to touch on every access.",
        &CACHE_TOUCH_GRANULARITY_MS,
        0,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_cache_max_mb",
        c"Maximum cache size in MiB",
        c"Capacity limit for the local object cache in mebibytes. 0 disables capacity-based cleanup.",
        &CACHE_MAX_MB,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_cache_cleanup_start_percent",
        c"Cache usage percentage that triggers cleanup",
        c"When resident bytes exceed this fraction of cache_max_mb, cleanup begins.",
        &CACHE_CLEANUP_START_PERCENT,
        1,
        100,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_cache_cleanup_target_percent",
        c"Target cache usage percentage after cleanup",
        c"Cleanup evicts until resident bytes drop below this fraction of cache_max_mb.",
        &CACHE_CLEANUP_TARGET_PERCENT,
        0,
        100,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_cache_cleanup_interval_ms",
        c"Periodic cache cleanup interval in milliseconds",
        c"How often the background cleanup task runs. 0 disables periodic cleanup.",
        &CACHE_CLEANUP_INTERVAL_MS,
        0,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_cache_cleanup_batch_items",
        c"Maximum number of items evicted per cleanup batch",
        c"Limits work per cleanup iteration to avoid blocking cache access too long.",
        &CACHE_CLEANUP_BATCH_ITEMS,
        1,
        1_000_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb.storage_server_cache_cleanup_batch_mb",
        c"Maximum MiB evicted per cleanup batch",
        c"Limits I/O work per cleanup iteration, in mebibytes.",
        &CACHE_CLEANUP_BATCH_MB,
        1,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

pub fn enabled() -> bool {
    ENABLED.get()
}

pub fn socket_path() -> Option<String> {
    SOCKET_PATH.get().and_then(non_empty_lossy_string)
}

pub fn cache_dir() -> Option<String> {
    CACHE_DIR.get().and_then(non_empty_lossy_string)
}

/// Convert a GUC `CString` into an owned `String`, returning `None` for
/// empty values. Skips the `to_string_lossy().into_owned()` conversion when
/// the value is empty so the common (unset) GUC path does no extra heap
/// work inside this helper. The outer `GucSetting::<Option<CString>>::get`
/// already owns its own `CString` clone, which we cannot influence.
fn non_empty_lossy_string(value: CString) -> Option<String> {
    if value.as_bytes().is_empty() {
        None
    } else {
        Some(value.to_string_lossy().into_owned())
    }
}

pub fn worker_threads() -> usize {
    WORKER_THREADS.get().max(1) as usize
}

pub fn shutdown_timeout_ms() -> u64 {
    SHUTDOWN_TIMEOUT_MS.get().max(100) as u64
}

pub fn log_channel_capacity() -> usize {
    LOG_CHANNEL_CAPACITY.get().max(64) as usize
}

pub fn max_connections() -> usize {
    MAX_CONNECTIONS.get().max(1) as usize
}

pub fn backend_max_idle_connections() -> usize {
    BACKEND_MAX_IDLE_CONNECTIONS.get().max(1) as usize
}

pub fn max_read_size() -> u32 {
    MAX_READ_SIZE.get().max(1) as u32
}

pub fn storage_volume_retirement_grace_period_ms() -> u64 {
    let seconds = u64::try_from(STORAGE_VOLUME_RETIREMENT_GRACE_PERIOD_SECONDS.get())
        .expect("storage volume retirement grace period GUC is positive");
    seconds
        .checked_mul(1_000)
        .expect("storage volume retirement grace period fits in milliseconds")
}

pub fn cache_touch_granularity() -> std::time::Duration {
    std::time::Duration::from_millis(CACHE_TOUCH_GRANULARITY_MS.get().max(0) as u64)
}

/// Returns `None` (disabled) when the value is `0`; otherwise `Some(bytes)`
/// converted from MiB.
pub fn cache_max_bytes() -> Option<u64> {
    let raw = CACHE_MAX_MB.get();
    if raw <= 0 {
        None
    } else {
        Some((raw as u64) * 1024 * 1024)
    }
}

pub fn cache_cleanup_start_percent() -> u8 {
    CACHE_CLEANUP_START_PERCENT.get().clamp(1, 100) as u8
}

pub fn cache_cleanup_target_percent() -> u8 {
    CACHE_CLEANUP_TARGET_PERCENT.get().clamp(0, 100) as u8
}

/// Returns `None` (disabled) when the value is `0`; otherwise `Some(duration)`.
pub fn cache_cleanup_interval() -> Option<std::time::Duration> {
    let raw = CACHE_CLEANUP_INTERVAL_MS.get();
    if raw <= 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(raw as u64))
    }
}

pub fn cache_cleanup_batch_items() -> usize {
    CACHE_CLEANUP_BATCH_ITEMS.get().max(1) as usize
}

pub fn cache_cleanup_batch_bytes() -> u64 {
    (CACHE_CLEANUP_BATCH_MB.get().max(1) as u64) * 1024 * 1024
}
