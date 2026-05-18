//! GUC definitions for the pg-lakebase storage background worker.
//!
//! All GUCs use `GucContext::Postmaster`; changes require a PostgreSQL restart.

use std::ffi::CString;

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

static MAX_READ_SIZE: GucSetting<i32> = GucSetting::<i32>::new(1024 * 1024);

/// Periodic full-resync interval for the distributed tablespace store
/// reconciler, in milliseconds.
///
/// Syscache invalidation already wakes the reconciler on every
/// `pg_tablespace` change. The periodic resync exists only as a safety net
/// against missed invalidation messages or future bugs; setting it to `0`
/// disables periodic resync and relies entirely on syscache wake-ups.
static TABLESPACE_RECONCILE_INTERVAL_MS: GucSetting<i32> =
    GucSetting::<i32>::new(30_000);

pub fn init() {
    GucRegistry::define_bool_guc(
        c"pg_lakebase.storage_server.enabled",
        c"Start pg-lakebase-storage background worker",
        c"When true, a background worker running the local storage service is started at postmaster startup.",
        &ENABLED,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_lakebase.storage_server.socket_path",
        c"Unix socket path for pg-lakebase-storage",
        c"Absolute path to the Unix socket. Empty or unset means derive from DataDir.",
        &SOCKET_PATH,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_lakebase.storage_server.cache_dir",
        c"Cache directory for pg-lakebase-storage",
        c"Absolute path to the local cache directory. Empty or unset means derive from DataDir.",
        &CACHE_DIR,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_lakebase.storage_server.worker_threads",
        c"Number of Tokio worker threads for the storage server",
        c"Controls the size of the Tokio multi-thread runtime thread pool.",
        &WORKER_THREADS,
        1,
        256,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_lakebase.storage_server.shutdown_timeout_ms",
        c"Shutdown timeout in milliseconds for the storage worker",
        c"Maximum time to wait for in-flight connections during shutdown.",
        &SHUTDOWN_TIMEOUT_MS,
        100,
        60000,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_lakebase.storage_server.log_channel_capacity",
        c"Bounded log channel capacity for the storage worker",
        c"Number of log events buffered between Tokio threads and the PG log bridge.",
        &LOG_CHANNEL_CAPACITY,
        64,
        1_000_000,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_lakebase.storage_server.max_connections",
        c"Maximum concurrent connections to the storage server",
        c"Limits the number of simultaneously connected backends.",
        &MAX_CONNECTIONS,
        1,
        100_000,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_lakebase.storage_server.max_read_size",
        c"Maximum read size in bytes per storage request",
        c"Upper bound on data returned in a single storage read response.",
        &MAX_READ_SIZE,
        1,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_lakebase.storage_server.tablespace_reconcile_interval_ms",
        c"Periodic full-resync interval for the distributed tablespace store reconciler",
        c"How often the storage worker rescans pg_tablespace as a safety net behind syscache invalidation. 0 disables the periodic resync; syscache wake-ups still apply.",
        &TABLESPACE_RECONCILE_INTERVAL_MS,
        0,
        3_600_000,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

pub fn enabled() -> bool {
    ENABLED.get()
}

pub fn socket_path() -> Option<String> {
    SOCKET_PATH.get().and_then(|s| {
        let v = s.to_string_lossy().to_string();
        if v.is_empty() { None } else { Some(v) }
    })
}

pub fn cache_dir() -> Option<String> {
    CACHE_DIR.get().and_then(|s| {
        let v = s.to_string_lossy().to_string();
        if v.is_empty() { None } else { Some(v) }
    })
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

pub fn max_read_size() -> u32 {
    MAX_READ_SIZE.get().max(1) as u32
}

/// Periodic full-resync interval as `Some(Duration)`, or `None` when
/// `pg_lakebase.storage_server.tablespace_reconcile_interval_ms` is `0`.
pub fn tablespace_reconcile_interval() -> Option<std::time::Duration> {
    let raw = TABLESPACE_RECONCILE_INTERVAL_MS.get();
    if raw <= 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(raw as u64))
    }
}
