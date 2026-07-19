use std::time::Duration;

use pg_lakebase_storage::protocol::{
    MAX_BULK_DELETE_OBJECT_KEYS, MAX_LIST_PAGE_SIZE,
};
use pg_lakebase_storage::{
    DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS, LIST_CURSOR_IDLE_TTL_MS,
};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum};

use crate::state::{MAX_RECONCILERS, MAX_WORKERS};

static MAX_REGISTRATIONS: GucSetting<i32> = GucSetting::<i32>::new(256);
static LAUNCHER_NAPTIME_MS: GucSetting<i32> = GucSetting::<i32>::new(1_000);
static RECONCILE_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(30_000);
static STOP_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(10_000);
static MAX_DATABASE_RECONCILERS: GucSetting<i32> = GucSetting::<i32>::new(4);
static MAINTENANCE_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static MAINTENANCE_ACTOR_THREADS: GucSetting<i32> = GucSetting::<i32>::new(1);
static MAINTENANCE_BATCH_ITEMS: GucSetting<i32> = GucSetting::<i32>::new(128);
static MAINTENANCE_RETRY_BASE_MS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS + 1_000);
static MAINTENANCE_RETRY_MAX_MS: GucSetting<i32> = GucSetting::<i32>::new(300_000);
static MAINTENANCE_RETRY_MAX_ATTEMPTS: GucSetting<i32> = GucSetting::<i32>::new(32);
static MAINTENANCE_REQUEST_TIMEOUT_MS: GucSetting<i32> =
    GucSetting::<i32>::new(30_000);
static MAINTENANCE_SHUTDOWN_TIMEOUT_MS: GucSetting<i32> =
    GucSetting::<i32>::new(30_000);
static VACUUM_MAX_INPUT_OBJECTS: GucSetting<i32> = GucSetting::<i32>::new(1_000);
static VACUUM_MAX_INPUT_MB: GucSetting<i32> = GucSetting::<i32>::new(4_096);
static VACUUM_MAX_GROUP_OBJECTS: GucSetting<i32> = GucSetting::<i32>::new(10_000);
static VACUUM_MAX_GROUP_MB: GucSetting<i32> = GucSetting::<i32>::new(4_096);
static CUSTOMSCAN_MODE: GucSetting<CustomScanMode> =
    GucSetting::<CustomScanMode>::new(CustomScanMode::Auto);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PostgresGucEnum)]
enum CustomScanMode {
    Off,
    Auto,
    Force,
}

pub(crate) fn customscan_mode_code() -> u32 {
    match CUSTOMSCAN_MODE.get() {
        CustomScanMode::Off => 0,
        CustomScanMode::Auto => 1,
        CustomScanMode::Force => 2,
    }
}

pub(crate) fn maintenance_config()
-> pg_lakebase_core::runtime_api::RuntimeMaintenanceConfigV1 {
    pg_lakebase_core::runtime_api::RuntimeMaintenanceConfigV1 {
        enabled: u8::from(MAINTENANCE_ENABLED.get()),
        _padding: [0; 3],
        actor_threads: MAINTENANCE_ACTOR_THREADS.get(),
        batch_items: MAINTENANCE_BATCH_ITEMS.get(),
        retry_base_ms: MAINTENANCE_RETRY_BASE_MS.get(),
        retry_max_ms: MAINTENANCE_RETRY_MAX_MS.get(),
        retry_max_attempts: MAINTENANCE_RETRY_MAX_ATTEMPTS.get(),
        request_timeout_ms: MAINTENANCE_REQUEST_TIMEOUT_MS.get(),
        shutdown_timeout_ms: MAINTENANCE_SHUTDOWN_TIMEOUT_MS.get(),
        vacuum_max_input_objects: VACUUM_MAX_INPUT_OBJECTS.get(),
        vacuum_max_input_mb: VACUUM_MAX_INPUT_MB.get(),
        vacuum_max_group_objects: VACUUM_MAX_GROUP_OBJECTS.get(),
        vacuum_max_group_mb: VACUUM_MAX_GROUP_MB.get(),
    }
}

pub(crate) fn init() {
    init_shared_framework_gucs();
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

fn init_shared_framework_gucs() {
    const MAX_BATCH_ITEMS: i32 =
        if MAX_BULK_DELETE_OBJECT_KEYS < MAX_LIST_PAGE_SIZE as usize {
            MAX_BULK_DELETE_OBJECT_KEYS as i32
        } else {
            MAX_LIST_PAGE_SIZE as i32
        };
    const MIN_RETRY_MS: i32 = DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS + 1_000;
    const MAX_REQUEST_TIMEOUT_MS: i32 = LIST_CURSOR_IDLE_TTL_MS * 4 / 5;

    GucRegistry::define_enum_guc(
        c"pg_lakebase.customscan_mode",
        c"Path-emission mode for the pg-lakebase CustomScan framework",
        c"off disables paths, auto uses cost, and force biases legal paths for tests.",
        &CUSTOMSCAN_MODE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_lakebase.maintenance_worker_enabled",
        c"Start the Lakebase maintenance background worker",
        c"Runs the format-neutral durable physical maintenance queue consumer.",
        &MAINTENANCE_ENABLED,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_lakebase.maintenance_actor_threads",
        c"Number of maintenance storage actors",
        c"Each actor owns one independent storage connection; restart required.",
        &MAINTENANCE_ACTOR_THREADS,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_lakebase.maintenance_batch_items",
        c"Maximum physical objects processed in one maintenance round",
        c"Bounds list-page memory, delete work, and producer batch size.",
        &MAINTENANCE_BATCH_ITEMS,
        1,
        MAX_BATCH_ITEMS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    define_ms(
        c"pg_lakebase.maintenance_retry_base_ms",
        c"Initial maintenance retry delay",
        &MAINTENANCE_RETRY_BASE_MS,
        MIN_RETRY_MS,
        3_600_000,
        GucContext::Sighup,
    );
    define_ms(
        c"pg_lakebase.maintenance_retry_max_ms",
        c"Maximum maintenance retry delay",
        &MAINTENANCE_RETRY_MAX_MS,
        MIN_RETRY_MS,
        86_400_000,
        GucContext::Sighup,
    );
    GucRegistry::define_int_guc(
        c"pg_lakebase.maintenance_retry_max_attempts",
        c"Maximum attempts before a maintenance item is failed",
        c"A failed item requires an explicit operator retry.",
        &MAINTENANCE_RETRY_MAX_ATTEMPTS,
        1,
        1_000_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    define_ms(
        c"pg_lakebase.maintenance_request_timeout_ms",
        c"Storage RPC timeout for maintenance work",
        &MAINTENANCE_REQUEST_TIMEOUT_MS,
        100,
        MAX_REQUEST_TIMEOUT_MS,
        GucContext::Sighup,
    );
    define_ms(
        c"pg_lakebase.maintenance_shutdown_timeout_ms",
        c"Graceful maintenance actor shutdown deadline",
        &MAINTENANCE_SHUTDOWN_TIMEOUT_MS,
        100,
        3_600_000,
        GucContext::Sighup,
    );
    define_budget(
        c"pg_lakebase.vacuum_max_input_objects",
        c"Maximum selected input objects for ordinary provider VACUUM",
        &VACUUM_MAX_INPUT_OBJECTS,
    );
    define_budget(
        c"pg_lakebase.vacuum_max_input_mb",
        c"Maximum selected input MiB for ordinary provider VACUUM",
        &VACUUM_MAX_INPUT_MB,
    );
    define_budget(
        c"pg_lakebase.vacuum_max_group_objects",
        c"Maximum input objects in one provider maintenance group",
        &VACUUM_MAX_GROUP_OBJECTS,
    );
    define_budget(
        c"pg_lakebase.vacuum_max_group_mb",
        c"Maximum input MiB in one provider maintenance group",
        &VACUUM_MAX_GROUP_MB,
    );
}

fn define_budget(
    name: &'static std::ffi::CStr,
    description: &'static std::ffi::CStr,
    setting: &'static GucSetting<i32>,
) {
    GucRegistry::define_int_guc(
        name,
        description,
        c"VACUUM FULL ignores soft input limits but retains hard group limits.",
        setting,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
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
