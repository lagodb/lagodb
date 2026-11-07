//! GUCs published to providers as the runtime maintenance configuration.

use std::ffi::CStr;

use lagodb_core::runtime_api::RuntimeMaintenanceConfig;
use lagodb_storage::{
    DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS, LIST_CURSOR_IDLE_TTL_MS,
    MAX_BULK_DELETE_OBJECT_KEYS, MAX_LIST_PAGE_SIZE,
};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

const MAX_BATCH_ITEMS: i32 =
    if MAX_BULK_DELETE_OBJECT_KEYS < MAX_LIST_PAGE_SIZE as usize {
        MAX_BULK_DELETE_OBJECT_KEYS as i32
    } else {
        MAX_LIST_PAGE_SIZE as i32
    };
const MIN_RETRY_MS: i32 = DEFAULT_CONNECTION_DRAIN_TIMEOUT_MS + 1_000;
const MAX_REQUEST_TIMEOUT_MS: i32 = LIST_CURSOR_IDLE_TTL_MS * 4 / 5;

static MAINTENANCE_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static MAINTENANCE_ACTOR_THREADS: GucSetting<i32> = GucSetting::<i32>::new(1);
static MAINTENANCE_BATCH_ITEMS: GucSetting<i32> = GucSetting::<i32>::new(128);
static MAINTENANCE_RETRY_BASE_MS: GucSetting<i32> =
    GucSetting::<i32>::new(MIN_RETRY_MS);
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

/// The configuration a provider reads through the runtime ABI.
pub(super) fn runtime_config() -> RuntimeMaintenanceConfig {
    RuntimeMaintenanceConfig {
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

pub(super) fn define() {
    GucRegistry::define_bool_guc(
        c"lagodb.maintenance_worker_enabled",
        c"Start the LagoDB maintenance background worker",
        c"Runs the format-neutral durable physical maintenance queue consumer.",
        &MAINTENANCE_ENABLED,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.maintenance_actor_threads",
        c"Number of maintenance storage actors",
        c"Each actor owns one independent storage connection; restart required.",
        &MAINTENANCE_ACTOR_THREADS,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.maintenance_batch_items",
        c"Maximum physical objects processed in one maintenance round",
        c"Bounds list-page memory, delete work, and producer batch size.",
        &MAINTENANCE_BATCH_ITEMS,
        1,
        MAX_BATCH_ITEMS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.maintenance_retry_base_ms",
        c"Initial maintenance retry delay",
        c"Value is measured in milliseconds.",
        &MAINTENANCE_RETRY_BASE_MS,
        MIN_RETRY_MS,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.maintenance_retry_max_ms",
        c"Maximum maintenance retry delay",
        c"Value is measured in milliseconds.",
        &MAINTENANCE_RETRY_MAX_MS,
        MIN_RETRY_MS,
        86_400_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.maintenance_retry_max_attempts",
        c"Maximum attempts before a maintenance item is failed",
        c"A failed item requires an explicit operator retry.",
        &MAINTENANCE_RETRY_MAX_ATTEMPTS,
        1,
        1_000_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.maintenance_request_timeout_ms",
        c"Storage RPC timeout for maintenance work",
        c"Value is measured in milliseconds.",
        &MAINTENANCE_REQUEST_TIMEOUT_MS,
        100,
        MAX_REQUEST_TIMEOUT_MS,
        GucContext::Sighup,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.maintenance_shutdown_timeout_ms",
        c"Graceful maintenance actor shutdown deadline",
        c"Value is measured in milliseconds.",
        &MAINTENANCE_SHUTDOWN_TIMEOUT_MS,
        100,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );
    define_budget(
        c"lagodb.vacuum_max_input_objects",
        c"Maximum selected input objects for ordinary provider VACUUM",
        &VACUUM_MAX_INPUT_OBJECTS,
    );
    define_budget(
        c"lagodb.vacuum_max_input_mb",
        c"Maximum selected input MiB for ordinary provider VACUUM",
        &VACUUM_MAX_INPUT_MB,
    );
    define_budget(
        c"lagodb.vacuum_max_group_objects",
        c"Maximum input objects in one provider maintenance group",
        &VACUUM_MAX_GROUP_OBJECTS,
    );
    define_budget(
        c"lagodb.vacuum_max_group_mb",
        c"Maximum input MiB in one provider maintenance group",
        &VACUUM_MAX_GROUP_MB,
    );
}

fn define_budget(
    name: &'static CStr,
    description: &'static CStr,
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
