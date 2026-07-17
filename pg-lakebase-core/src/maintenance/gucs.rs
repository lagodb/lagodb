//! Reads maintenance settings backed by `pg_lakebase_runtime`.

use std::mem::MaybeUninit;
use std::time::Duration;

use crate::table_maintenance::abi::RuntimeMaintenanceConfigV1;

struct RuntimeSettings;

impl RuntimeSettings {
    fn get() -> RuntimeMaintenanceConfigV1 {
        let api = crate::table_maintenance::abi::runtime_api().unwrap_or_else(|| {
            panic!("pg_lakebase runtime API is unavailable while reading maintenance settings")
        });
        let mut config = MaybeUninit::uninit();
        unsafe {
            (api.maintenance_config)(config.as_mut_ptr());
            config.assume_init()
        }
    }
}

pub(crate) fn table_maintenance_budget() -> crate::table_maintenance::TableMaintenanceBudget {
    const MIB: u64 = 1_048_576;
    let config = RuntimeSettings::get();
    let positive = |value| {
        u64::try_from(value)
            .expect("PostgreSQL enforces positive VACUUM budget GUC values")
    };
    let mib = |value| {
        positive(value)
            .checked_mul(MIB)
            .expect("VACUUM budget MiB value exceeds u64")
    };
    crate::table_maintenance::TableMaintenanceBudget {
        max_input_objects: positive(config.vacuum_max_input_objects),
        max_input_bytes: mib(config.vacuum_max_input_mb),
        max_group_objects: positive(config.vacuum_max_group_objects),
        max_group_bytes: mib(config.vacuum_max_group_mb),
    }
}

pub(crate) fn producer_batch_items() -> usize {
    usize::try_from(RuntimeSettings::get().batch_items)
        .expect("PostgreSQL enforces a positive maintenance batch size")
}

pub(crate) fn enabled() -> bool {
    RuntimeSettings::get().enabled != 0
}

pub(crate) fn actor_threads() -> usize {
    usize::try_from(RuntimeSettings::get().actor_threads)
        .expect("PostgreSQL enforces a positive maintenance actor count")
}

pub(crate) fn batch_items() -> usize {
    producer_batch_items()
}

pub(crate) fn retry_base_ms() -> u64 {
    u64::try_from(RuntimeSettings::get().retry_base_ms)
        .expect("PostgreSQL enforces a positive maintenance retry delay")
}

pub(crate) fn retry_max_ms() -> u64 {
    u64::try_from(RuntimeSettings::get().retry_max_ms)
        .expect("PostgreSQL enforces a positive maintenance retry delay")
}

pub(crate) fn retry_max_attempts() -> i32 {
    RuntimeSettings::get().retry_max_attempts
}

pub(crate) fn request_timeout() -> Duration {
    Duration::from_millis(
        u64::try_from(RuntimeSettings::get().request_timeout_ms)
            .expect("PostgreSQL enforces a positive maintenance timeout"),
    )
}

pub(crate) fn shutdown_timeout() -> Duration {
    Duration::from_millis(
        u64::try_from(RuntimeSettings::get().shutdown_timeout_ms)
            .expect("PostgreSQL enforces a positive maintenance timeout"),
    )
}
