//! Shared maintenance settings loaded from the runtime extension.

use std::time::Duration;

use crate::runtime_api::RuntimeMaintenanceConfig;

/// Runtime-backed settings shared by physical object cleanup and logical
/// table maintenance.
#[derive(Clone, Copy)]
pub(crate) struct MaintenanceSettings {
    config: RuntimeMaintenanceConfig,
}

/// Validated VACUUM resource bounds exposed to the table-maintenance layer.
#[derive(Clone, Copy)]
pub(crate) struct VacuumBudgetSettings {
    pub(crate) max_input_objects: u64,
    pub(crate) max_input_bytes: u64,
    pub(crate) max_group_objects: u64,
    pub(crate) max_group_bytes: u64,
}

impl MaintenanceSettings {
    pub(crate) fn load() -> Self {
        let config = crate::runtime_api::RuntimeClient::connect()
            .unwrap_or_else(|error| {
                panic!("cannot read maintenance settings: {error}")
            })
            .maintenance_config();
        Self { config }
    }

    pub(crate) fn enabled(self) -> bool {
        self.config.enabled != 0
    }

    pub(crate) fn actor_threads(self) -> usize {
        usize::try_from(self.config.actor_threads)
            .expect("PostgreSQL enforces a positive maintenance actor count")
    }

    pub(crate) fn batch_items(self) -> usize {
        usize::try_from(self.config.batch_items)
            .expect("PostgreSQL enforces a positive maintenance batch size")
    }

    pub(crate) fn retry_base_ms(self) -> u64 {
        u64::try_from(self.config.retry_base_ms)
            .expect("PostgreSQL enforces a positive maintenance retry delay")
    }

    pub(crate) fn retry_max_ms(self) -> u64 {
        u64::try_from(self.config.retry_max_ms)
            .expect("PostgreSQL enforces a positive maintenance retry delay")
    }

    pub(crate) fn retry_max_attempts(self) -> i32 {
        self.config.retry_max_attempts
    }

    pub(crate) fn request_timeout(self) -> Duration {
        Duration::from_millis(
            u64::try_from(self.config.request_timeout_ms)
                .expect("PostgreSQL enforces a positive maintenance timeout"),
        )
    }

    pub(crate) fn shutdown_timeout(self) -> Duration {
        Duration::from_millis(
            u64::try_from(self.config.shutdown_timeout_ms)
                .expect("PostgreSQL enforces a positive maintenance timeout"),
        )
    }

    pub(crate) fn vacuum_budget(self) -> VacuumBudgetSettings {
        const MIB: u64 = 1_048_576;
        let positive = |value| {
            u64::try_from(value)
                .expect("PostgreSQL enforces positive VACUUM budget GUC values")
        };
        let mib = |value| {
            positive(value)
                .checked_mul(MIB)
                .expect("VACUUM budget MiB value exceeds u64")
        };
        VacuumBudgetSettings {
            max_input_objects: positive(self.config.vacuum_max_input_objects),
            max_input_bytes: mib(self.config.vacuum_max_input_mb),
            max_group_objects: positive(self.config.vacuum_max_group_objects),
            max_group_bytes: mib(self.config.vacuum_max_group_mb),
        }
    }
}
