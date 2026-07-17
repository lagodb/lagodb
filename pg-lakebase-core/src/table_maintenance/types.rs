use std::ffi::CStr;

use pgrx::pg_sys;

use crate::handles::VacuumParamsHandle;

/// PostgreSQL command intensity and locking profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableMaintenanceMode {
    Routine,
    Full,
}

/// Provider-relevant PostgreSQL VACUUM options, parsed once at the boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TableMaintenanceOptions {
    pub verbose: bool,
    pub analyze: bool,
    pub skip_locked: bool,
    pub process_main: bool,
}

impl TableMaintenanceOptions {
    pub fn from_vacuum_params(params: &VacuumParamsHandle<'_>) -> Self {
        let options = params.as_ref().options;
        Self {
            verbose: options & pg_sys::VACOPT_VERBOSE != 0,
            analyze: options & pg_sys::VACOPT_ANALYZE != 0,
            skip_locked: options & pg_sys::VACOPT_SKIP_LOCKED != 0,
            process_main: options & pg_sys::VACOPT_PROCESS_MAIN != 0,
        }
    }
}

/// Common resource bounds. Providers define what one selected input object is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableMaintenanceBudget {
    pub max_input_objects: u64,
    pub max_input_bytes: u64,
    pub max_group_objects: u64,
    pub max_group_bytes: u64,
}

impl TableMaintenanceBudget {
    pub fn configured() -> Self {
        crate::maintenance::table_maintenance_budget()
    }

    pub fn without_soft_limit(self, mode: TableMaintenanceMode) -> Self {
        match mode {
            TableMaintenanceMode::Routine => self,
            TableMaintenanceMode::Full => Self {
                max_input_objects: u64::MAX,
                max_input_bytes: u64::MAX,
                ..self
            },
        }
    }
}

/// A command-stable Unix epoch timestamp in milliseconds.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TableMaintenanceCommandTime(i64);

impl TableMaintenanceCommandTime {
    const POSTGRES_TO_UNIX_EPOCH_MS: i64 = 10_957 * 86_400_000;

    pub fn now() -> Result<Self, super::TableMaintenanceError> {
        let pg_timestamp_us = unsafe { pg_sys::GetCurrentStatementStartTimestamp() };
        let pg_timestamp_ms = pg_timestamp_us.div_euclid(1_000);
        pg_timestamp_ms
            .checked_add(Self::POSTGRES_TO_UNIX_EPOCH_MS)
            .map(Self)
            .ok_or_else(|| {
                super::TableMaintenanceError::framework(
                    "PostgreSQL command timestamp is outside Unix millisecond range",
                )
            })
    }

    pub const fn from_unix_epoch_ms(value: i64) -> Self {
        Self(value)
    }

    pub const fn unix_epoch_ms(self) -> i64 {
        self.0
    }
}

/// One bounded provider-specific observability counter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableMaintenanceMetric {
    pub name: &'static CStr,
    pub value: u64,
}

/// Common result counters for one relation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableMaintenanceReport {
    pub groups_rewritten: u64,
    pub input_objects: u64,
    pub input_bytes: u64,
    pub output_objects: u64,
    pub output_bytes: u64,
    pub snapshots_expired: u64,
    pub manifests_rewritten: u64,
    pub objects_scheduled_for_deletion: u64,
    pub cas_retries: u64,
    provider_metrics: Vec<TableMaintenanceMetric>,
}

impl TableMaintenanceReport {
    const MAX_PROVIDER_METRICS: usize = 32;

    pub fn record_provider_metric(
        &mut self,
        metric: TableMaintenanceMetric,
    ) -> Result<(), super::TableMaintenanceError> {
        if let Some(existing) = self
            .provider_metrics
            .iter_mut()
            .find(|existing| existing.name == metric.name)
        {
            existing.value = metric.value;
            return Ok(());
        }
        if self.provider_metrics.len() == Self::MAX_PROVIDER_METRICS {
            return Err(super::TableMaintenanceError::framework(
                "table-maintenance provider report exceeded 32 metrics",
            ));
        }
        self.provider_metrics.push(metric);
        Ok(())
    }

    pub fn provider_metrics(&self) -> &[TableMaintenanceMetric] {
        &self.provider_metrics
    }
}

/// Lightweight provider inspection used by common diagnostics.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableMaintenanceStats {
    pub provider: String,
    pub format: Option<String>,
    pub history_points: u64,
    pub current_content_objects: u64,
    pub current_content_bytes: u64,
    pub retained_content_objects: u64,
    pub retained_content_bytes: u64,
    pub current_data_objects: u64,
    pub current_data_bytes: u64,
    pub retained_data_objects: u64,
    pub retained_data_bytes: u64,
}
