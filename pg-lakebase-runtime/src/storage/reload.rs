//! Runtime GUC reload and storage-volume registry resynchronization state.

use std::time::{Duration, Instant};

use pg_lakebase_storage::{StorageRuntime, StorageRuntimeConfig};
use pgrx::pg_sys;

use super::catalog::VolumeConfigSource;
use super::config::StorageWorkerRuntimeConfig;
use super::logging;
use super::reconciler::{ReconcileError, ReconcileReport, StoreConfigReconciler};

pub(super) type StorageReconciler = StoreConfigReconciler<VolumeConfigSource>;
pub(super) type StorageReconcileError = ReconcileError;

pub(super) struct SupervisorReloadState {
    config: StorageWorkerRuntimeConfig,
    reconcile_interval: Option<Duration>,
    next_periodic_reconcile: Option<Instant>,
}

impl SupervisorReloadState {
    pub(super) fn new(config: StorageWorkerRuntimeConfig) -> Self {
        let reconcile_interval = config.volume_reconcile_interval;
        Self {
            config,
            reconcile_interval,
            next_periodic_reconcile: Self::next_deadline(
                reconcile_interval,
                Instant::now(),
            ),
        }
    }

    pub(super) fn shutdown_timeout(&self) -> Duration {
        self.config.shutdown_timeout
    }

    pub(super) fn periodic_reconcile_due(&self, now: Instant) -> bool {
        self.next_periodic_reconcile
            .is_some_and(|deadline| now >= deadline)
    }

    pub(super) fn schedule_next_reconcile(&mut self, now: Instant) {
        self.next_periodic_reconcile =
            Self::next_deadline(self.reconcile_interval, now);
    }

    pub(super) fn wait_timeout(&self) -> Duration {
        let base = Duration::from_millis(100);
        let Some(interval) = self.reconcile_interval else {
            return base;
        };
        let until_periodic = self
            .next_periodic_reconcile
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(interval);
        base.min(until_periodic.max(Duration::from_millis(1)))
    }

    pub(super) fn reload_from_gucs(
        &mut self,
        storage_runtime: &StorageRuntime,
        now: Instant,
    ) {
        let new_config = StorageWorkerRuntimeConfig::from_gucs();
        if new_config == self.config {
            logging::emit_pg_log(
                pg_sys::INFO as i32,
                "SIGHUP received; runtime configuration unchanged",
            );
            return;
        }

        let old_config = self.config.clone();
        let old_storage = old_config.storage.clone();
        let interval_changed = new_config.volume_reconcile_interval
            != old_config.volume_reconcile_interval;
        let applied_storage = if new_config.storage != old_config.storage {
            Self::apply_storage_config(storage_runtime, new_config.storage.clone())
        } else {
            None
        };

        self.config = new_config;
        self.config.storage = applied_storage.unwrap_or(old_storage);
        Self::log_config_change(&old_config, &self.config);

        if interval_changed {
            self.reconcile_interval = self.config.volume_reconcile_interval;
            self.next_periodic_reconcile =
                Self::next_deadline(self.reconcile_interval, now);
        }
    }

    /// Load and apply one complete machine-managed volume snapshot without a
    /// PostgreSQL transaction or database-local catalog access.
    pub(super) fn reconcile(
        reconciler: &mut StorageReconciler,
        phase: &str,
        force_default_chain: bool,
    ) -> Result<ReconcileReport, StorageReconcileError> {
        let desired = reconciler.load_desired()?;
        let report = reconciler.apply_desired(desired, force_default_chain)?;

        for failure in &report.failures {
            logging::emit_pg_log(
                pg_sys::WARNING as i32,
                &format!(
                    "storage volume store {} apply failed ({}): {}",
                    failure.store_id,
                    failure.state.as_str(),
                    failure.message,
                ),
            );
        }

        let changed = report.added + report.replaced + report.removed > 0;
        if phase == "startup" || changed || !report.failures.is_empty() {
            logging::emit_pg_log(
                pg_sys::INFO as i32,
                &format!(
                    "storage worker {phase} reconcile: added={} replaced={} removed={} unchanged={} desired={} loaded={} stale={} unavailable={}",
                    report.added,
                    report.replaced,
                    report.removed,
                    report.unchanged,
                    report.desired,
                    report.loaded,
                    report.stale,
                    report.unavailable,
                ),
            );
        }
        Ok(report)
    }

    /// Re-read PostgreSQL configuration into this bgworker process.
    ///
    /// # Safety
    /// Must run on the bgworker main thread after `sighup_received()`.
    pub(super) unsafe fn reload_postgres_config() {
        unsafe {
            (&raw mut pg_sys::ConfigReloadPending)
                .write_volatile(0 as pg_sys::sig_atomic_t);
            pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
        }
    }

    fn apply_storage_config(
        storage_runtime: &StorageRuntime,
        config: StorageRuntimeConfig,
    ) -> Option<StorageRuntimeConfig> {
        match storage_runtime.apply(config) {
            Ok(report) if report.changed => {
                let snapshot = (*storage_runtime.snapshot()).clone();
                logging::emit_pg_log(
                    pg_sys::INFO as i32,
                    &format!(
                        "storage runtime config applied (version {})",
                        report.version,
                    ),
                );
                Some(snapshot)
            }
            Ok(_) => Some((*storage_runtime.snapshot()).clone()),
            Err(error) => {
                logging::emit_pg_log(
                    pg_sys::WARNING as i32,
                    &format!(
                        "storage runtime config apply failed, keeping old values: {error}",
                    ),
                );
                None
            }
        }
    }

    fn next_deadline(interval: Option<Duration>, now: Instant) -> Option<Instant> {
        interval.map(|duration| now + duration)
    }

    fn log_config_change(
        old: &StorageWorkerRuntimeConfig,
        new: &StorageWorkerRuntimeConfig,
    ) {
        let mut parts = Vec::new();
        if old.shutdown_timeout != new.shutdown_timeout {
            parts.push(format!(
                "shutdown_timeout: {}ms -> {}ms",
                old.shutdown_timeout.as_millis(),
                new.shutdown_timeout.as_millis(),
            ));
        }
        if old.volume_reconcile_interval != new.volume_reconcile_interval {
            let format_duration = |value: &Option<Duration>| match value {
                Some(duration) => format!("{}ms", duration.as_millis()),
                None => "disabled".to_owned(),
            };
            parts.push(format!(
                "volume_reconcile_interval: {} -> {}",
                format_duration(&old.volume_reconcile_interval),
                format_duration(&new.volume_reconcile_interval),
            ));
        }
        if old.storage != new.storage {
            if old.storage.cache.touch_granularity
                != new.storage.cache.touch_granularity
            {
                parts.push(format!(
                    "cache_touch_granularity: {}ms -> {}ms",
                    old.storage.cache.touch_granularity.as_millis(),
                    new.storage.cache.touch_granularity.as_millis(),
                ));
            }
            let old_cleanup = &old.storage.cache.cleanup;
            let new_cleanup = &new.storage.cache.cleanup;
            if old_cleanup != new_cleanup {
                let format_bytes = |value: &Option<u64>| match value {
                    Some(bytes) => format!("{}MiB", bytes / (1024 * 1024)),
                    None => "disabled".to_owned(),
                };
                let format_interval = |value: &Option<Duration>| match value {
                    Some(duration) => format!("{}ms", duration.as_millis()),
                    None => "disabled".to_owned(),
                };
                parts.push(format!(
                    "cache_cleanup: max={}->{} start={}%->{}% target={}%->{}% interval={}->{} batch_items={}->{} batch_mb={}->{}",
                    format_bytes(&old_cleanup.max_cache_bytes),
                    format_bytes(&new_cleanup.max_cache_bytes),
                    old_cleanup.cleanup_start_percent,
                    new_cleanup.cleanup_start_percent,
                    old_cleanup.cleanup_target_percent,
                    new_cleanup.cleanup_target_percent,
                    format_interval(&old_cleanup.cleanup_interval),
                    format_interval(&new_cleanup.cleanup_interval),
                    old_cleanup.max_cleanup_batch_items,
                    new_cleanup.max_cleanup_batch_items,
                    old_cleanup.max_cleanup_batch_bytes / (1024 * 1024),
                    new_cleanup.max_cleanup_batch_bytes / (1024 * 1024),
                ));
            }
        }
        if !parts.is_empty() {
            logging::emit_pg_log(
                pg_sys::INFO as i32,
                &format!(
                    "SIGHUP: runtime configuration updated ({})",
                    parts.join(", ")
                ),
            );
        }
    }
}
