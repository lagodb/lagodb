//! Runtime GUC reload and storage-volume registry resynchronization state.

use std::time::{Duration, Instant};

use lagodb_storage::{StorageRuntime, StorageRuntimeConfig};
use pgrx::pg_sys;

use super::catalog::VolumeConfigSource;
use super::config::StorageWorkerRuntimeConfig;
use super::logging;
use super::reconciler::{ReconcileError, ReconcileReport, StoreConfigReconciler};

pub(super) type StorageReconciler = StoreConfigReconciler<VolumeConfigSource>;
pub(super) type StorageReconcileError = ReconcileError;

const VOLUME_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

pub(super) struct SupervisorReloadState {
    config: StorageWorkerRuntimeConfig,
    next_volume_sweep: Instant,
}

impl SupervisorReloadState {
    pub(super) fn new(config: StorageWorkerRuntimeConfig) -> Self {
        Self {
            config,
            next_volume_sweep: Instant::now() + VOLUME_SWEEP_INTERVAL,
        }
    }

    pub(super) fn shutdown_timeout(&self) -> Duration {
        self.config.shutdown_timeout
    }

    pub(super) fn volume_sweep_due(&self, now: Instant) -> bool {
        now >= self.next_volume_sweep
    }

    pub(super) fn schedule_volume_sweep(&mut self, now: Instant) {
        self.next_volume_sweep = now + VOLUME_SWEEP_INTERVAL;
    }

    pub(super) fn wait_timeout(&self) -> Duration {
        let base = Duration::from_millis(100);
        let until_sweep = self
            .next_volume_sweep
            .saturating_duration_since(Instant::now());
        base.min(until_sweep.max(Duration::from_millis(1)))
    }

    pub(super) fn reload_from_gucs(&mut self, storage_runtime: &StorageRuntime) {
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
        let applied_storage = if new_config.storage != old_config.storage {
            Self::apply_storage_config(storage_runtime, new_config.storage.clone())
        } else {
            None
        };

        self.config = new_config;
        self.config.storage = applied_storage.unwrap_or(old_storage);
        Self::log_config_change(&old_config, &self.config);
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
                    failure.volume_id,
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
