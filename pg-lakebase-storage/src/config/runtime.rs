//! Runtime configuration that can be hot-reloaded without restarting the
//! storage server.
//!
//! [`StorageRuntime`] holds the current [`StorageRuntimeConfig`] behind an
//! [`ArcSwap`] for lock-free reads on the request hot path. Subscribers
//! (e.g. the periodic cleanup task) can watch for version bumps via a
//! [`tokio::sync::watch`] channel.
//!
//! The storage crate itself does **not** know about PostgreSQL GUCs or SIGHUP.
//! The embedding layer (`pg-lakebase-core`) is responsible for driving
//! [`StorageRuntime::apply`] after a configuration reload.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::watch;

use super::cleanup::CacheCleanupConfig;
use crate::error::StorageResult;

/// Runtime-tunable cache parameters.
///
/// Only fields that are safe to change while the server is running belong
/// here. Layout parameters like `small_object_limit` and `chunk_size` must
/// remain startup-only because changing them would invalidate existing cached
/// data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheRuntimeConfig {
    pub touch_granularity: Duration,
    pub cleanup: CacheCleanupConfig,
}

impl Default for CacheRuntimeConfig {
    fn default() -> Self {
        Self {
            touch_granularity: super::service::DEFAULT_CACHE_TOUCH_GRANULARITY,
            cleanup: CacheCleanupConfig::default(),
        }
    }
}

/// Top-level runtime configuration snapshot.
///
/// Currently contains only cache tunables. Future phases may add connection
/// or request-level runtime parameters.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageRuntimeConfig {
    pub cache: CacheRuntimeConfig,
}

impl StorageRuntimeConfig {
    /// Normalize values to safe minimums (delegates to sub-config normalization).
    pub fn normalized(mut self) -> Self {
        self.cache.cleanup = self.cache.cleanup.normalized();
        self
    }

    /// Validate internal consistency.
    pub fn validate(&self) -> StorageResult<()> {
        let c = &self.cache.cleanup;
        if c.cleanup_start_percent < c.cleanup_target_percent {
            return Err(crate::error::StorageError::configuration(format!(
                "cleanup_start_percent ({}) must be >= cleanup_target_percent ({})",
                c.cleanup_start_percent, c.cleanup_target_percent,
            )));
        }
        Ok(())
    }
}

/// Report returned by [`StorageRuntime::apply`].
#[derive(Clone, Debug)]
pub struct RuntimeApplyReport {
    /// Whether the configuration actually changed.
    pub changed: bool,
    /// Monotonically increasing version after the apply.
    pub version: u64,
}

impl RuntimeApplyReport {
    fn noop(version: u64) -> Self {
        Self {
            changed: false,
            version,
        }
    }
}

/// Shared handle to the current runtime configuration.
///
/// Cloning a `StorageRuntime` gives another handle to the **same** underlying
/// config store — like an `Arc`. Multiple components (service, cleanup task,
/// embedder) share one instance.
#[derive(Clone)]
pub struct StorageRuntime {
    inner: Arc<StorageRuntimeInner>,
}

struct StorageRuntimeInner {
    current: ArcSwap<StorageRuntimeConfig>,
    version: AtomicU64,
    version_tx: watch::Sender<u64>,
}

impl StorageRuntime {
    /// Create a new runtime store seeded with the given config.
    ///
    /// Validates the raw config first (to reject obviously wrong combinations),
    /// then normalizes edge-case values before storing.
    pub fn new(config: StorageRuntimeConfig) -> StorageResult<Self> {
        config.validate()?;
        let config = config.normalized();

        let (version_tx, _) = watch::channel(0u64);
        Ok(Self {
            inner: Arc::new(StorageRuntimeInner {
                current: ArcSwap::from_pointee(config),
                version: AtomicU64::new(0),
                version_tx,
            }),
        })
    }

    /// Load the current configuration snapshot (lock-free).
    pub fn snapshot(&self) -> Arc<StorageRuntimeConfig> {
        self.inner.current.load_full()
    }

    /// Subscribe to version changes. The receiver yields the new version
    /// number each time [`apply`](Self::apply) commits a change.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.version_tx.subscribe()
    }

    /// Current version number (starts at 0, incremented on each successful
    /// apply that produces a real change).
    pub fn version(&self) -> u64 {
        self.inner.version.load(Ordering::Acquire)
    }

    /// Validate and atomically replace the runtime configuration.
    ///
    /// Validates the raw config first, then normalizes before comparing/storing.
    /// Returns a report indicating whether anything actually changed. If the
    /// new config is identical to the current one, no version bump or
    /// notification is emitted.
    pub fn apply(
        &self,
        config: StorageRuntimeConfig,
    ) -> StorageResult<RuntimeApplyReport> {
        config.validate()?;
        let new = config.normalized();

        let old = self.snapshot();
        if *old == new {
            return Ok(RuntimeApplyReport::noop(self.version()));
        }

        self.inner.current.store(Arc::new(new));
        let version = self.inner.version.fetch_add(1, Ordering::AcqRel) + 1;
        let _ = self.inner.version_tx.send(version);

        Ok(RuntimeApplyReport {
            changed: true,
            version,
        })
    }

    /// Narrow view of the runtime intended for the cache subsystem.
    ///
    /// `CacheManager` uses this handle to read its slice of the live config and to subscribe
    /// to change notifications, without taking a dependency on unrelated runtime fields that
    /// future revisions of [`StorageRuntimeConfig`] may add (connection / request-level
    /// tunables, etc).
    pub(crate) fn cache_handle(&self) -> CacheRuntimeHandle {
        CacheRuntimeHandle {
            inner: self.clone(),
        }
    }
}

/// Narrow read/subscribe handle scoped to [`CacheRuntimeConfig`].
///
/// Returned by [`StorageRuntime::cache_handle`]. Equivalent in cost to cloning a
/// [`StorageRuntime`] (both are `Arc`-handles into the same underlying store), but the API
/// surface is restricted to the cache slice — callers cannot accidentally read unrelated
/// runtime fields, which keeps the cache subsystem decoupled from the rest of
/// [`StorageRuntimeConfig`].
///
/// This type is `pub(crate)` because it is an internal contract between
/// [`crate::cache::CacheManager`] and the runtime store. External embedders interact with
/// the runtime through [`StorageRuntime`] and pass it to [`crate::cache::CacheManager::new`];
/// they do not construct or hold this handle directly.
#[derive(Clone)]
pub(crate) struct CacheRuntimeHandle {
    inner: StorageRuntime,
}

impl CacheRuntimeHandle {
    /// Read just the touch granularity. Used on the OPEN hot path; `ArcSwap::load_full` is
    /// one Arc bump and the [`Duration`] is `Copy`, so this allocates nothing.
    pub(crate) fn touch_granularity(&self) -> Duration {
        self.inner.snapshot().cache.touch_granularity
    }

    /// Atomic snapshot of the cleanup-relevant slice of the cache config.
    ///
    /// The scheduler's actor loop wants `(interval, cleanup_policy)` from the *same* config
    /// version — reading them through two separate `inner.snapshot()` calls would race
    /// against an `apply()` between them. This method projects both fields off a single
    /// `ArcSwap::load_full` and returns them by value (small, [`Copy`]).
    pub(crate) fn cleanup_snapshot(&self) -> CacheCleanupSnapshot {
        let snapshot = self.inner.snapshot();
        CacheCleanupSnapshot {
            interval: snapshot.cache.cleanup.cleanup_interval,
            policy: snapshot.cache.cleanup.to_policy(),
        }
    }

    /// Receiver that yields the new version number on each [`StorageRuntime::apply`] that
    /// commits a real change. Mirrors [`StorageRuntime::subscribe`].
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.inner.subscribe()
    }
}

/// Cleanup-relevant slice of the cache runtime, projected off a single ArcSwap snapshot so
/// the scheduler reads `interval` and `policy` from the same config version.
///
/// `pub(crate)` for the same reason as [`CacheRuntimeHandle`]: an internal data carrier
/// between the runtime store and the cleanup scheduler.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CacheCleanupSnapshot {
    pub(crate) interval: Option<Duration>,
    pub(crate) policy: Option<crate::cache::CacheCleanupPolicy>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_apply_does_not_bump_version() {
        let rt = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let report = rt.apply(StorageRuntimeConfig::default()).unwrap();
        assert!(!report.changed);
        assert_eq!(report.version, 0);
        assert_eq!(rt.version(), 0);
    }

    #[test]
    fn changed_apply_bumps_version_and_notifies() {
        let rt = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let mut rx = rt.subscribe();

        let mut new_config = StorageRuntimeConfig::default();
        new_config.cache.touch_granularity = Duration::from_secs(120);

        let report = rt.apply(new_config.clone()).unwrap();
        assert!(report.changed);
        assert_eq!(report.version, 1);
        assert_eq!(rt.version(), 1);

        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), 1);

        let snapshot = rt.snapshot();
        assert_eq!(snapshot.cache.touch_granularity, Duration::from_secs(120));
    }

    #[test]
    fn sequential_applies_increment_version() {
        let rt = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();

        let mut c1 = StorageRuntimeConfig::default();
        c1.cache.touch_granularity = Duration::from_secs(30);
        assert_eq!(rt.apply(c1).unwrap().version, 1);

        let mut c2 = StorageRuntimeConfig::default();
        c2.cache.touch_granularity = Duration::from_secs(90);
        assert_eq!(rt.apply(c2).unwrap().version, 2);

        assert_eq!(rt.version(), 2);
    }

    #[test]
    fn validate_rejects_start_less_than_target() {
        use crate::config::CacheCleanupConfig;

        let config = StorageRuntimeConfig {
            cache: CacheRuntimeConfig {
                cleanup: CacheCleanupConfig {
                    cleanup_start_percent: 50,
                    cleanup_target_percent: 80,
                    ..CacheCleanupConfig::default()
                },
                ..CacheRuntimeConfig::default()
            },
        };
        assert!(StorageRuntime::new(config.clone()).is_err());

        let rt = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        assert!(rt.apply(config).is_err());
    }
}
