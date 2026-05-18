//! Capacity / orphan cleanup policy used by [`super::runtime::CacheRuntimeConfig`].
//!
//! Orphan cleanup is always enabled — it is a mandatory correctness feature, not a tunable.
//! `cleanup_interval` drives the periodic janitor pass which **always** reclaims orphans;
//! capacity eviction also happens on that tick when `max_cache_bytes` is set.

use std::time::Duration;

use crate::cache::{
    CacheCleanupPolicy, DEFAULT_CACHE_CLEANUP_BATCH_BYTES,
    DEFAULT_CACHE_CLEANUP_BATCH_ITEMS, DEFAULT_CACHE_CLEANUP_START_PERCENT,
    DEFAULT_CACHE_CLEANUP_TARGET_PERCENT,
};

/// Capacity cleanup thresholds and periodic janitor knobs used by
/// [`super::runtime::CacheRuntimeConfig`].
///
/// Orphan reclamation is unconditionally enabled and not exposed as a tunable; what this
/// struct controls is when capacity eviction kicks in (the `cleanup_*_percent` fields and
/// `max_cache_bytes`) and how often the periodic janitor pass runs (`cleanup_interval`).
/// The interval is meaningful even without `max_cache_bytes`: the periodic tick still runs
/// orphan reclamation, just without the capacity step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheCleanupConfig {
    pub max_cache_bytes: Option<u64>,
    pub cleanup_start_percent: u8,
    pub cleanup_target_percent: u8,
    pub max_cleanup_batch_items: usize,
    pub max_cleanup_batch_bytes: u64,
    pub cleanup_interval: Option<Duration>,
}

impl Default for CacheCleanupConfig {
    fn default() -> Self {
        Self {
            max_cache_bytes: None,
            cleanup_start_percent: DEFAULT_CACHE_CLEANUP_START_PERCENT,
            cleanup_target_percent: DEFAULT_CACHE_CLEANUP_TARGET_PERCENT,
            max_cleanup_batch_items: DEFAULT_CACHE_CLEANUP_BATCH_ITEMS,
            max_cleanup_batch_bytes: DEFAULT_CACHE_CLEANUP_BATCH_BYTES,
            // Disabled by default. Periodic orphan reclamation works without
            // `max_cache_bytes`, but the GUC layer (bgworker path) sets both together for
            // ease of operation. Embedders that want the periodic janitor without a
            // capacity cap can opt in by calling `with_cleanup_interval` alone.
            cleanup_interval: None,
        }
    }
}

impl CacheCleanupConfig {
    pub fn with_max_cache_bytes(mut self, max_cache_bytes: u64) -> Self {
        self.max_cache_bytes = Some(max_cache_bytes.max(1));
        self
    }

    pub fn with_thresholds(
        mut self,
        cleanup_start_percent: u8,
        cleanup_target_percent: u8,
    ) -> Self {
        self.cleanup_start_percent = cleanup_start_percent;
        self.cleanup_target_percent = cleanup_target_percent;
        self
    }

    pub fn with_max_cleanup_batch_bytes(
        mut self,
        max_cleanup_batch_bytes: u64,
    ) -> Self {
        self.max_cleanup_batch_bytes = max_cleanup_batch_bytes;
        self
    }

    pub fn with_cleanup_interval(mut self, cleanup_interval: Duration) -> Self {
        self.cleanup_interval = Some(cleanup_interval);
        self
    }

    pub fn normalized(mut self) -> Self {
        self.max_cache_bytes = self.max_cache_bytes.map(|bytes| bytes.max(1));
        self.cleanup_start_percent = self.cleanup_start_percent.clamp(1, 100);
        self.cleanup_target_percent = self
            .cleanup_target_percent
            .clamp(0, self.cleanup_start_percent);
        self.max_cleanup_batch_items = self.max_cleanup_batch_items.max(1);
        self.max_cleanup_batch_bytes = self.max_cleanup_batch_bytes.max(1);
        if self.cleanup_interval == Some(Duration::ZERO) {
            self.cleanup_interval = None;
        }
        self
    }

    /// Derive a [`CacheCleanupPolicy`] from this config, returning `None`
    /// when no capacity cap is configured.
    ///
    /// `None` means **only** "no capacity eviction" — orphan reclamation is unaffected and
    /// will still run on the periodic tick / reload / manual paths. The scheduler treats
    /// `None` as "skip the LRU walk", not "the cleanup subsystem is disabled".
    pub fn to_policy(&self) -> Option<CacheCleanupPolicy> {
        let config = self.clone().normalized();
        let max_cache_bytes = config.max_cache_bytes?;
        let mut policy = CacheCleanupPolicy::new(max_cache_bytes);
        policy.cleanup_start_ratio = f64::from(config.cleanup_start_percent) / 100.0;
        policy.cleanup_target_ratio =
            f64::from(config.cleanup_target_percent) / 100.0;
        policy.max_cleanup_batch_items = config.max_cleanup_batch_items.max(1);
        policy.max_cleanup_batch_bytes = config.max_cleanup_batch_bytes.max(1);
        Some(policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_config_normalizes_batch_and_zero_interval() {
        let config = CacheCleanupConfig {
            max_cache_bytes: Some(100),
            cleanup_start_percent: 120,
            cleanup_target_percent: 90,
            max_cleanup_batch_items: 0,
            max_cleanup_batch_bytes: 0,
            cleanup_interval: Some(Duration::ZERO),
        }
        .normalized();

        assert_eq!(config.cleanup_start_percent, 100);
        assert_eq!(config.cleanup_target_percent, 90);
        assert_eq!(config.max_cleanup_batch_items, 1);
        assert_eq!(config.max_cleanup_batch_bytes, 1);
        assert_eq!(config.cleanup_interval, None);
    }
}
