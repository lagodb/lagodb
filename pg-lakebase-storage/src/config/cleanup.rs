//! Capacity / orphan cleanup policy layered onto [`super::StorageServiceConfig`].
//!
//! Orphan cleanup is always enabled — it is a mandatory correctness feature, not a tunable.

use std::time::Duration;

pub const DEFAULT_CACHE_CLEANUP_START_PERCENT: u8 = 80;
pub const DEFAULT_CACHE_CLEANUP_TARGET_PERCENT: u8 = 70;
pub const DEFAULT_CACHE_CLEANUP_BATCH_ITEMS: usize = 1024;
pub const DEFAULT_CACHE_CLEANUP_BATCH_BYTES: u64 = u64::MAX;

/// Capacity cleanup thresholds and periodic janitor knobs layered onto
/// [`super::StorageServiceConfig`].
///
/// Orphan cleanup is unconditionally enabled and not exposed as a configuration knob.
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
        self.normalized()
    }

    pub fn with_max_cleanup_batch_bytes(
        mut self,
        max_cleanup_batch_bytes: u64,
    ) -> Self {
        self.max_cleanup_batch_bytes = max_cleanup_batch_bytes;
        self.normalized()
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
