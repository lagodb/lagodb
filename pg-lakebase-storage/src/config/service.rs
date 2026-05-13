//! Read clamps, cache geometry, touch cadence, and cleanup policy consumed by
//! [`crate::service::StorageService`].

use std::time::Duration;

use super::cleanup::CacheCleanupConfig;
use crate::object::{normalize_chunk_size, DEFAULT_CHUNK_SIZE, DEFAULT_SMALL_OBJECT_LIMIT};

pub const DEFAULT_MAX_READ_SIZE: u32 = 1024 * 1024;
pub const DEFAULT_CACHE_TOUCH_GRANULARITY: Duration = Duration::from_secs(60);

/// Read clamps, cache geometry, touch cadence, and cleanup policy consumed by
/// [`crate::service::StorageService`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageServiceConfig {
    pub max_read_size: u32,
    pub small_object_limit: u64,
    pub chunk_size: u64,
    pub touch_granularity: Duration,
    pub cache_cleanup: CacheCleanupConfig,
}

impl Default for StorageServiceConfig {
    fn default() -> Self {
        Self {
            max_read_size: DEFAULT_MAX_READ_SIZE,
            small_object_limit: DEFAULT_SMALL_OBJECT_LIMIT,
            chunk_size: DEFAULT_CHUNK_SIZE,
            touch_granularity: DEFAULT_CACHE_TOUCH_GRANULARITY,
            cache_cleanup: CacheCleanupConfig::default(),
        }
    }
}

impl StorageServiceConfig {
    pub fn with_max_read_size(mut self, max_read_size: u32) -> Self {
        self.max_read_size = max_read_size.max(1);
        self
    }

    pub fn with_cache_limits(mut self, small_object_limit: u64, chunk_size: u64) -> Self {
        self.small_object_limit = small_object_limit;
        self.chunk_size = normalize_chunk_size(chunk_size);
        self
    }

    pub fn with_max_cache_bytes(mut self, max_cache_bytes: u64) -> Self {
        self.cache_cleanup.max_cache_bytes = Some(max_cache_bytes.max(1));
        self
    }

    pub fn with_cache_cleanup_config(mut self, cache_cleanup: CacheCleanupConfig) -> Self {
        self.cache_cleanup = cache_cleanup.normalized();
        self
    }

    pub fn with_touch_granularity(mut self, touch_granularity: Duration) -> Self {
        self.touch_granularity = touch_granularity;
        self
    }

    pub fn normalized(mut self) -> Self {
        self.max_read_size = self.max_read_size.max(1);
        self.chunk_size = normalize_chunk_size(self.chunk_size);
        self.cache_cleanup = self.cache_cleanup.normalized();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_config_normalizes_public_fields() {
        let config = StorageServiceConfig {
            max_read_size: 0,
            small_object_limit: 7,
            chunk_size: 0,
            touch_granularity: Duration::ZERO,
            cache_cleanup: CacheCleanupConfig {
                max_cache_bytes: Some(0),
                cleanup_start_percent: 0,
                cleanup_target_percent: 100,
                max_cleanup_batch_items: 0,
                max_cleanup_batch_bytes: 0,
                cleanup_interval: Some(Duration::ZERO),
            },
        }
        .normalized();

        assert_eq!(config.max_read_size, 1);
        assert_eq!(config.small_object_limit, 7);
        assert_eq!(config.chunk_size, 1);
        assert_eq!(config.cache_cleanup.cleanup_start_percent, 1);
        assert_eq!(config.cache_cleanup.cleanup_target_percent, 1);
        assert_eq!(config.cache_cleanup.max_cache_bytes, Some(1));
        assert_eq!(config.cache_cleanup.max_cleanup_batch_items, 1);
        assert_eq!(config.cache_cleanup.max_cleanup_batch_bytes, 1);
        assert_eq!(config.cache_cleanup.cleanup_interval, None);
    }
}
