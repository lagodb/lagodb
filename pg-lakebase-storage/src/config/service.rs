//! Read clamps and cache geometry consumed by [`crate::service::StorageService`].
//!
//! Runtime-tunable cache parameters (touch granularity, cleanup policy) live in
//! [`super::runtime::StorageRuntimeConfig`] and are hot-reloaded via
//! [`super::runtime::StorageRuntime`].

use crate::object::{
    DEFAULT_CHUNK_SIZE, DEFAULT_SMALL_OBJECT_LIMIT, normalize_chunk_size,
};

pub const DEFAULT_MAX_READ_SIZE: u32 = 1024 * 1024;
pub const DEFAULT_CACHE_TOUCH_GRANULARITY: std::time::Duration =
    std::time::Duration::from_secs(60);

/// Read clamps and cache geometry consumed by [`crate::service::StorageService`].
///
/// Cache runtime parameters (touch granularity, cleanup thresholds/intervals)
/// are managed separately by [`super::runtime::StorageRuntime`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageServiceConfig {
    pub max_read_size: u32,
    pub small_object_limit: u64,
    pub chunk_size: u64,
}

impl Default for StorageServiceConfig {
    fn default() -> Self {
        Self {
            max_read_size: DEFAULT_MAX_READ_SIZE,
            small_object_limit: DEFAULT_SMALL_OBJECT_LIMIT,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl StorageServiceConfig {
    pub fn with_max_read_size(mut self, max_read_size: u32) -> Self {
        self.max_read_size = max_read_size.max(1);
        self
    }

    pub fn with_cache_limits(
        mut self,
        small_object_limit: u64,
        chunk_size: u64,
    ) -> Self {
        self.small_object_limit = small_object_limit;
        self.chunk_size = normalize_chunk_size(chunk_size);
        self
    }

    pub fn normalized(mut self) -> Self {
        self.max_read_size = self.max_read_size.max(1);
        self.chunk_size = normalize_chunk_size(self.chunk_size);
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
        }
        .normalized();

        assert_eq!(config.max_read_size, 1);
        assert_eq!(config.small_object_limit, 7);
        assert_eq!(config.chunk_size, 1);
    }
}
