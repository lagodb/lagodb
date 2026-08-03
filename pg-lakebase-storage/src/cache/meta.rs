//! Cache-domain metadata: on-disk/in-index representation of a cached object and its residency.
//!
//! These types describe bytes that already live in the cache (small-KV embedded or complete-file on
//! disk). They are consumed by `cache::index` implementations (for durable serialization) and by
//! the [`crate::cache::CacheManager`] eviction / admission paths.

use crate::error::{StorageError, StorageResult};
use crate::object::{ObjectInfo, ObjectLocation};

/// Where durable cached bytes live for an object.
///
/// Large in-flight fills are process-local state and are deliberately not represented in durable metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheState {
    SmallKv = 1,
    CompleteFile = 3,
}

impl CacheState {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(value: u8) -> StorageResult<Self> {
        match value {
            1 => Ok(Self::SmallKv),
            3 => Ok(Self::CompleteFile),
            _ => Err(StorageError::protocol(format!(
                "unknown cache state {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectIdentity {
    pub(crate) key: ObjectLocation,
    pub(crate) size: u64,
    pub(crate) etag: Option<String>,
}

impl ObjectIdentity {
    pub(crate) fn new(key: ObjectLocation, info: ObjectInfo) -> Self {
        Self {
            key,
            size: info.size,
            etag: info.etag,
        }
    }

    pub(crate) fn info(&self) -> ObjectInfo {
        ObjectInfo {
            size: self.size,
            etag: self.etag.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CachedResidency {
    Small { bytes: u64 },
    Complete,
}

impl CachedResidency {
    fn cache_state(&self) -> CacheState {
        match self {
            Self::Small { .. } => CacheState::SmallKv,
            Self::Complete => CacheState::CompleteFile,
        }
    }
}

/// Authoritative cache record for one [`ObjectLocation`]: backend identity, cache residency state,
/// and LRU touch (`last_access_ns`).
///
/// Indexes serialize this structure durably; file payloads live separately under
/// [`crate::cache::CachePathResolver`] paths or inside small-object storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedObjectMeta {
    identity: ObjectIdentity,
    residency: CachedResidency,
    pub last_access_ns: u64,
}

impl CachedObjectMeta {
    pub fn small(key: ObjectLocation, info: ObjectInfo, bytes: u64) -> Self {
        Self::from_residency(
            ObjectIdentity::new(key, info),
            CachedResidency::Small { bytes },
            0,
        )
    }

    pub fn complete(key: ObjectLocation, info: ObjectInfo) -> Self {
        Self::from_residency(
            ObjectIdentity::new(key, info),
            CachedResidency::Complete,
            0,
        )
    }

    pub(crate) fn from_residency(
        identity: ObjectIdentity,
        residency: CachedResidency,
        last_access_ns: u64,
    ) -> Self {
        Self {
            identity,
            residency,
            last_access_ns,
        }
        .normalized()
    }

    pub fn normalized(mut self) -> Self {
        self.normalize_residency();
        self
    }

    pub fn cache_state(&self) -> CacheState {
        self.residency.cache_state()
    }

    pub fn key(&self) -> &ObjectLocation {
        &self.identity.key
    }

    pub fn size(&self) -> u64 {
        self.identity.size
    }

    pub fn etag(&self) -> Option<&str> {
        self.identity.etag.as_deref()
    }

    pub fn info(&self) -> ObjectInfo {
        self.identity.info()
    }

    pub(crate) fn residency(&self) -> &CachedResidency {
        &self.residency
    }

    pub fn is_cache_resident(&self) -> bool {
        self.cached_bytes() > 0
    }

    pub fn is_persistable(&self) -> bool {
        match &self.residency {
            CachedResidency::Small { .. } | CachedResidency::Complete => true,
        }
    }

    pub fn cached_bytes(&self) -> u64 {
        match &self.residency {
            CachedResidency::Small { bytes } => *bytes,
            CachedResidency::Complete => self.identity.size,
        }
    }

    pub fn set_small(&mut self, bytes: u64) {
        self.residency = CachedResidency::Small { bytes };
    }

    fn normalize_residency(&mut self) {
        let _ = &self.residency;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_meta_reports_full_size() {
        let key = ObjectLocation::new("store-a", "bucket", "file").unwrap();
        let meta = CachedObjectMeta::complete(
            key,
            ObjectInfo {
                size: 3,
                etag: None,
            },
        );
        assert_eq!(meta.cache_state(), CacheState::CompleteFile);
        assert_eq!(meta.cached_bytes(), 3);
    }
}
