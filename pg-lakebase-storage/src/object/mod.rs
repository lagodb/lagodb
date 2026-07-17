//! Crate-wide object-storage data model: [`StoreId`], [`ObjectLocation`], [`ObjectInfo`], and
//! chunk-coordinate helpers.
//!
//! These types are consumed by `backend`, `cache`, `protocol`, and `service`, so they live at the
//! crate root rather than inside any one of those layers.
//!
//! The [`path_encoding`] submodule provides the shared segment-encoding and path-validation
//! helpers used by both [`crate::cache::path::CachePathResolver`] and
//! [`crate::staging::path::StagingPathResolver`] to turn an [`ObjectLocation`] into a
//! deterministic on-disk path.

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::error::{StorageError, StorageResult};

pub(crate) mod path_encoding;

pub const DEFAULT_CHUNK_SIZE: u64 = 32 * 1024 * 1024;
pub const DEFAULT_SMALL_OBJECT_LIMIT: u64 = 4 * 1024;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StoreId(String);

impl StoreId {
    const MAX_LEN: usize = 128;

    pub fn new(value: impl Into<String>) -> StorageResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(StorageError::invalid_path("missing store id"));
        }
        if value.len() > Self::MAX_LEN {
            return Err(StorageError::invalid_path(format!(
                "store id exceeds maximum length of {} bytes",
                Self::MAX_LEN
            )));
        }
        if !value.bytes().all(is_store_id_byte) {
            return Err(StorageError::invalid_path(
                "store id may only contain ASCII letters, digits, '.', '_' or '-'",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StoreId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for StoreId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<StoreId> for String {
    fn from(value: StoreId) -> Self {
        value.0
    }
}

impl Hash for StoreId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

fn is_store_id_byte(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-')
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectLocation {
    store_id: StoreId,
    bucket: String,
    key: String,
}

impl ObjectLocation {
    pub fn new(
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<Self> {
        let store_id = StoreId::new(store_id)?;
        let bucket = bucket.into();
        let key = key.into();
        if bucket.is_empty() {
            return Err(StorageError::invalid_path("missing bucket"));
        }
        if bucket.contains('/') {
            return Err(StorageError::invalid_path("bucket must not contain '/'"));
        }
        if key.is_empty() {
            return Err(StorageError::invalid_path("missing object key"));
        }
        Ok(Self {
            store_id,
            bucket,
            key,
        })
    }

    pub fn parse_path(path: &str) -> StorageResult<Self> {
        let path = path.trim_start_matches('/');
        let (store_id, rest) = path.split_once('/').ok_or_else(|| {
            StorageError::invalid_path(format!(
                "expected /store_id/bucket/key, got {path:?}"
            ))
        })?;
        let (bucket, key) = rest.split_once('/').ok_or_else(|| {
            StorageError::invalid_path(format!(
                "expected /store_id/bucket/key, got {path:?}"
            ))
        })?;
        Self::new(store_id, bucket, key)
    }

    pub fn store_id(&self) -> &StoreId {
        &self.store_id
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl fmt::Display for ObjectLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.store_id, self.bucket, self.key)
    }
}

impl Hash for ObjectLocation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.store_id.hash(state);
        0xfe_u8.hash(state);
        self.bucket.hash(state);
        0xff_u8.hash(state);
        self.key.hash(state);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectInfo {
    pub size: u64,
    pub etag: Option<String>,
}

/// One entry returned by [`crate::backend::ObjectBackend::list`]: an object path under the
/// requested `(store_id, bucket)` plus the same `(size, etag)` facts surfaced by `head`.
///
/// The `key` is the object key relative to the bucket — i.e. it does **not** include the
/// `store_id` or `bucket` prefix. This mirrors the `object_store::Path` returned by
/// `ObjectStore::list`, which is bucket-relative.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    /// Backend-reported modification time in Unix epoch milliseconds.
    /// `None` means the object is not eligible for age-gated orphan removal.
    pub last_modified_ms: Option<i64>,
}

#[must_use]
pub fn chunk_count(size: u64, chunk_size: u64) -> u64 {
    let chunk_size = normalize_chunk_size(chunk_size);
    if size == 0 {
        0
    } else {
        size.div_ceil(chunk_size)
    }
}

#[must_use]
pub fn chunk_index(offset: u64, chunk_size: u64) -> u64 {
    let chunk_size = normalize_chunk_size(chunk_size);
    offset / chunk_size
}

#[must_use]
pub fn chunk_range(size: u64, chunk_size: u64, chunk: u64) -> std::ops::Range<u64> {
    let chunk_size = normalize_chunk_size(chunk_size);
    let start = chunk.saturating_mul(chunk_size);
    let end = std::cmp::min(start.saturating_add(chunk_size), size);
    start..end
}

pub(crate) fn normalize_chunk_size(chunk_size: u64) -> u64 {
    chunk_size.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_object_path() {
        let key = ObjectLocation::parse_path("/store-a/bucket/path/to/file").unwrap();
        assert_eq!(key.store_id().as_str(), "store-a");
        assert_eq!(key.bucket(), "bucket");
        assert_eq!(key.key(), "path/to/file");
    }

    #[test]
    fn rejects_bucket_path_separator() {
        let error =
            ObjectLocation::new("store-a", "bucket/path", "file").unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid path: bucket must not contain '/'"
        );
    }

    #[test]
    fn rejects_invalid_store_id() {
        let error = ObjectLocation::new("store/a", "bucket", "file").unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid path: store id may only contain ASCII letters, digits, '.', '_' or '-'"
        );
    }

    #[test]
    fn computes_chunk_ranges() {
        assert_eq!(chunk_count(0, 4), 0);
        assert_eq!(chunk_count(1, 4), 1);
        assert_eq!(chunk_count(8, 4), 2);
        assert_eq!(chunk_count(9, 4), 3);
        assert_eq!(chunk_range(10, 4, 2), 8..10);
        assert_eq!(
            chunk_range(u64::MAX, 4, u64::MAX / 4),
            u64::MAX - 3..u64::MAX
        );
    }

    #[test]
    fn normalizes_zero_chunk_size_for_infallible_helpers() {
        assert_eq!(chunk_count(3, 0), 3);
        assert_eq!(chunk_index(2, 0), 2);
        assert_eq!(chunk_range(3, 0, 2), 2..3);
    }
}
