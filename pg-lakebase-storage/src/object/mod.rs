//! Crate-wide object-storage data model: [`ObjectPath`], [`ObjectLocation`], [`ObjectInfo`], and
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

use crate::backend::BackendDataIdentity;
use crate::error::{StorageError, StorageResult};

pub(crate) mod path_encoding;

pub const DEFAULT_CHUNK_SIZE: u64 = 32 * 1024 * 1024;
pub const DEFAULT_SMALL_OBJECT_LIMIT: u64 = 4 * 1024;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectPath {
    bucket: String,
    key: String,
}

impl ObjectPath {
    pub fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<Self> {
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
        Ok(Self { bucket, key })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

impl fmt::Display for ObjectPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.bucket, self.key)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjectLocation {
    backend_identity: BackendDataIdentity,
    path: ObjectPath,
}

impl ObjectLocation {
    pub fn new(
        backend_identity: impl Into<BackendDataIdentity>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<Self> {
        Ok(Self {
            backend_identity: backend_identity.into(),
            path: ObjectPath::new(bucket, key)?,
        })
    }

    pub fn from_path(
        backend_identity: BackendDataIdentity,
        path: ObjectPath,
    ) -> Self {
        Self {
            backend_identity,
            path,
        }
    }

    pub fn parse_path(path: &str) -> StorageResult<Self> {
        let path = path.trim_start_matches('/');
        let (identity, rest) = path.split_once('/').ok_or_else(|| {
            StorageError::invalid_path(format!(
                "expected /backend_identity/bucket/key, got {path:?}"
            ))
        })?;
        let (bucket, key) = rest.split_once('/').ok_or_else(|| {
            StorageError::invalid_path(format!(
                "expected /backend_identity/bucket/key, got {path:?}"
            ))
        })?;
        let identity = path_encoding::decode_segment(identity).ok_or_else(|| {
            StorageError::invalid_path("invalid backend identity encoding")
        })?;
        let identity = BackendDataIdentity::from_cache_key(&identity)
            .map_err(|error| StorageError::invalid_path(error.to_string()))?;
        Self::new(identity, bucket, key)
    }

    pub fn backend_identity(&self) -> &BackendDataIdentity {
        &self.backend_identity
    }

    pub fn path(&self) -> &ObjectPath {
        &self.path
    }

    pub fn into_path(self) -> ObjectPath {
        self.path
    }

    pub fn bucket(&self) -> &str {
        self.path.bucket()
    }

    pub fn key(&self) -> &str {
        self.path.key()
    }
}

impl From<ObjectLocation> for ObjectPath {
    fn from(location: ObjectLocation) -> Self {
        location.into_path()
    }
}

impl fmt::Display for ObjectLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            path_encoding::encode_segment(self.backend_identity.cache_key()),
            self.bucket(),
            self.key()
        )
    }
}

impl Hash for ObjectLocation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.backend_identity.hash(state);
        0xfe_u8.hash(state);
        self.path.bucket.hash(state);
        0xff_u8.hash(state);
        self.path.key.hash(state);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectInfo {
    pub size: u64,
    pub etag: Option<String>,
}

/// One entry returned by [`crate::backend::ObjectBackend::list`]: an object path under the
/// requested bucket plus the same `(size, etag)` facts surfaced by `head`.
///
/// The `key` is the object key relative to the bucket — i.e. it does **not** include the
/// bucket prefix. This mirrors the `object_store::Path` returned by
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
        let original =
            ObjectLocation::new("store-a", "bucket", "path/to/file").unwrap();
        let key = ObjectLocation::parse_path(&original.to_string()).unwrap();
        assert_eq!(key.backend_identity(), original.backend_identity());
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
    fn rejects_empty_bucket() {
        let error = ObjectLocation::new("store-a", "", "file").unwrap_err();

        assert_eq!(error.to_string(), "invalid path: missing bucket");
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
