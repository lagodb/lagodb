use std::path::{Path, PathBuf};

use crate::error::StorageResult;
use crate::object::path_encoding::{build_encoded_object_path, decode_segment, normal_components, validate_portable_path};
use crate::object::ObjectLocation;

/// Which kind of cache file a path represents. The distinction is carried by the file suffix
/// (`.complete` / `.part`) rather than a directory, so both variants live side-by-side under
/// [`CachePathResolver::OBJECTS_DIR`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheFileKind {
    Complete,
    Partial,
}

impl CacheFileKind {
    /// Suffix appended to every cache file of this kind.
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Complete => CachePathResolver::COMPLETE_SUFFIX,
            Self::Partial => CachePathResolver::PARTIAL_SUFFIX,
        }
    }
}

/// Maps [`crate::object::ObjectLocation`] values to deterministic filesystem paths under
/// `<root>/objects/`. Complete and partial files for the same key share the same directory and
/// stem, differing only by their suffix (`.complete` vs `.part`).
///
/// Encoding rules (segment escapes, length limits) are shared with the staging resolver via
/// [`crate::path_encoding`] so the two layouts stay consistent and remain reversible via
/// [`Self::parse_cache_path`].
#[derive(Clone, Debug)]
pub struct CachePathResolver {
    root: PathBuf,
}

impl CachePathResolver {
    pub const OBJECTS_DIR: &'static str = "objects";
    pub const CACHE_FILE_PREFIX: &'static str = "pgl-cache.";
    pub const COMPLETE_SUFFIX: &'static str = ".complete";
    pub const PARTIAL_SUFFIX: &'static str = ".part";

    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Single cache-file directory. Complete and partial files both live here, partitioned by
    /// store / bucket / key segments and distinguished by file suffix.
    pub fn objects_dir(&self) -> PathBuf {
        self.root.join(Self::OBJECTS_DIR)
    }

    pub fn complete_path(&self, key: &ObjectLocation) -> StorageResult<PathBuf> {
        self.path_for(key, CacheFileKind::Complete)
    }

    pub fn partial_path(&self, key: &ObjectLocation) -> StorageResult<PathBuf> {
        self.path_for(key, CacheFileKind::Partial)
    }

    /// Reverse mapping: recover `(ObjectLocation, CacheFileKind)` from a cache file path.
    ///
    /// Returns `None` if the path does not live under [`Self::objects_dir`], does not end in one
    /// of the known suffixes, or cannot be decoded back to a valid [`ObjectLocation`].
    pub fn parse_cache_path(&self, path: &Path) -> Option<(ObjectLocation, CacheFileKind)> {
        let relative = path.strip_prefix(self.objects_dir()).ok()?;
        let components = normal_components(relative)?;
        if components.len() < 3 {
            return None;
        }

        let file_name = components.last()?;
        let stem_after_prefix = file_name.strip_prefix(Self::CACHE_FILE_PREFIX)?;
        let (basename, kind) = if let Some(basename) = stem_after_prefix.strip_suffix(Self::COMPLETE_SUFFIX) {
            (basename, CacheFileKind::Complete)
        } else if let Some(basename) = stem_after_prefix.strip_suffix(Self::PARTIAL_SUFFIX) {
            (basename, CacheFileKind::Partial)
        } else {
            return None;
        };

        let store_id = decode_segment(components[0])?;
        let bucket = decode_segment(components[1])?;
        let mut key_parts = Vec::with_capacity(components.len() - 2);
        for component in &components[2..components.len() - 1] {
            key_parts.push(decode_segment(component)?);
        }
        key_parts.push(decode_segment(basename)?);
        let location = ObjectLocation::new(store_id, bucket, key_parts.join("/")).ok()?;
        Some((location, kind))
    }

    fn path_for(&self, key: &ObjectLocation, kind: CacheFileKind) -> StorageResult<PathBuf> {
        let readable = self.objects_dir().join(Self::relative_path_for(key, kind.suffix()));
        validate_portable_path(key, &readable)?;
        Ok(readable)
    }

    fn relative_path_for(key: &ObjectLocation, suffix: &str) -> PathBuf {
        build_encoded_object_path(key, Self::CACHE_FILE_PREFIX, suffix)
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Component, PathBuf};

    use super::*;
    use crate::error::StorageError;

    #[test]
    fn resolves_readable_key_derived_paths_and_reverses_them() {
        let resolver = CachePathResolver::new("/tmp/cache-root");
        let key = ObjectLocation::new("store-a", "bucket-b", "path/to/file.txt").unwrap();

        let complete = resolver.complete_path(&key).unwrap();
        let partial = resolver.partial_path(&key).unwrap();

        assert_eq!(
            complete,
            PathBuf::from("/tmp/cache-root/objects/store-a/bucket-b/path/to/pgl-cache.file.txt.complete")
        );
        assert_eq!(partial, PathBuf::from("/tmp/cache-root/objects/store-a/bucket-b/path/to/pgl-cache.file.txt.part"));
        assert_eq!(resolver.parse_cache_path(&complete), Some((key.clone(), CacheFileKind::Complete)));
        assert_eq!(resolver.parse_cache_path(&partial), Some((key, CacheFileKind::Partial)));
    }

    #[test]
    fn complete_and_partial_share_parent_directory() {
        let resolver = CachePathResolver::new("/tmp/cache-root");
        let key = ObjectLocation::new("store-a", "bucket-b", "path/to/file.txt").unwrap();

        let complete = resolver.complete_path(&key).unwrap();
        let partial = resolver.partial_path(&key).unwrap();

        assert_eq!(
            complete.parent(),
            partial.parent(),
            "partial and complete files must live in the same directory so promotion is a same-dir rename"
        );
    }

    #[test]
    fn encodes_dangerous_path_segments_without_directory_escape() {
        let resolver = CachePathResolver::new("/tmp/cache-root");
        let key = ObjectLocation::new("store-a", "bucket", "dir/../a?b").unwrap();

        let complete = resolver.complete_path(&key).unwrap();

        assert!(!complete.components().any(|component| matches!(component, Component::ParentDir)));
        assert!(complete.to_string_lossy().contains("%2e%2e"));
        assert!(complete.to_string_lossy().contains("pgl-cache.a%3fb.complete"));
        assert_eq!(resolver.parse_cache_path(&complete), Some((key, CacheFileKind::Complete)));
    }

    #[test]
    fn store_id_partitions_same_bucket_and_object_paths() {
        let resolver = CachePathResolver::new("/tmp/cache-root");
        let first = ObjectLocation::new("store-a", "bucket", "same/file.txt").unwrap();
        let second = ObjectLocation::new("store-b", "bucket", "same/file.txt").unwrap();

        let first_path = resolver.complete_path(&first).unwrap();
        let second_path = resolver.complete_path(&second).unwrap();

        assert_ne!(first_path, second_path);
        assert_eq!(resolver.parse_cache_path(&first_path), Some((first, CacheFileKind::Complete)));
        assert_eq!(resolver.parse_cache_path(&second_path), Some((second, CacheFileKind::Complete)));
    }

    #[test]
    fn rejects_cache_paths_that_exceed_local_path_limits() {
        let resolver = CachePathResolver::new("/tmp/cache-root");
        let key = ObjectLocation::new("store-a", "bucket", "x".repeat(300)).unwrap();

        let error = resolver.complete_path(&key).unwrap_err();

        assert!(matches!(error, StorageError::InvalidPath { .. }));
        assert!(error.wire_message().contains("maximum component length"));
    }

    #[test]
    fn parse_cache_path_rejects_unknown_suffix() {
        let resolver = CachePathResolver::new("/tmp/cache-root");
        let stray = PathBuf::from("/tmp/cache-root/objects/store/bucket/dir/pgl-cache.file.tmp");
        assert!(resolver.parse_cache_path(&stray).is_none());
    }
}
