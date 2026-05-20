//! Filesystem layout for staging files.
//!
//! Staging files mirror the `objects/` layout (same segment encoding, same store / bucket / key
//! partitioning) so operator debugging is obvious, but live under a completely separate root
//! (`<cache_dir>/staging/`) that the cache never scans. That separation is how we get the
//! external lifecycle for staging without teaching the cache layer anything new.

use std::path::{Path, PathBuf};

use crate::error::StorageResult;
use crate::object::ObjectLocation;
use crate::object::path_encoding::{
    build_encoded_object_path, validate_portable_path,
};

/// Maps [`ObjectLocation`] to deterministic staging paths under `<root>/staging/`.
///
/// The resolver is intentionally tiny: there is exactly one path shape per key, so the interface
/// is a single `path_for` plus root accessors for callers that own staging cleanup.
#[derive(Clone, Debug)]
pub struct StagingPathResolver {
    root: PathBuf,
}

impl StagingPathResolver {
    pub const STAGING_DIR: &'static str = "staging";
    pub const STAGING_FILE_PREFIX: &'static str = "pgl-staging.";

    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Single staging-file directory. The database owns creation and cleanup of this tree.
    pub fn staging_dir(&self) -> PathBuf {
        self.root.join(Self::STAGING_DIR)
    }

    /// Absolute path of the staging file for `key`, with the same per-component and total length
    /// checks the cache uses. Returns an error if the derived path would exceed portable limits.
    pub fn path_for(&self, key: &ObjectLocation) -> StorageResult<PathBuf> {
        let path = self.staging_dir().join(Self::relative_path_for(key));
        validate_portable_path(key, &path)?;
        Ok(path)
    }

    fn relative_path_for(key: &ObjectLocation) -> PathBuf {
        build_encoded_object_path(key, Self::STAGING_FILE_PREFIX, "")
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::error::StorageError;

    #[test]
    fn staging_path_is_under_root_and_encoded() {
        let resolver = StagingPathResolver::new("/tmp/root");
        let key = ObjectLocation::new("store-a", "bucket", "dir/file.txt").unwrap();

        let path = resolver.path_for(&key).unwrap();

        assert_eq!(
            path,
            PathBuf::from(
                "/tmp/root/staging/store-a/bucket/dir/pgl-staging.file.txt"
            )
        );
    }

    #[test]
    fn staging_path_rejects_overlong_key_components() {
        let resolver = StagingPathResolver::new("/tmp/root");
        let key = ObjectLocation::new("store-a", "bucket", "x".repeat(300)).unwrap();

        let error = resolver.path_for(&key).unwrap_err();

        assert!(matches!(error, StorageError::InvalidPath { .. }));
        assert!(error.wire_message().contains("maximum component length"));
    }

    #[test]
    fn dangerous_key_segments_stay_inside_root() {
        let resolver = StagingPathResolver::new("/tmp/root");
        let key = ObjectLocation::new("store-a", "bucket", "dir/../escape").unwrap();

        let path = resolver.path_for(&key).unwrap();

        assert!(path.starts_with("/tmp/root/staging/"));
        assert!(
            !path.components().any(|component| matches!(
                component,
                std::path::Component::ParentDir
            ))
        );
    }

    #[test]
    fn paths_for_different_store_ids_do_not_collide() {
        let resolver = StagingPathResolver::new("/tmp/root");
        let a = ObjectLocation::new("store-a", "bucket", "file.txt").unwrap();
        let b = ObjectLocation::new("store-b", "bucket", "file.txt").unwrap();
        assert_ne!(
            resolver.path_for(&a).unwrap(),
            resolver.path_for(&b).unwrap()
        );
    }

    #[test]
    fn remove_dir_all_on_staging_dir_cleans_all_staging_files() {
        let root = tempfile::tempdir().unwrap();
        let resolver = StagingPathResolver::new(root.path());

        let key_a =
            ObjectLocation::new("store-a", "bucket", "data/file1.parquet").unwrap();
        let key_b = ObjectLocation::new("store-a", "bucket", "meta/v1.json").unwrap();
        let path_a = resolver.path_for(&key_a).unwrap();
        let path_b = resolver.path_for(&key_b).unwrap();

        // Create staging files (simulates StagingFile::create)
        std::fs::create_dir_all(path_a.parent().unwrap()).unwrap();
        std::fs::write(&path_a, b"parquet data").unwrap();
        std::fs::create_dir_all(path_b.parent().unwrap()).unwrap();
        std::fs::write(&path_b, b"metadata json").unwrap();
        assert!(path_a.exists());
        assert!(path_b.exists());

        // Startup cleanup: remove entire staging directory
        let staging_dir = resolver.staging_dir();
        assert!(staging_dir.exists());
        std::fs::remove_dir_all(&staging_dir).unwrap();

        assert!(!staging_dir.exists());
        assert!(!path_a.exists());
        assert!(!path_b.exists());
    }

    #[test]
    fn remove_dir_all_on_nonexistent_staging_dir_is_harmless() {
        let root = tempfile::tempdir().unwrap();
        let resolver = StagingPathResolver::new(root.path());
        let staging_dir = resolver.staging_dir();

        assert!(!staging_dir.exists());
        // Should not panic or error - this is what the supervisor checks before calling remove_dir_all
    }
}
