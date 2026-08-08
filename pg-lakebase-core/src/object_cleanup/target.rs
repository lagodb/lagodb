//! Validated physical storage targets consumed by the cleanup executor.

use pg_lakebase_storage::{ObjectPath, StorageError};

use crate::storage::volume::StorageVolumeId;

const MAX_NAMESPACE_BYTES: usize = 255;
pub const MAX_OBJECT_PATH_BYTES: usize = 1_024;

/// A validated single-object deletion target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectTarget {
    volume_id: StorageVolumeId,
    path: ObjectPath,
}

impl ObjectTarget {
    pub fn new(
        volume_id: StorageVolumeId,
        namespace: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let namespace = namespace.into();
        let path = path.into();
        validate_lengths(&namespace, &path)?;
        validate_relative_path(&path, "object path")?;
        Ok(Self {
            volume_id,
            path: ObjectPath::new(namespace, path)?,
        })
    }

    pub const fn volume_id(&self) -> StorageVolumeId {
        self.volume_id
    }

    pub fn namespace(&self) -> &str {
        self.path.bucket()
    }

    pub fn path(&self) -> &str {
        self.path.key()
    }
}

/// A validated, owned object-tree target.
///
/// The trailing slash is part of the invariant: listing this prefix cannot
/// accidentally match a sibling whose name merely starts with the same bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectTreeTarget {
    volume_id: StorageVolumeId,
    namespace: String,
    prefix: String,
}

impl ObjectTreeTarget {
    pub fn new(
        volume_id: StorageVolumeId,
        namespace: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let namespace = namespace.into();
        let raw_prefix = prefix.into();
        let mut prefix = raw_prefix.trim_start_matches('/').to_owned();
        if prefix.is_empty() {
            return Err(StorageError::invalid_path(
                "maintenance tree prefix must not be the namespace root",
            ));
        }
        validate_relative_path(&prefix, "maintenance tree prefix")?;
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        validate_lengths(&namespace, &prefix)?;

        let validated = ObjectPath::new(namespace, &prefix)?;
        Ok(Self {
            volume_id,
            namespace: validated.bucket().to_owned(),
            prefix,
        })
    }

    pub const fn volume_id(&self) -> StorageVolumeId {
        self.volume_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

fn validate_relative_path(path: &str, description: &str) -> Result<(), StorageError> {
    if path.starts_with('/') {
        return Err(StorageError::invalid_path(format!(
            "{description} must be namespace-relative"
        )));
    }
    if path.split('/').any(|segment| segment == "..") {
        return Err(StorageError::invalid_path(format!(
            "{description} must not contain '..' segments"
        )));
    }
    Ok(())
}

fn validate_lengths(namespace: &str, path: &str) -> Result<(), StorageError> {
    if namespace.len() > MAX_NAMESPACE_BYTES {
        return Err(StorageError::invalid_path(
            "object namespace exceeds 255 bytes",
        ));
    }
    if path.len() > MAX_OBJECT_PATH_BYTES {
        return Err(StorageError::invalid_path("object path exceeds 1024 bytes"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn volume_id() -> StorageVolumeId {
        StorageVolumeId::new(1).expect("valid test volume ID")
    }

    #[test]
    fn tree_prefix_is_normalized_once() {
        let target = ObjectTreeTarget::new(volume_id(), "bucket", "/table/root")
            .expect("valid target");
        assert_eq!(target.prefix(), "table/root/");
    }

    #[test]
    fn tree_rejects_namespace_root_and_parent_segments() {
        assert!(ObjectTreeTarget::new(volume_id(), "bucket", "/").is_err());
        assert!(
            ObjectTreeTarget::new(volume_id(), "bucket", "table/../other").is_err()
        );
    }

    #[test]
    fn object_rejects_non_normalized_paths() {
        assert!(ObjectTarget::new(volume_id(), "bucket", "/table/file").is_err());
        assert!(ObjectTarget::new(volume_id(), "bucket", "table/../other").is_err());
    }

    #[test]
    fn object_and_tree_targets_are_distinct_types() {
        let object = ObjectTarget::new(volume_id(), "bucket", "table/file")
            .expect("valid object");
        let tree = ObjectTreeTarget::new(volume_id(), "bucket", "table")
            .expect("valid tree");
        assert_eq!(object.path(), "table/file");
        assert_eq!(tree.prefix(), "table/");
    }

    #[test]
    fn object_path_limit_is_measured_after_tree_normalization() {
        let object_path = "x".repeat(MAX_OBJECT_PATH_BYTES);
        assert!(ObjectTarget::new(volume_id(), "bucket", &object_path).is_ok());
        assert!(
            ObjectTarget::new(volume_id(), "bucket", format!("{object_path}x"))
                .is_err()
        );

        let tree_without_slash = "x".repeat(MAX_OBJECT_PATH_BYTES - 1);
        assert!(
            ObjectTreeTarget::new(volume_id(), "bucket", &tree_without_slash).is_ok()
        );
        assert!(ObjectTreeTarget::new(volume_id(), "bucket", object_path).is_err());
    }

    #[test]
    fn object_path_limit_counts_utf8_bytes() {
        let within_limit = "界".repeat(MAX_OBJECT_PATH_BYTES / 3);
        assert!(ObjectTarget::new(volume_id(), "bucket", within_limit).is_ok());

        let over_limit = "界".repeat(MAX_OBJECT_PATH_BYTES / 3 + 1);
        assert!(ObjectTarget::new(volume_id(), "bucket", over_limit).is_err());
    }
}
