//! Managed tablespace object storage bound to one effective base URI.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use iceberg_lite::io::{FileMetadata, FileWrite, OpenedFile, Storage};
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_core::object_cleanup::ObjectTarget;
use lagodb_core::storage::service::BackendStorageService;
use lagodb_core::storage::volume::StorageVolumeId;
use lagodb_storage::{StagingPathResolver, StorageError, StorageErrorKind};

use super::object_bucket::ObjectBucket;
use super::object_uri::resolve_object_uri;

pub(crate) fn storage_err(e: StorageError) -> Error {
    let kind = match e.kind() {
        StorageErrorKind::NotFound => ErrorKind::DataInvalid,
        StorageErrorKind::InvalidPath => ErrorKind::DataInvalid,
        _ => ErrorKind::IoError,
    };
    Error::new(kind, format!("{e}")).with_source(e)
}

#[derive(Clone)]
pub(crate) struct ObjectStorage {
    effective_base_uri: Arc<str>,
    volume_id: StorageVolumeId,
    bucket: ObjectBucket,
}

// SAFETY: `ObjectStorage` is the private adapter that satisfies
// iceberg-lite's `Storage: Send + Sync` requirement for a naturally
// thread-bound `BackendStorageService`. LagoDB opens, invokes, clones, and
// drops it only from PostgreSQL-owned serial scan/mutation lifecycles.
unsafe impl Send for ObjectStorage {}
// SAFETY: no extension executor invokes one storage instance concurrently;
// upstream shared access is confined by the same backend-thread owner.
unsafe impl Sync for ObjectStorage {}

impl fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStorage")
            .field("scheme", &self.scheme())
            .field("volume_id", &self.volume_id)
            .field("bucket", &self.bucket.name())
            .finish()
    }
}

impl ObjectStorage {
    pub(crate) fn new(
        effective_base_uri: impl Into<String>,
        volume_id: StorageVolumeId,
        bucket: &str,
        service: BackendStorageService,
        staging_resolver: StagingPathResolver,
    ) -> Self {
        let effective_base_uri = effective_base_uri.into();
        Self {
            effective_base_uri: Arc::from(effective_base_uri.into_boxed_str()),
            volume_id,
            bucket: ObjectBucket::new(bucket, service, staging_resolver),
        }
    }

    pub(crate) fn maintenance_target_owned(
        &self,
        mut uri: String,
    ) -> Result<ObjectTarget> {
        let relative_path_pos = resolve_object_uri(&self.effective_base_uri, &uri)?;
        let path = uri.split_off(relative_path_pos);
        ObjectTarget::new(self.volume_id, self.bucket.name(), path)
            .map_err(storage_err)
    }

    pub(crate) fn list_older_than(
        &self,
        table_location: &str,
        cutoff_ms: i64,
    ) -> Result<HashSet<String>> {
        let relative = resolve_object_uri(&self.effective_base_uri, table_location)?;
        let prefix = format!("{}/", table_location[relative..].trim_end_matches('/'));
        let uses_absolute_uris = table_location.contains("://");
        let scheme = self.scheme();
        let mut paths = HashSet::new();
        let mut listing = self.bucket.list(&prefix, 0)?;
        loop {
            let Some(entries) = listing.next_page().map_err(storage_err)? else {
                break;
            };
            for entry in entries {
                if entry
                    .last_modified_ms
                    .is_some_and(|modified| modified < cutoff_ms)
                {
                    if uses_absolute_uris {
                        paths.insert(format!(
                            "{}://{}/{}",
                            scheme,
                            self.bucket.name(),
                            entry.key
                        ));
                    } else {
                        paths.insert(entry.key);
                    }
                }
            }
            if listing.is_exhausted() {
                break;
            }
        }
        Ok(paths)
    }

    pub(crate) fn location_is_empty(&self, location: &str) -> Result<bool> {
        let relative = resolve_object_uri(&self.effective_base_uri, location)?;
        let prefix = format!("{}/", location[relative..].trim_end_matches('/'));
        let mut listing = self.bucket.list(&prefix, 1)?;
        loop {
            match listing.next_page().map_err(storage_err)? {
                Some(entries) if !entries.is_empty() => return Ok(false),
                Some(_) if listing.is_exhausted() => return Ok(true),
                Some(_) => {}
                None => return Ok(true),
            }
        }
    }
}

impl Storage for ObjectStorage {
    fn resolve_uri(&self, uri: &str) -> Result<usize> {
        resolve_object_uri(&self.effective_base_uri, uri)
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.bucket.delete(path)
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        self.bucket.delete_prefix(path)
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        self.bucket.status(path)
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        self.bucket.open_reader(path)
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.bucket.writer(path)
    }

    fn finalize_write(&self, path: &str) -> Result<()> {
        self.bucket.finalize_write(path)
    }

    fn delete_cleanup(&self, path: &str) -> Result<()> {
        self.bucket.delete_cleanup(path)
    }

    fn remove_dir_all_cleanup(&self, path: &str) -> Result<()> {
        self.bucket.delete_prefix_cleanup(path)
    }

    fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
        Ok(())
    }

    fn scheme(&self) -> &str {
        self.effective_base_uri
            .split_once("://")
            .expect("tablespace binding validated the effective base URI")
            .0
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
