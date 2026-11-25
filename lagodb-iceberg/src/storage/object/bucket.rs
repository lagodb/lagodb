//! One object-store bucket bound to a backend storage service.
//!
//! Managed tablespace storage and REST catalog routes reach object data
//! through the same bucket operations, readers and staged writers.

use std::sync::Arc;

use iceberg_lite::io::{FileMetadata, FileWrite, OpenedFile};
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_core::storage::service::BackendStorageService;
use lagodb_storage::{
    ListSession, ObjectLocation, StagingPathResolver, StorageErrorKind,
};

use super::reader::ObjectReader;
use super::wait_event::ObjectWaitEvent;
use super::writer::ObjectWriter;
use crate::error::{IcebergError, storage_error_to_iceberg_lite};
use crate::storage::transaction_resources::{
    ensure_object_file_staged, mark_object_file_uploaded, register_object_file_staged,
};

/// A bucket and the backend storage service that serves it.
#[derive(Clone)]
pub(crate) struct ObjectBucket {
    name: Arc<str>,
    service: BackendStorageService,
    staging_resolver: StagingPathResolver,
}

impl ObjectBucket {
    pub(crate) fn new(
        name: &str,
        service: BackendStorageService,
        staging_resolver: StagingPathResolver,
    ) -> Self {
        Self {
            name: Arc::from(name),
            service,
            staging_resolver,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn delete(&self, key: &str) -> Result<()> {
        self.service
            .delete(&self.name, key)
            .map_err(storage_error_to_iceberg_lite)
    }

    /// Delete from a transaction-end callback; see
    /// [`BackendStorageService::delete_cleanup`].
    pub(crate) fn delete_cleanup(&self, key: &str) -> Result<()> {
        self.service
            .delete_cleanup(&self.name, key)
            .map_err(storage_error_to_iceberg_lite)
    }

    pub(crate) fn delete_prefix(&self, key: &str) -> Result<()> {
        self.service
            .delete_prefix(&self.name, key)
            .map(|_| ())
            .map_err(storage_error_to_iceberg_lite)
    }

    /// Delete a prefix from a transaction-end callback; see
    /// [`BackendStorageService::delete_prefix_cleanup`].
    pub(crate) fn delete_prefix_cleanup(&self, key: &str) -> Result<()> {
        self.service
            .delete_prefix_cleanup(&self.name, key)
            .map(|_| ())
            .map_err(storage_error_to_iceberg_lite)
    }

    pub(crate) fn list(&self, prefix: &str, page_size: u32) -> Result<ListSession> {
        self.service
            .list_session(&self.name, Some(prefix), page_size)
            .map_err(storage_error_to_iceberg_lite)
    }

    pub(crate) fn status(&self, key: &str) -> Result<Option<FileMetadata>> {
        match self.service.head(&self.name, key) {
            Ok(info) => Ok(Some(FileMetadata { size: info.size })),
            Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
            Err(error) => Err(storage_error_to_iceberg_lite(error)),
        }
    }

    pub(crate) fn open_reader(&self, key: &str) -> Result<OpenedFile> {
        let file = self
            .service
            .open(&self.name, key)
            .map_err(storage_error_to_iceberg_lite)?;
        let metadata = FileMetadata { size: file.size() };
        Ok(OpenedFile {
            metadata,
            reader: Box::new(ObjectReader::new(
                self.service.clone(),
                Arc::clone(&self.name),
                Arc::from(key),
                file,
            )),
        })
    }

    pub(crate) fn writer(&self, key: &str) -> Result<Box<dyn FileWrite>> {
        let (staging, location) = self
            .service
            .stage_object(&self.staging_resolver, &self.name, key)
            .map_err(storage_error_to_iceberg_lite)?;
        register_object_file_staged(
            location,
            staging.path().to_path_buf(),
            self.service.clone(),
        );
        Ok(Box::new(ObjectWriter::new(staging)))
    }

    pub(crate) fn finalize_write(&self, key: &str) -> Result<()> {
        let location = self
            .service
            .object_location(&self.name, key)
            .map_err(storage_error_to_iceberg_lite)?;

        // 1. Pre-check: staged entry MUST exist before we attempt upload.
        ensure_object_file_staged(&location)
            .map_err(|error| Self::lifecycle_err(&location, error))?;

        // 2. Upload the staging file to object storage.
        //    On failure the registry stays Staged; abort will only unlink
        //    the local staging file without touching the remote store.
        {
            let _wait = ObjectWaitEvent::ObjectUpload.enter();
            self.service
                .upload(&self.name, key)
                .map_err(storage_error_to_iceberg_lite)?;
        }

        // 3. Transition Staged → Uploaded so commit cleans staging and
        //    abort knows to delete the remote object.
        mark_object_file_uploaded(&location)
            .map_err(|error| Self::lifecycle_err(&location, error))
    }

    fn lifecycle_err(location: &ObjectLocation, error: IcebergError) -> Error {
        Error::new(
            ErrorKind::Unexpected,
            format!("object staging lifecycle invariant failed for '{location}'"),
        )
        .with_source(error)
    }
}
