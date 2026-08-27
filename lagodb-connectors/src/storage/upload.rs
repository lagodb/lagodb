//! Shared local staging and one-shot object upload lifecycle.

use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use lagodb_storage::{StagingFile, StorageError, StorageErrorKind, StorageResult};
use pg_lakebase_core::diag::report_warning;
use pg_lakebase_core::storage::foreign::ObjectAccess;
use pg_lakebase_core::transaction::cleanup::{
    PendingDelete, register_pending_delete,
};

use super::AllocatedObject;

const WRITE_BUFFER_SIZE: usize = 64 * 1024;

/// Buffered writer for one object. Successful finalization uploads once;
/// Drop closes and removes only the local staging file.
pub(crate) struct StagedObjectWriter {
    staging: Option<StagingFile>,
    buffer: Vec<u8>,
    bytes_written: u64,
}

impl StagedObjectWriter {
    fn record_write(&mut self, bytes: usize) {
        let bytes = u64::try_from(bytes).expect(
            "PostgreSQL is supported only on platforms where usize fits in u64",
        );
        self.bytes_written += bytes;
    }

    fn flush_buffer(&mut self) -> StorageResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.staging
            .as_mut()
            .expect("staging file remains open until local finish")
            .write(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }

    pub(crate) fn finish_local(mut self) -> StorageResult<()> {
        self.flush_buffer()?;
        self.staging
            .as_ref()
            .expect("staging file remains open until local finish")
            .sync()?;
        drop(self.staging.take());
        Ok(())
    }

    /// Encoded bytes accepted by this writer, including bytes still resident
    /// in its fixed-size buffer.
    pub(crate) const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl Write for StagedObjectWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() >= WRITE_BUFFER_SIZE {
            self.flush_buffer().map_err(io::Error::other)?;
            self.staging
                .as_mut()
                .expect("staging file remains open until local finish")
                .write(data)
                .map_err(io::Error::other)?;
            self.record_write(data.len());
            return Ok(data.len());
        }

        let remaining = WRITE_BUFFER_SIZE - self.buffer.len();
        if data.len() < remaining {
            self.buffer.extend_from_slice(data);
            self.record_write(data.len());
            return Ok(data.len());
        }

        let (prefix, suffix) = data.split_at(remaining);
        self.buffer.extend_from_slice(prefix);
        self.flush_buffer().map_err(io::Error::other)?;
        self.buffer.extend_from_slice(suffix);
        self.record_write(data.len());
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer().map_err(io::Error::other)
    }
}

/// Backend-local upload capability paired with one Send-safe staging writer.
/// It never enters Parquet's generic writer type.
pub(crate) struct StagedObjectUpload {
    object: ObjectAccess,
    staging_path: Option<PathBuf>,
    delete_on_abort: bool,
}

impl StagedObjectUpload {
    pub(crate) fn start(
        allocation: AllocatedObject,
    ) -> StorageResult<(StagedObjectWriter, Self)> {
        let (object, delete_on_abort) = allocation.into_parts();
        let staging = object.create_staging()?;
        let staging_path = staging.path().to_owned();
        Ok((
            StagedObjectWriter {
                staging: Some(staging),
                buffer: Vec::with_capacity(WRITE_BUFFER_SIZE),
                bytes_written: 0,
            },
            Self {
                object,
                staging_path: Some(staging_path),
                delete_on_abort,
            },
        ))
    }

    pub(crate) fn finish(mut self) -> StorageResult<()> {
        // Object output is immutable: prefix output uses an operation-unique
        // key, and exact output must name a previously unused key. This write
        // path deliberately does not invalidate a prior cache residency and
        // must not be treated as an object-replacement protocol.
        //
        // Exceptional replacement of an externally managed key requires the
        // caller to upload first and then successfully invoke
        // `lagodb.invalidate_object_cache`. A Busy result must be retried after
        // the current reader or fill ends. That explicit recovery operation
        // still cannot provide concurrent-read consistency; a stronger
        // contract requires atomic retire/upload/publish rather than changes to
        // this immutable-output lifecycle.
        if self.delete_on_abort {
            // Abort deletion is best-effort garbage collection for an
            // operation-unique prefix object, not transactional publication;
            // the uploaded file is intentionally visible before commit.
            // Only a remote upload attempt can create the prefix object. Keep
            // local staging/encoding failures from registering a delete for a
            // key that this statement never attempted to create. Registration
            // still precedes the request, so an ambiguous upload failure is
            // reconciled by transaction or savepoint abort cleanup.
            register_pending_delete(Box::new(UploadedObjectDelete {
                object: self.object.clone(),
            }));
            self.delete_on_abort = false;
        }
        let upload = self.object.upload();
        let cleanup = self.remove_local();

        match (upload, cleanup) {
            (Ok(_), Ok(())) => Ok(()),
            (Ok(_), Err(error)) => {
                report_warning(format_args!(
                    "object upload succeeded but local staging cleanup failed: {error}"
                ));
                Ok(())
            }
            (Err(upload_error), Ok(())) => Err(upload_error),
            (Err(upload_error), Err(cleanup_error)) => {
                report_warning(format_args!(
                    "object upload failed and local staging cleanup also failed: {cleanup_error}"
                ));
                Err(upload_error)
            }
        }
    }

    fn remove_local(&mut self) -> StorageResult<()> {
        let Some(path) = self.staging_path.take() else {
            return Ok(());
        };
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::io(
                format!("remove object staging file {}", path.display()),
                error,
            )),
        }
    }
}

struct UploadedObjectDelete {
    object: ObjectAccess,
}

impl std::fmt::Debug for UploadedObjectDelete {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("UploadedObjectDelete")
    }
}

impl PendingDelete for UploadedObjectDelete {
    fn execute(&self) {
        match self.object.delete() {
            Ok(()) => {}
            Err(error) if error.kind() == StorageErrorKind::NotFound => {}
            Err(error) => report_warning(format_args!(
                "failed to delete transaction-created object during rollback: {error}"
            )),
        }
    }
}

impl Drop for StagedObjectUpload {
    fn drop(&mut self) {
        if let Err(error) = self.remove_local() {
            report_warning(format_args!(
                "failed to clean up abandoned object staging file: {error}"
            ));
        }
    }
}
