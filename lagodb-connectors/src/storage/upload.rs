//! Buffered format output and one-shot local or remote publication.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::PathBuf;

use lagodb_core::diag::report_warning;
use lagodb_core::storage::foreign::ObjectAccess;
use lagodb_core::transaction::cleanup::{PendingDelete, register_pending_delete};
use lagodb_storage::{StagingFile, StorageError, StorageErrorKind, StorageResult};

use super::AllocatedObject;
use super::local::LocalFile;
use super::local_output::LocalPublication;

const WRITE_BUFFER_SIZE: usize = 64 * 1024;

enum OutputFile {
    Staging(StagingFile),
    Local(File),
}

impl OutputFile {
    fn write(&mut self, data: &[u8]) -> StorageResult<()> {
        match self {
            Self::Staging(file) => file.write(data),
            Self::Local(file) => file.write_all(data).map_err(Into::into),
        }
    }

    fn finish(self) -> StorageResult<()> {
        match self {
            Self::Staging(file) => file.sync(),
            // Match COPY's checked close without adding an fsync contract.
            Self::Local(file) => LocalFile::close(file),
        }
    }
}

/// Send-safe buffered output for format encoders. The paired publication
/// capability owns backend accounting and abandoned staging-file cleanup.
pub(crate) struct OutputWriter {
    file: OutputFile,
    buffer: Vec<u8>,
    bytes_written: u64,
}

impl OutputWriter {
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
        self.file.write(&self.buffer)?;
        self.buffer.clear();
        Ok(())
    }

    pub(crate) fn finish_file(mut self) -> StorageResult<()> {
        self.flush_buffer()?;
        self.file.finish()
    }

    /// Encoded bytes accepted by this writer, including bytes still resident
    /// in its fixed-size buffer.
    pub(crate) const fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl Write for OutputWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() >= WRITE_BUFFER_SIZE {
            self.flush_buffer().map_err(io::Error::other)?;
            self.file.write(data).map_err(io::Error::other)?;
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

/// Backend-local publication capability paired with one Send-safe writer.
/// It never enters Parquet's generic writer type.
pub(crate) enum FilePublication {
    Remote(RemoteUpload),
    Local(LocalPublication),
}

pub(crate) struct RemoteUpload {
    object: ObjectAccess,
    staging_path: Option<PathBuf>,
    delete_on_abort: bool,
}

impl FilePublication {
    pub(crate) fn start(
        allocation: AllocatedObject,
    ) -> StorageResult<(OutputWriter, Self)> {
        let (file, upload) = match allocation {
            AllocatedObject::Remote {
                object,
                delete_on_abort,
            } => {
                let staging = object.create_staging()?;
                let staging_path = staging.path().to_owned();
                (
                    OutputFile::Staging(staging),
                    Self::Remote(RemoteUpload {
                        object,
                        staging_path: Some(staging_path),
                        delete_on_abort,
                    }),
                )
            }
            AllocatedObject::Local {
                path,
                delete_on_abort,
            } => {
                let (file, upload) = LocalPublication::start(path, delete_on_abort)?;
                (OutputFile::Local(file), Self::Local(upload))
            }
        };
        Ok((
            OutputWriter {
                file,
                buffer: Vec::with_capacity(WRITE_BUFFER_SIZE),
                bytes_written: 0,
            },
            upload,
        ))
    }

    pub(crate) fn finish(self) -> StorageResult<()> {
        match self {
            Self::Remote(upload) => upload.finish(),
            Self::Local(upload) => upload.finish(),
        }
    }
}

impl RemoteUpload {
    fn finish(mut self) -> StorageResult<()> {
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

impl Drop for RemoteUpload {
    fn drop(&mut self) {
        if let Err(error) = self.remove_local() {
            report_warning(format_args!(
                "failed to clean up abandoned object staging file: {error}"
            ));
        }
    }
}
