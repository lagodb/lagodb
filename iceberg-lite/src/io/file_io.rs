use crate::error::Result;
use crate::{Error, ErrorKind};
use bytes::Bytes;
use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use url::Url;

use super::{LocalStorage, MemoryStorage};

/// Metadata about a file in storage.
///
/// Derives `Clone` and `Copy` so callers can read fields (e.g. `size`) before moving the
/// struct into consumers like `ArrowFileReader::new()`.
#[derive(Clone, Copy)]
pub struct FileMetadata {
    pub size: u64,
}

pub struct OpenedFile {
    pub metadata: FileMetadata,
    pub reader: Box<dyn FileRead>,
}

pub trait Storage: Send + Sync + std::fmt::Debug {
    fn delete(&self, path: &str) -> Result<()>;
    fn remove_dir_all(&self, path: &str) -> Result<()>;
    fn status(&self, path: &str) -> Result<Option<FileMetadata>>;

    fn open_reader(&self, path: &str) -> Result<OpenedFile>;
    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>>;

    fn initialize(&mut self, props: HashMap<String, String>) -> Result<()>;

    fn scheme(&self) -> &str;
    fn as_any(&self) -> &dyn Any;

    /// Called after a file has been fully written and closed.
    ///
    /// Object-storage backends use this to upload the staging file to the
    /// remote store. Local backends can treat it as a no-op. Errors are
    /// propagated to the caller so that network or backend failures are
    /// visible to the SQL statement that produced the file.
    fn finalize_write(&self, _path: &str) -> Result<()> {
        Ok(())
    }

    /// Resolve a raw URI or path into a storage-relative key.
    ///
    /// Returns the byte offset into `uri` where the storage-relative key begins.
    /// Implementations should validate that the URI targets the correct storage
    /// namespace (e.g., bucket, container) and reject mismatches.
    ///
    /// The default implementation strips the `scheme://` prefix.
    fn resolve_uri(&self, uri: &str) -> Result<usize> {
        let prefix = format!("{}://", self.scheme());
        if uri.starts_with(&prefix) {
            Ok(prefix.len())
        } else {
            Ok(0)
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileIO {
    storage: Arc<dyn Storage>,
}

impl FileIO {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    pub fn memory() -> Self {
        Self::new(Arc::new(MemoryStorage::new()))
    }

    pub fn local() -> Self {
        Self::new(Arc::new(LocalStorage::default()))
    }

    pub fn from_scheme_with_props(
        scheme: &str,
        props: HashMap<String, String>,
    ) -> Result<Self> {
        let mut storage: Arc<dyn Storage> = match scheme {
            "memory" => Arc::new(MemoryStorage::new()),
            "file" | "" => Arc::new(LocalStorage::default()),
            _ => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("Unsupported storage scheme: {}", scheme),
                ));
            }
        };

        // Initialize storage with props if any
        if !props.is_empty() {
            if let Some(s) = Arc::get_mut(&mut storage) {
                s.initialize(props)?;
            }
        }

        Ok(Self::new(storage))
    }

    pub fn from_path(path: impl AsRef<str>) -> Result<Self> {
        Self::from_path_with_props(path, HashMap::new())
    }

    pub fn from_path_with_props(
        path: impl AsRef<str>,
        props: HashMap<String, String>,
    ) -> Result<Self> {
        let url = Url::parse(path.as_ref())
            .map_err(Error::from)
            .or_else(|e| {
                Url::from_file_path(path.as_ref()).map_err(|_| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Input is neither a valid url nor path",
                    )
                    .with_context("input", path.as_ref().to_string())
                    .with_source(e)
                })
            })?;

        Self::from_scheme_with_props(url.scheme(), props)
    }

    pub fn new_input(&self, path: impl AsRef<str>) -> Result<InputFile> {
        let path_str = path.as_ref().to_string();
        let relative_path_pos = self.storage.resolve_uri(&path_str)?;
        Ok(InputFile {
            op: self.storage.clone(),
            path: path_str,
            relative_path_pos,
        })
    }

    pub fn new_output(&self, path: impl AsRef<str>) -> Result<OutputFile> {
        let path_str = path.as_ref().to_string();
        let relative_path_pos = self.storage.resolve_uri(&path_str)?;
        Ok(OutputFile {
            op: self.storage.clone(),
            path: path_str,
            relative_path_pos,
        })
    }

    pub fn delete(&self, path: impl AsRef<str>) -> Result<()> {
        let path_str = path.as_ref();
        let relative_path_pos = self.storage.resolve_uri(path_str)?;
        self.storage.delete(&path_str[relative_path_pos..])
    }

    /// Check if a file exists at the given path.
    pub fn exists(&self, path: impl AsRef<str>) -> Result<bool> {
        let path_str = path.as_ref();
        let relative_path_pos = self.storage.resolve_uri(path_str)?;
        Ok(self
            .storage
            .status(&path_str[relative_path_pos..])?
            .is_some())
    }

    /// Remove a directory and all its contents recursively.
    pub fn remove_dir_all(&self, path: impl AsRef<str>) -> Result<()> {
        let path_str = path.as_ref();
        let relative_path_pos = self.storage.resolve_uri(path_str)?;
        self.storage.remove_dir_all(&path_str[relative_path_pos..])
    }

    /// Get the underlying storage implementation.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }
}

pub trait FileRead: io::Read + io::Seek + Send + Sync {
    /// Read bytes from the specified range
    fn read_range(&self, range: Range<u64>) -> Result<Bytes>;
    /// Read all bytes from the file
    fn read_all(&self) -> Result<Bytes>;
    /// Clone this reader, similar to File::try_clone().
    /// Mutations of one reader may affect all readers sharing the same underlying resource.
    fn try_clone(&self) -> io::Result<Box<dyn FileRead>>;
}

impl FileRead for Box<dyn FileRead> {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        self.as_ref().read_range(range)
    }
    fn read_all(&self) -> Result<Bytes> {
        self.as_ref().read_all()
    }
    fn try_clone(&self) -> io::Result<Box<dyn FileRead>> {
        self.as_ref().try_clone()
    }
}

/// Input file is used for reading from files.
#[derive(Debug)]
pub struct InputFile {
    op: Arc<dyn Storage>,
    path: String,
    relative_path_pos: usize,
}

impl InputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub fn exists(&self) -> crate::Result<bool> {
        Ok(self
            .op
            .status(&self.path[self.relative_path_pos..])?
            .is_some())
    }

    pub fn metadata(&self) -> crate::Result<FileMetadata> {
        let path = &self.path[self.relative_path_pos..];
        self.op.status(path)?.ok_or_else(|| {
            Error::new(
                ErrorKind::IoError,
                format!("file metadata not found for {path:?}"),
            )
        })
    }

    pub fn open_reader(&self) -> crate::Result<OpenedFile> {
        self.op.open_reader(&self.path[self.relative_path_pos..])
    }

    pub fn read(&self) -> crate::Result<Bytes> {
        self.open_reader()?.reader.read_all()
    }

    pub fn reader(&self) -> crate::Result<Box<dyn FileRead>> {
        Ok(self.open_reader()?.reader)
    }
}

/// Trait for writing to files.
///
/// This trait extends `std::io::Write` to provide compatibility with
/// standard Rust I/O operations while also supporting the `close` method
/// for proper resource cleanup.
pub trait FileWrite: std::io::Write + Send + Sync {
    /// Close the writer's local handle.
    ///
    /// Implementations must make bytes accepted by prior `write` calls visible
    /// to the backend-specific finalization path. Durable local sync semantics
    /// are backend-specific and should be exposed through `Write::flush` when
    /// the backend supports them.
    fn close(&mut self) -> Result<()>;
}

impl FileWrite for Box<dyn FileWrite> {
    fn close(&mut self) -> Result<()> {
        self.as_mut().close()
    }
}

#[derive(Debug)]
pub struct OutputFile {
    op: Arc<dyn Storage>,
    path: String,
    relative_path_pos: usize,
}

impl OutputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub fn exists(&self) -> Result<bool> {
        Ok(self
            .op
            .status(&self.path[self.relative_path_pos..])?
            .is_some())
    }

    pub fn delete(&self) -> Result<()> {
        self.op.delete(&self.path[self.relative_path_pos..])
    }

    pub fn to_input_file(self) -> InputFile {
        InputFile {
            op: self.op,
            path: self.path,
            relative_path_pos: self.relative_path_pos,
        }
    }

    pub fn write(&self, bs: &[u8]) -> crate::Result<()> {
        use std::io::Write;
        let mut writer = self.create_writer()?;
        writer.write_all(bs)?;
        writer.finish()
    }

    /// Create a writer whose [`OutputFileWriter::finish`] method completes the
    /// whole file lifecycle: local close first, then storage finalization.
    pub fn create_writer(&self) -> crate::Result<OutputFileWriter> {
        let writer = self.writer()?;
        Ok(OutputFileWriter {
            output_file: OutputFile {
                op: Arc::clone(&self.op),
                path: self.path.clone(),
                relative_path_pos: self.relative_path_pos,
            },
            writer: Some(writer),
            local_closed: false,
            close_failed: false,
            finished: false,
        })
    }

    /// Notify the storage backend that the file has been fully written and
    /// closed. Object-storage backends upload the staging file here; local
    /// backends treat this as a no-op.
    pub(crate) fn finalize_write(&self) -> crate::Result<()> {
        self.op.finalize_write(&self.path[self.relative_path_pos..])
    }

    pub(crate) fn writer(&self) -> crate::Result<Box<dyn FileWrite>> {
        self.op.writer(&self.path[self.relative_path_pos..])
    }
}

/// Writer for an [`OutputFile`] that makes finalization explicit and fallible.
///
/// Dropping this value only performs best-effort local close; it never calls
/// storage finalization because object uploads must be reported through
/// [`Self::finish`].
pub struct OutputFileWriter {
    output_file: OutputFile,
    writer: Option<Box<dyn FileWrite>>,
    local_closed: bool,
    close_failed: bool,
    finished: bool,
}

impl OutputFileWriter {
    /// Close the local writer if it is still open. This does not finalize the
    /// storage object and therefore does not upload object-storage staging files.
    pub(crate) fn close_local(&mut self) -> crate::Result<()> {
        if self.close_failed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "output file writer close previously failed",
            ));
        }

        if self.local_closed {
            return Ok(());
        }

        let mut writer = self.writer.take().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "output file writer is missing its inner writer",
            )
        })?;
        self.local_closed = true;
        match writer.close() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.close_failed = true;
                Err(error)
            }
        }
    }

    /// Finish the file and make it visible through the backing storage.
    ///
    /// This first closes the local writer, then invokes storage finalization.
    /// Object-storage upload errors are returned to the caller from this method.
    pub fn finish(&mut self) -> crate::Result<()> {
        if self.finished {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "output file writer already finished",
            ));
        }

        self.close_local()?;
        self.output_file.finalize_write()?;
        self.finished = true;
        Ok(())
    }
}

impl io::Write for OutputFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.writer.as_mut() {
            Some(writer) => writer.write(buf),
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "output file writer is already closed",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.writer.as_mut() {
            Some(writer) => writer.flush(),
            None => Ok(()),
        }
    }
}

impl Drop for OutputFileWriter {
    fn drop(&mut self) {
        if !self.local_closed {
            if let Err(error) = self.close_local() {
                log::error!(
                    "failed to close output file writer during drop: {error}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct FailingCloseStorage {
        close_count: Arc<AtomicUsize>,
        finalize_count: Arc<AtomicUsize>,
    }

    impl Storage for FailingCloseStorage {
        fn delete(&self, _path: &str) -> Result<()> {
            Ok(())
        }

        fn remove_dir_all(&self, _path: &str) -> Result<()> {
            Ok(())
        }

        fn status(&self, _path: &str) -> Result<Option<FileMetadata>> {
            Ok(None)
        }

        fn open_reader(&self, _path: &str) -> Result<OpenedFile> {
            Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "test storage does not support reads",
            ))
        }

        fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
            Ok(Box::new(FailingCloseWriter {
                close_count: Arc::clone(&self.close_count),
            }))
        }

        fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
            Ok(())
        }

        fn scheme(&self) -> &str {
            "failing-close"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn finalize_write(&self, _path: &str) -> Result<()> {
            self.finalize_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FailingCloseWriter {
        close_count: Arc<AtomicUsize>,
    }

    impl io::Write for FailingCloseWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl FileWrite for FailingCloseWriter {
        fn close(&mut self) -> Result<()> {
            self.close_count.fetch_add(1, Ordering::SeqCst);
            Err(Error::new(ErrorKind::Unexpected, "close failed"))
        }
    }

    #[test]
    fn finish_does_not_retry_or_finalize_after_close_failure() {
        let close_count = Arc::new(AtomicUsize::new(0));
        let finalize_count = Arc::new(AtomicUsize::new(0));
        let storage = Arc::new(FailingCloseStorage {
            close_count: Arc::clone(&close_count),
            finalize_count: Arc::clone(&finalize_count),
        });
        let output_file = FileIO::new(storage).new_output("data/file").unwrap();
        let mut writer = output_file.create_writer().unwrap();

        assert!(writer.finish().is_err());
        assert_eq!(close_count.load(Ordering::SeqCst), 1);
        assert_eq!(finalize_count.load(Ordering::SeqCst), 0);

        assert!(writer.finish().is_err());
        drop(writer);

        assert_eq!(close_count.load(Ordering::SeqCst), 1);
        assert_eq!(finalize_count.load(Ordering::SeqCst), 0);
    }
}
