use iceberg_lite::io::{FileMetadata, FileRead, FileWrite, OpenedFile, Storage};
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_core::object_cleanup::ObjectTarget;
use lagodb_core::storage::service::BackendStorageService;
use lagodb_core::storage::volume::StorageVolumeId;
use lagodb_storage::{StagingFile, StagingPathResolver, StorageError, StorageFile};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::io::SeekFrom;
use std::ops::Range;
use std::sync::Arc;

use super::injection_points::StorageInjectionPoints;
use super::object_uri::resolve_object_uri;
use super::transaction_resources::{
    ensure_object_file_staged, mark_object_file_uploaded, register_object_file_staged,
};
use super::wait_event::{StorageWaitEvent, StorageWaitGuard};

// The storage wire protocol accepts a u32 read length per request, and the
// service clamps each response to its configured max_read_size. Keep the
// adapter chunk size aligned with the default service clamp so large Iceberg
// `read_range` calls do not turn into multi-GB direct-I/O allocations.
const OBJECT_READ_CHUNK_LEN: u32 = lagodb_storage::DEFAULT_MAX_READ_SIZE;

pub(crate) fn storage_err(e: StorageError) -> Error {
    let kind = match e.kind() {
        lagodb_storage::StorageErrorKind::NotFound => ErrorKind::DataInvalid,
        lagodb_storage::StorageErrorKind::InvalidPath => ErrorKind::DataInvalid,
        _ => ErrorKind::IoError,
    };
    Error::new(kind, format!("{e}")).with_source(e)
}

#[derive(Clone)]
pub struct ObjectStorage {
    effective_base_uri: Arc<str>,
    volume_id: StorageVolumeId,
    bucket: Arc<str>,
    service: BackendStorageService,
    staging_resolver: StagingPathResolver,
}

impl fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStorage")
            .field("scheme", &self.scheme())
            .field("volume_id", &self.volume_id)
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl ObjectStorage {
    pub fn new(
        effective_base_uri: impl Into<String>,
        volume_id: StorageVolumeId,
        bucket: impl Into<String>,
        service: BackendStorageService,
        staging_resolver: StagingPathResolver,
    ) -> Self {
        let effective_base_uri = effective_base_uri.into();
        let bucket = bucket.into();
        Self {
            effective_base_uri: Arc::from(effective_base_uri.into_boxed_str()),
            volume_id,
            bucket: Arc::from(bucket.into_boxed_str()),
            service,
            staging_resolver,
        }
    }

    pub(crate) fn maintenance_target_owned(
        &self,
        mut uri: String,
    ) -> Result<ObjectTarget> {
        let relative_path_pos = resolve_object_uri(&self.effective_base_uri, &uri)?;
        let path = uri.split_off(relative_path_pos);
        ObjectTarget::new(self.volume_id, self.bucket.as_ref(), path)
            .map_err(storage_err)
    }

    pub(crate) fn list_older_than(
        &self,
        table_location: &str,
        cutoff_ms: i64,
    ) -> Result<std::collections::HashSet<String>> {
        let relative = resolve_object_uri(&self.effective_base_uri, table_location)?;
        let prefix = format!("{}/", table_location[relative..].trim_end_matches('/'));
        let uses_absolute_uris = table_location.contains("://");
        let scheme = self.scheme();
        let mut paths = std::collections::HashSet::new();
        let mut listing = self
            .service
            .list_session(self.bucket.as_ref(), Some(&prefix), 0)
            .map_err(storage_err)?;
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
                            scheme, self.bucket, entry.key
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
}

impl Storage for ObjectStorage {
    fn resolve_uri(&self, uri: &str) -> Result<usize> {
        resolve_object_uri(&self.effective_base_uri, uri)
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.service
            .delete(self.bucket.as_ref(), path)
            .map_err(storage_err)
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        self.service
            .delete_prefix(self.bucket.as_ref(), path)
            .map(|_| ())
            .map_err(storage_err)
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        match self.service.head(self.bucket.as_ref(), path) {
            Ok(info) => Ok(Some(FileMetadata { size: info.size })),
            Err(e) if e.kind() == lagodb_storage::StorageErrorKind::NotFound => {
                Ok(None)
            }
            Err(e) => Err(storage_err(e)),
        }
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let file = self
            .service
            .open(self.bucket.as_ref(), path)
            .map_err(storage_err)?;
        let metadata = FileMetadata { size: file.size() };
        Ok(OpenedFile {
            metadata,
            reader: Box::new(ObjectReader::new(
                self.service.clone(),
                Arc::clone(&self.bucket),
                Arc::from(path),
                file,
            )),
        })
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let staging = self
            .service
            .create_staging_file(&self.staging_resolver, self.bucket.as_ref(), path)
            .map_err(storage_err)?;

        let location = self
            .service
            .object_location(self.bucket.as_ref(), path)
            .map_err(storage_err)?;
        register_object_file_staged(
            location,
            staging.path().to_path_buf(),
            self.service.clone(),
        );

        Ok(Box::new(ObjectWriter::new(staging)))
    }

    fn finalize_write(&self, path: &str) -> Result<()> {
        let location = self
            .service
            .object_location(self.bucket.as_ref(), path)
            .map_err(storage_err)?;

        // 1. Pre-check: staged entry MUST exist before we attempt upload.
        ensure_object_file_staged(&location)
            .map_err(|msg| Error::new(ErrorKind::Unexpected, msg))?;

        // 2. Upload the staging file to object storage.
        //    On failure the registry stays Staged; abort will only unlink
        //    the local staging file without touching the remote store.
        {
            let _wait = StorageWaitGuard::start(StorageWaitEvent::ObjectUpload);
            self.service
                .upload(self.bucket.as_ref(), path)
                .map_err(storage_err)?;
        }

        // 3. Transition Staged → Uploaded so commit cleans staging and
        //    abort knows to delete the remote object.
        mark_object_file_uploaded(&location)
            .map_err(|msg| Error::new(ErrorKind::Unexpected, msg))?;

        Ok(())
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

pub(crate) struct ObjectReader {
    service: BackendStorageService,
    bucket: Arc<str>,
    key: Arc<str>,
    file: StorageFile,
}

// SAFETY: `ObjectReader` is an adapter for `iceberg_lite::io::FileRead`, whose
// upstream contract requires `Send + Sync`. PostgreSQL constructs, uses,
// clones, and drops every reader on one backend main thread. Neither the
// extension nor its synchronous executor transfers a reader to another
// thread or invokes it concurrently. The contained `StorageFile` deliberately
// remains `!Send + !Sync`; this unsafe boundary is confined to the host adapter
// that owns the PostgreSQL execution invariant.
unsafe impl Send for ObjectReader {}
unsafe impl Sync for ObjectReader {}

impl ObjectReader {
    pub(crate) fn new(
        service: BackendStorageService,
        bucket: Arc<str>,
        key: Arc<str>,
        file: StorageFile,
    ) -> Self {
        Self {
            service,
            bucket,
            key,
            file,
        }
    }

    fn validate_read_range(&self, range: &Range<u64>) -> Result<usize> {
        if range.start > range.end {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "invalid read range: start {} > end {}",
                    range.start, range.end
                ),
            ));
        }

        let size = self.file.size();
        if range.end > size {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "read range end {} exceeds object size {} for '{}'",
                    range.end, size, &*self.key
                ),
            ));
        }

        usize::try_from(range.end - range.start).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "read range length {} exceeds addressable memory on this platform",
                    range.end - range.start
                ),
            )
        })
    }

    fn read_range_chunked(&self, range: Range<u64>) -> Result<bytes::Bytes> {
        let len = self.validate_read_range(&range)?;
        if len == 0 {
            return Ok(bytes::Bytes::new());
        }

        // `FileRead` returns one Bytes value, so the full logical range still
        // must fit in backend memory. Chunking keeps the storage protocol's
        // u32 request length as a per-read detail instead of a file/range cap.
        let mut data = Vec::new();
        data.try_reserve_exact(len).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!(
                    "failed to reserve {} bytes for object read '{}': {}",
                    len, &*self.key, err
                ),
            )
        })?;
        // Keep the buffer initialized: `read_at_into` accepts `&mut [u8]`,
        // not a MaybeUninit-backed spare capacity slice.
        data.resize(len, 0);

        let mut offset = range.start;
        let mut remaining = len;
        let mut written = 0;

        let _wait = StorageWaitGuard::start(StorageWaitEvent::ObjectRead);
        while remaining > 0 {
            let request_len =
                std::cmp::min(remaining, OBJECT_READ_CHUNK_LEN as usize);
            let bytes_read = self
                .file
                .read_at_into(offset, &mut data[written..written + request_len])
                .map_err(storage_err)?;

            if bytes_read == 0 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "object read for '{}' returned 0 bytes before range {}..{} completed",
                        &*self.key, range.start, range.end
                    ),
                ));
            }
            if bytes_read > request_len {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "object read for '{}' returned {} bytes into a {} byte buffer",
                        &*self.key, bytes_read, request_len
                    ),
                ));
            }

            offset += bytes_read as u64;
            written += bytes_read;
            remaining -= bytes_read;
        }

        Ok(bytes::Bytes::from(data))
    }
}

impl Drop for ObjectReader {
    fn drop(&mut self) {
        StorageInjectionPoints::OBJECT_READER_BEFORE_DROP.run();
    }
}

impl std::io::Read for ObjectReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let _wait = StorageWaitGuard::start(StorageWaitEvent::ObjectRead);
        self.file.read_into(buf).map_err(std::io::Error::other)
    }
}

impl std::io::Seek for ObjectReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let seek_pos = match pos {
            SeekFrom::Start(offset) => lagodb_storage::SeekFrom::Start(offset),
            SeekFrom::End(offset) => lagodb_storage::SeekFrom::End(offset),
            SeekFrom::Current(offset) => lagodb_storage::SeekFrom::Current(offset),
        };
        Ok(self.file.seek(seek_pos))
    }
}

impl FileRead for ObjectReader {
    fn read_range(&self, range: Range<u64>) -> Result<bytes::Bytes> {
        self.read_range_chunked(range)
    }

    fn read_all(&self) -> Result<bytes::Bytes> {
        self.read_range_chunked(0..self.file.size())
    }

    fn try_clone(&self) -> std::io::Result<Box<dyn FileRead>> {
        let pos = self.file.position();
        let mut new_file = self
            .service
            .open(self.bucket.as_ref(), self.key.as_ref())
            .map_err(std::io::Error::other)?;
        new_file.seek(lagodb_storage::SeekFrom::Start(pos));
        Ok(Box::new(ObjectReader::new(
            self.service.clone(),
            Arc::clone(&self.bucket),
            Arc::clone(&self.key),
            new_file,
        )))
    }
}

pub(crate) struct ObjectWriter {
    staging: Option<StagingFile>,
}

impl ObjectWriter {
    pub(crate) fn new(staging: StagingFile) -> Self {
        Self {
            staging: Some(staging),
        }
    }
}

impl std::io::Write for ObjectWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let staging = self
            .staging
            .as_mut()
            .ok_or_else(|| std::io::Error::other("writer already closed"))?;
        let _wait = StorageWaitGuard::start(StorageWaitEvent::StagingFileWrite);
        staging.write(buf).map_err(std::io::Error::other)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(staging) = &self.staging {
            let _wait = StorageWaitGuard::start(StorageWaitEvent::StagingFileSync);
            staging.sync().map_err(std::io::Error::other)?;
        }
        Ok(())
    }
}

impl FileWrite for ObjectWriter {
    fn close(&mut self) -> Result<()> {
        // Intentionally do not call `flush()` here. Object writes stage bytes in
        // a local file that `Storage::finalize_write` uploads in the same
        // statement path; a crash before upload aborts the transaction and leaves
        // only cleanup work. Callers that need durable staged bytes before upload
        // can call `flush()` explicitly, but making close fsync every object file
        // would add latency without improving the committed object-store state.
        self.staging.take();
        Ok(())
    }
}
