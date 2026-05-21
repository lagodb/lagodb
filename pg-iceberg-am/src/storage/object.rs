use iceberg_lite::io::{FileMetadata, FileRead, FileWrite, OpenedFile, Storage};
use iceberg_lite::{Error, ErrorKind, Result};
use pg_lakebase_storage::{
    ObjectLocation, StagingFile, StagingPathResolver, StorageClient, StorageError,
    StorageFile, StorageResult, StoreId,
};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::io::SeekFrom;
use std::ops::Range;
use std::sync::Arc;

use crate::storage::transactional_artifacts::{
    ensure_object_file_staged, mark_object_file_uploaded, register_object_file_staged,
};
use crate::storage::wait_event::{StorageWaitEvent, StorageWaitGuard};

// The storage wire protocol accepts a u32 read length per request, and the
// service clamps each response to its configured max_read_size. Keep the
// adapter chunk size aligned with the default service clamp so large Iceberg
// `read_range` calls do not turn into multi-GB direct-I/O allocations.
const OBJECT_READ_CHUNK_LEN: u32 = pg_lakebase_storage::DEFAULT_MAX_READ_SIZE;

fn storage_err(e: StorageError) -> Error {
    let kind = match e.kind() {
        pg_lakebase_storage::StorageErrorKind::NotFound => ErrorKind::DataInvalid,
        pg_lakebase_storage::StorageErrorKind::InvalidPath => ErrorKind::DataInvalid,
        _ => ErrorKind::IoError,
    };
    Error::new(kind, format!("{e}")).with_source(e)
}

#[derive(Clone)]
pub struct ObjectStorage {
    scheme: Arc<str>,
    store_id: Arc<StoreId>,
    bucket: Arc<str>,
    client: StorageClient,
    staging_resolver: StagingPathResolver,
}

impl fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStorage")
            .field("scheme", &self.scheme)
            .field("store_id", &self.store_id)
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl ObjectStorage {
    pub fn new(
        scheme: impl Into<String>,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        client: StorageClient,
        staging_resolver: StagingPathResolver,
    ) -> StorageResult<Self> {
        let scheme = scheme.into();
        let bucket = bucket.into();

        Ok(Self {
            scheme: Arc::from(scheme.into_boxed_str()),
            store_id: Arc::new(StoreId::new(store_id)?),
            bucket: Arc::from(bucket.into_boxed_str()),
            client,
            staging_resolver,
        })
    }
}

fn resolve_object_uri(scheme: &str, bucket: &str, uri: &str) -> Result<usize> {
    let scheme_prefix = format!("{}://", scheme);
    if uri.starts_with(&scheme_prefix) {
        let after_scheme = &uri[scheme_prefix.len()..];
        if let Some(rest) = after_scheme.strip_prefix(bucket) {
            // Ensure the bucket name matched completely (followed by '/' or end of string),
            // not just a prefix overlap (e.g., bucket "my-lake" inside "my-lakehouse").
            if rest.is_empty() || rest.starts_with('/') {
                match rest.strip_prefix('/') {
                    Some(key) if !key.is_empty() => {
                        return Ok(uri.len() - key.len());
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!(
                                "object path {:?} points at bucket root, not an object",
                                uri
                            ),
                        ));
                    }
                }
            }
        }
        let foreign = after_scheme.split('/').next().unwrap_or("");
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "namespace mismatch: URI targets bucket {:?} but storage is bound to {:?}",
                foreign, bucket
            ),
        ));
    }

    if uri.contains("://") {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "scheme mismatch: expected {:?}, got {:?}",
                format!("{}://", scheme),
                uri
            ),
        ));
    }

    if uri.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "object path must include an object key",
        ));
    }

    Ok(0)
}

impl Storage for ObjectStorage {
    fn resolve_uri(&self, uri: &str) -> Result<usize> {
        resolve_object_uri(&self.scheme, &self.bucket, uri)
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.client
            .delete(self.store_id.as_str(), self.bucket.as_ref(), path)
            .map_err(storage_err)
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        self.client
            .delete_prefix(self.store_id.as_str(), self.bucket.as_ref(), path)
            .map(|_| ())
            .map_err(storage_err)
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        match self
            .client
            .head(self.store_id.as_str(), self.bucket.as_ref(), path)
        {
            Ok(info) => Ok(Some(FileMetadata { size: info.size })),
            Err(e) if e.kind() == pg_lakebase_storage::StorageErrorKind::NotFound => {
                Ok(None)
            }
            Err(e) => Err(storage_err(e)),
        }
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let file = self
            .client
            .open(self.store_id.as_str(), self.bucket.as_ref(), path)
            .map_err(storage_err)?;
        let metadata = FileMetadata { size: file.size() };
        Ok(OpenedFile {
            metadata,
            reader: Box::new(ObjectReader::new(
                self.client.clone(),
                Arc::clone(&self.store_id),
                Arc::clone(&self.bucket),
                Arc::from(path),
                file,
            )),
        })
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let staging = StagingFile::create(
            &self.staging_resolver,
            self.store_id.as_str(),
            self.bucket.as_ref(),
            path,
        )
        .map_err(storage_err)?;

        let location =
            ObjectLocation::new(self.store_id.as_str(), self.bucket.as_ref(), path)
                .map_err(storage_err)?;
        register_object_file_staged(
            location,
            staging.path().to_path_buf(),
            self.client.clone(),
        );

        Ok(Box::new(ObjectWriter {
            staging: Some(staging),
        }))
    }

    fn finalize_write(&self, path: &str) -> Result<()> {
        let location =
            ObjectLocation::new(self.store_id.as_str(), self.bucket.as_ref(), path)
                .map_err(storage_err)?;

        // 1. Pre-check: staged entry MUST exist before we attempt upload.
        ensure_object_file_staged(&location)
            .map_err(|msg| Error::new(ErrorKind::Unexpected, msg))?;

        // 2. Upload the staging file to object storage.
        //    On failure the registry stays Staged; abort will only unlink
        //    the local staging file without touching the remote store.
        {
            let _wait = StorageWaitGuard::start(StorageWaitEvent::ObjectUpload);
            self.client
                .upload(self.store_id.as_str(), self.bucket.as_ref(), path)
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
        &self.scheme
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct ObjectReader {
    client: StorageClient,
    store_id: Arc<StoreId>,
    bucket: Arc<str>,
    key: Arc<str>,
    file: StorageFile,
}

impl ObjectReader {
    fn new(
        client: StorageClient,
        store_id: Arc<StoreId>,
        bucket: Arc<str>,
        key: Arc<str>,
        file: StorageFile,
    ) -> Self {
        Self {
            client,
            store_id,
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

impl std::io::Read for ObjectReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let _wait = StorageWaitGuard::start(StorageWaitEvent::ObjectRead);
        self.file.read_into(buf).map_err(std::io::Error::other)
    }
}

impl std::io::Seek for ObjectReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let seek_pos = match pos {
            SeekFrom::Start(offset) => pg_lakebase_storage::SeekFrom::Start(offset),
            SeekFrom::End(offset) => pg_lakebase_storage::SeekFrom::End(offset),
            SeekFrom::Current(offset) => {
                pg_lakebase_storage::SeekFrom::Current(offset)
            }
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
            .client
            .open(
                self.store_id.as_str(),
                self.bucket.as_ref(),
                self.key.as_ref(),
            )
            .map_err(std::io::Error::other)?;
        new_file.seek(pg_lakebase_storage::SeekFrom::Start(pos));
        Ok(Box::new(ObjectReader::new(
            self.client.clone(),
            Arc::clone(&self.store_id),
            Arc::clone(&self.bucket),
            Arc::clone(&self.key),
            new_file,
        )))
    }
}

pub struct ObjectWriter {
    staging: Option<StagingFile>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_uri_strips_scheme_and_bucket() {
        let uri = "s3://my-lake/metadata/v1.json";
        let offset = resolve_object_uri("s3", "my-lake", uri).unwrap();
        assert_eq!(&uri[offset..], "metadata/v1.json");
    }

    #[test]
    fn resolve_uri_strips_scheme_and_bucket_nested_path() {
        let uri = "s3://my-lake/data/dir/file.parquet";
        let offset = resolve_object_uri("s3", "my-lake", uri).unwrap();
        assert_eq!(&uri[offset..], "data/dir/file.parquet");
    }

    #[test]
    fn resolve_uri_rejects_bucket_root() {
        assert!(resolve_object_uri("s3", "my-lake", "s3://my-lake").is_err());
        assert!(resolve_object_uri("s3", "my-lake", "s3://my-lake/").is_err());
    }

    #[test]
    fn resolve_uri_rejects_namespace_mismatch() {
        let err = resolve_object_uri("s3", "my-lake", "s3://other-bucket/a.parquet")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("namespace mismatch"), "got: {msg}");
        assert!(msg.contains("other-bucket"), "got: {msg}");
    }

    #[test]
    fn resolve_uri_rejects_scheme_mismatch() {
        let err = resolve_object_uri("s3", "my-lake", "gs://my-lake/file.parquet")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("scheme mismatch"), "got: {msg}");
    }

    #[test]
    fn resolve_uri_accepts_relative_key() {
        let key = "metadata/v1.json";
        let offset = resolve_object_uri("s3", "my-lake", key).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(&key[offset..], "metadata/v1.json");
    }

    #[test]
    fn resolve_uri_rejects_empty_path() {
        assert!(resolve_object_uri("s3", "my-lake", "").is_err());
    }

    #[test]
    fn resolve_uri_works_for_gcs() {
        let uri = "gs://my-bucket/data/file.parquet";
        let offset = resolve_object_uri("gs", "my-bucket", uri).unwrap();
        assert_eq!(&uri[offset..], "data/file.parquet");
    }

    #[test]
    fn resolve_uri_works_for_azure() {
        let uri = "az://my-container/data/file.parquet";
        let offset = resolve_object_uri("az", "my-container", uri).unwrap();
        assert_eq!(&uri[offset..], "data/file.parquet");
    }

    #[test]
    fn resolve_uri_rejects_bucket_name_prefix_overlap() {
        let err = resolve_object_uri("s3", "my-lake", "s3://my-lakehouse/file")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("namespace mismatch"), "got: {msg}");
    }
}
