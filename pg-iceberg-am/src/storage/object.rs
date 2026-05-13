use iceberg_lite::io::{FileMetadata, FileRead, FileWrite, OpenedFile, Storage};
use iceberg_lite::{Error, ErrorKind, Result};
use pg_lakebase_storage::{
    StagingFile, StorageClient, StorageFile, StorageResult, StoreId,
};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::io::SeekFrom;
use std::sync::Mutex;

#[derive(Clone)]
pub struct ObjectStorage {
    scheme: String,
    store_id: StoreId,
    bucket: String,
    client: StorageClient,
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
    ) -> StorageResult<Self> {
        Ok(Self {
            scheme: scheme.into(),
            store_id: StoreId::new(store_id)?,
            bucket: bucket.into(),
            client,
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
            .delete(self.store_id.as_str(), &self.bucket, path)
            .map_err(|e| Error::new(ErrorKind::IoError, e.to_string()))
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        self.client
            .delete_prefix(self.store_id.as_str(), &self.bucket, path)
            .map(|_| ())
            .map_err(|e| Error::new(ErrorKind::IoError, e.to_string()))
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        match self.client.head(self.store_id.as_str(), &self.bucket, path) {
            Ok(info) => Ok(Some(FileMetadata { size: info.size })),
            Err(e) if e.kind() == pg_lakebase_storage::StorageErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::new(ErrorKind::IoError, e.to_string())),
        }
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let file = self
            .client
            .open(self.store_id.as_str(), &self.bucket, path)
            .map_err(|e| Error::new(ErrorKind::IoError, e.to_string()))?;
        let metadata = FileMetadata { size: file.size() };
        Ok(OpenedFile {
            metadata,
            reader: Box::new(ObjectReader::new(
                self.client.clone(),
                self.store_id.clone(),
                self.bucket.clone(),
                path.to_string(),
                file,
            )),
        })
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let staging = self
            .client
            .stage(self.store_id.as_str(), &self.bucket, path)
            .map_err(|e| Error::new(ErrorKind::IoError, e.to_string()))?;
        Ok(Box::new(ObjectWriter {
            client: self.client.clone(),
            store_id: self.store_id.clone(),
            bucket: self.bucket.clone(),
            key: path.to_string(),
            staging: Some(staging),
        }))
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
    store_id: StoreId,
    bucket: String,
    key: String,
    file: Mutex<StorageFile>,
}

impl ObjectReader {
    fn new(
        client: StorageClient,
        store_id: StoreId,
        bucket: String,
        key: String,
        file: StorageFile,
    ) -> Self {
        Self {
            client,
            store_id,
            bucket,
            key,
            file: Mutex::new(file),
        }
    }
}

impl std::io::Read for ObjectReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let file = self.file.get_mut().unwrap();
        file.read_into(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }
}

impl std::io::Seek for ObjectReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let file = self.file.get_mut().unwrap();
        let seek_pos = match pos {
            SeekFrom::Start(offset) => pg_lakebase_storage::SeekFrom::Start(offset),
            SeekFrom::End(offset) => pg_lakebase_storage::SeekFrom::End(offset),
            SeekFrom::Current(offset) => {
                pg_lakebase_storage::SeekFrom::Current(offset)
            }
        };
        Ok(file.seek(seek_pos))
    }
}

impl FileRead for ObjectReader {
    fn read_range(&self, range: std::ops::Range<u64>) -> Result<bytes::Bytes> {
        let mut file = self.file.lock().unwrap();
        let offset = range.start;
        let len = (range.end - range.start) as u32;
        file.seek(pg_lakebase_storage::SeekFrom::Start(offset));
        let data = file
            .read(len)
            .map_err(|e| Error::new(ErrorKind::IoError, e.to_string()))?;
        Ok(bytes::Bytes::from(data))
    }

    fn read_all(&self) -> Result<bytes::Bytes> {
        let mut file = self.file.lock().unwrap();
        let size = file.size();
        file.seek(pg_lakebase_storage::SeekFrom::Start(0));
        let data = file
            .read(size as u32)
            .map_err(|e| Error::new(ErrorKind::IoError, e.to_string()))?;
        Ok(bytes::Bytes::from(data))
    }

    fn try_clone(&self) -> std::io::Result<Box<dyn FileRead>> {
        let pos = self.file.lock().unwrap().position();
        let mut new_file = self
            .client
            .open(self.store_id.as_str(), &self.bucket, &self.key)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        new_file.seek(pg_lakebase_storage::SeekFrom::Start(pos));
        Ok(Box::new(ObjectReader::new(
            self.client.clone(),
            self.store_id.clone(),
            self.bucket.clone(),
            self.key.clone(),
            new_file,
        )))
    }
}

pub struct ObjectWriter {
    client: StorageClient,
    store_id: StoreId,
    bucket: String,
    key: String,
    staging: Option<StagingFile>,
}

impl std::io::Write for ObjectWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let staging = self.staging.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "writer already closed")
        })?;
        staging
            .write(buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(staging) = &self.staging {
            staging
                .sync()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }
        Ok(())
    }
}

impl FileWrite for ObjectWriter {
    fn close(&mut self) -> Result<()> {
        self.staging.take();
        self.client
            .commit(self.store_id.as_str(), &self.bucket, &self.key)
            .map(|_| ())
            .map_err(|e| Error::new(ErrorKind::IoError, e.to_string()))
    }
}

// SAFETY: StagingFile writes to a local file and StorageClient is internally mutex-protected.
unsafe impl Send for ObjectWriter {}
unsafe impl Sync for ObjectWriter {}

// SAFETY: ObjectReader's StorageFile is protected by a Mutex.
unsafe impl Send for ObjectReader {}
unsafe impl Sync for ObjectReader {}

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
