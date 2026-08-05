//! Provider-facing foreign-store operations.

use std::rc::Rc;

use pg_lakebase_storage::{
    ListCursor, ListPage, ObjectInfo, SeekFrom, StorageError, StorageFile,
    StorageResult, UploadInfo,
};
use pgrx::pg_sys;

use super::cache::ForeignStoreCacheEntry;
use super::identity::ForeignStoreIdentity;

/// Provider-facing handle for one operation-stable foreign storage context.
///
/// Providers use typed object operations and never see storage connection or
/// attach details.
#[derive(Clone)]
pub struct ForeignStoreHandle {
    entry: Rc<ForeignStoreCacheEntry>,
}

impl ForeignStoreHandle {
    pub(crate) fn new(entry: Rc<ForeignStoreCacheEntry>) -> Self {
        Self { entry }
    }

    pub fn identity(&self) -> &ForeignStoreIdentity {
        &self.entry.identity
    }

    pub fn umid(&self) -> pg_sys::Oid {
        self.entry.umid
    }

    /// Opens an object for reading.
    ///
    /// A transport failure during OPEN causes one reconnect, reattach, and
    /// replay. A server-reported object error is returned unchanged.
    pub fn open(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<ForeignStoreFile> {
        let bucket = bucket.into();
        let key = key.into();
        let file = self.open_object(&bucket, &key)?;
        Ok(ForeignStoreFile::new(self.clone(), bucket, key, file))
    }

    /// Fetches object metadata.
    ///
    /// A transport failure during HEAD causes one reconnect, reattach,
    /// and replay. A server-reported object error is returned unchanged.
    pub fn head(&self, bucket: &str, key: &str) -> StorageResult<ObjectInfo> {
        self.entry.service.head(bucket, key)
    }

    /// Publishes a staged object.
    ///
    /// Upload is not replayed after a transport failure because the remote
    /// object may already have been published. The returned error is typed as
    /// ambiguous so the provider can decide how to reconcile it.
    pub fn upload(&self, bucket: &str, key: &str) -> StorageResult<UploadInfo> {
        self.entry.service.upload(bucket, key)
    }

    /// Deletes one object.
    ///
    /// DELETE is not replayed after a transport failure because the remote
    /// result may already be committed. The returned error is typed as
    /// ambiguous so the provider can reconcile the object state explicitly.
    pub fn delete(&self, bucket: &str, key: &str) -> StorageResult<()> {
        self.entry.service.delete(bucket, key)
    }

    /// Fetches one page of a listing.
    ///
    /// The page is not replayed after a transport failure: a stateful cursor
    /// may already have advanced on the server. The caller must restart or
    /// otherwise reconcile the listing.
    pub fn list_page(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        cursor: Option<ListCursor>,
        page_size: u32,
    ) -> StorageResult<ListPage> {
        self.entry
            .service
            .list_page(bucket, prefix, cursor, page_size)
    }

    fn open_object(&self, bucket: &str, key: &str) -> StorageResult<StorageFile> {
        self.entry.service.open(bucket, key)
    }
}

/// Reconnectable read handle returned by ForeignStoreHandle::open.
///
/// Reads use absolute offsets internally, so a mediated READ transport failure
/// can reopen the object and replay the same read without duplicating a
/// side-effect. Direct-I/O reads continue using their local file descriptor.
pub struct ForeignStoreFile {
    owner: ForeignStoreHandle,
    bucket: String,
    key: String,
    file: Option<StorageFile>,
    position: u64,
    size: u64,
    direct_io: bool,
    closed: bool,
}

impl ForeignStoreFile {
    fn new(
        owner: ForeignStoreHandle,
        bucket: String,
        key: String,
        file: StorageFile,
    ) -> Self {
        let size = file.size();
        let direct_io = file.is_direct_io();
        Self {
            owner,
            bucket,
            key,
            file: Some(file),
            position: 0,
            size,
            direct_io,
            closed: false,
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn is_direct_io(&self) -> bool {
        self.direct_io
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn seek(&mut self, pos: SeekFrom) -> u64 {
        let new_position = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.position.saturating_add(offset as u64)
                } else {
                    self.position.saturating_sub(offset.unsigned_abs())
                }
            }
            SeekFrom::End(offset) => {
                if offset >= 0 {
                    self.size.saturating_add(offset as u64)
                } else {
                    self.size.saturating_sub(offset.unsigned_abs())
                }
            }
        };
        self.position = new_position;
        new_position
    }

    pub fn read(&mut self, len: u32) -> StorageResult<Vec<u8>> {
        let offset = self.position;
        let data = self.read_with_recovery(|file| file.read_at(offset, len))?;
        self.position = self.position.saturating_add(data.len() as u64);
        Ok(data)
    }

    pub fn read_into(&mut self, buf: &mut [u8]) -> StorageResult<usize> {
        let offset = self.position;
        let bytes_read =
            self.read_with_recovery(|file| file.read_at_into(offset, buf))?;
        self.position = self.position.saturating_add(bytes_read as u64);
        Ok(bytes_read)
    }

    pub fn read_at(&mut self, offset: u64, len: u32) -> StorageResult<Vec<u8>> {
        self.read_with_recovery(|file| file.read_at(offset, len))
    }

    pub fn read_at_into(
        &mut self,
        offset: u64,
        buf: &mut [u8],
    ) -> StorageResult<usize> {
        self.read_with_recovery(|file| file.read_at_into(offset, buf))
    }

    pub fn close(&mut self) -> StorageResult<()> {
        if self.closed {
            return Ok(());
        }
        let Some(file) = self.file.as_mut() else {
            self.closed = true;
            return Ok(());
        };
        let result = file.close();
        let connection_failed = self
            .file
            .as_ref()
            .is_some_and(|file| !file.is_connection_usable());
        if result.is_ok() || connection_failed {
            self.closed = true;
            drop(self.file.take());
        }
        result
    }

    fn read_with_recovery<T>(
        &mut self,
        mut operation: impl FnMut(&StorageFile) -> StorageResult<T>,
    ) -> StorageResult<T> {
        // The storage contract makes physical `(backend, bucket, key)` an immutable
        // object identity: its ETag and size do not change.  That is why a
        // transport-only failure can reopen the same location and replay an
        // absolute-offset read without mixing two object versions.  If a
        // provider ever relaxes that contract, this recovery path must fail
        // until the protocol carries a verifiable object version.
        let result = operation(self.ensure_file()?);
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                let connection_failed = self
                    .file
                    .as_ref()
                    .is_some_and(|file| !file.is_connection_usable());
                if !connection_failed {
                    return Err(error);
                }

                drop(self.file.take());
                operation(self.ensure_file()?)
            }
        }
    }

    fn ensure_file(&mut self) -> StorageResult<&StorageFile> {
        if self.closed {
            return Err(StorageError::protocol("foreign read handle is closed"));
        }
        if self.file.is_none() {
            let file = self.owner.open_object(&self.bucket, &self.key)?;
            self.size = file.size();
            self.direct_io = file.is_direct_io();
            self.file = Some(file);
        }
        self.file.as_ref().ok_or_else(|| {
            StorageError::protocol("foreign read handle is unavailable")
        })
    }
}
