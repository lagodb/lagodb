//! Blocking Unix peer for tests / tools / foreign-language wrappers.
//!
//! The client exposes three surfaces:
//!
//! * [`StorageClient`] — connection-bound request/response primitive. Cloneable; all clones
//!   share one underlying non-multiplexed Unix connection for single-threaded blocking use.
//! * [`StorageFile`]   — open read handle returned by [`StorageClient::open`]. Seek / read / close.
//! * [`StagingFile`]   — local file handle constructed by the caller via
//!   [`StagingFile::create`](crate::client::StagingFile::create) using a
//!   [`StagingPathResolver`](crate::staging::StagingPathResolver) rooted at the storage
//!   server's `cache_dir`. The server is not in the data path for writes — the caller creates
//!   the file, appends bytes, and later issues `Upload` through [`StorageClient`]. Cleanup of
//!   the staging directory is the caller's responsibility.
//!
//! That last property is the whole point of the staging surface: a database transaction can
//! write a staging file, close any client connection it had, and hours later upload from any
//! other connection so long as it knows the identity tuple.

use std::cell::{RefCell, RefMut};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::rc::Rc;

use crate::backend::{StorageProbeResult, StoreConfig};
use crate::error::{StorageError, StorageResult};
use crate::handle::{FileHandle, OpenFlags};
use crate::object::ObjectInfo;
use crate::protocol::{ListCursor, WireRequestPayload, WireResponsePayload};

mod connection;
mod fd;
mod list;
mod staging_file;
mod storage_file;

#[cfg(test)]
mod tests;

pub use fd::{ExternalFdLease, ExternalFdPolicy};
pub use list::ListIter;
pub use staging_file::StagingFile;
pub use storage_file::{SeekFrom, StorageFile};

use connection::{ClientConnection, ReceivedFd};
use storage_file::{DirectReader, ReadPath};

fn unexpected_response(operation: &str, got: &WireResponsePayload) -> StorageError {
    StorageError::protocol(format!("unexpected {operation} response: {got:?}"))
}

/// Connection-bound request/response handle to a storage server over a Unix socket.
///
/// Cloneable: all clones share one underlying connection state. This blocking client is
/// intentionally single-threaded and non-multiplexed: each call writes one request and reads its
/// matching response before the next call may use the connection. For independent concurrent work,
/// open multiple `StorageClient` connections instead of cloning one client.
///
/// # Connection failures
///
/// Local I/O, framing, request-ID, and response-shape failures permanently
/// poison the shared connection generation and close its socket. Server-reported
/// operation errors leave the protocol stream synchronized and reusable.
///
/// # Long-running calls
///
/// Some methods can occupy the connection for a long time (notably
/// [`Self::delete_prefix`], which lists and deletes every matching object before returning).
/// While such a call is in flight, the same `StorageClient` connection cannot be used for another
/// request. If a workload mixes long-running admin calls with latency-sensitive reads, dedicate
/// separate `StorageClient` instances to each.
#[derive(Clone)]
pub struct StorageClient {
    // One client is one blocking protocol state machine. Clones share it within one thread
    // without pretending the connection is safe for concurrent use.
    inner: Rc<RefCell<ClientConnection>>,
}

// SAFETY: this is a temporary compatibility boundary for `pg-iceberg-am`, whose
// `iceberg-lite` storage traits inherit upstream `Send + Sync` bounds. The
// PostgreSQL AM integration uses `StorageClient` only from one backend thread with
// blocking, non-multiplexed calls. `StorageClient` must not be moved to a worker
// thread or shared for concurrent use; doing so would violate the `Rc<RefCell<_>>`
// invariants and PostgreSQL FD-accounting thread affinity. This remains an
// acknowledged soundness risk until the Parquet `ChunkReader` boundary is
// redesigned without per-read owner-thread checks.
unsafe impl Send for StorageClient {}
unsafe impl Sync for StorageClient {}

/// Reported outcome of a successful [`StorageClient::upload`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadInfo {
    pub size: u64,
    pub etag: Option<String>,
}

/// One element returned by [`StorageClient::list`] / [`StorageClient::list_page`].
///
/// Re-exported from [`crate::object::ListEntry`] (the same type the backend trait surfaces) to
/// keep the client and backend vocabularies aligned: a list entry has exactly one shape across
/// the codebase.
pub use crate::object::ListEntry;

/// One page of a [`StorageClient::list_page`] call.
///
/// `next_cursor.is_none()` means the listing is complete; otherwise the same cursor must be
/// passed back in to fetch the next page. The cursor is opaque — callers should not parse or
/// modify its contents.
#[derive(Clone, Debug)]
pub struct ListPage {
    pub entries: Vec<ListEntry>,
    pub next_cursor: Option<ListCursor>,
}

impl StorageClient {
    pub fn connect(socket_path: impl AsRef<Path>) -> StorageResult<Self> {
        let stream = UnixStream::connect(socket_path)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(ClientConnection::new(stream, None, None))),
        })
    }

    /// Connect with descriptor accounting supplied by the embedding runtime.
    ///
    /// The policy reserves one descriptor before the socket is opened and is
    /// retained by the shared connection state. It is also used before each
    /// direct-I/O descriptor is received.
    pub fn connect_with_fd_policy(
        socket_path: impl AsRef<Path>,
        fd_policy: Box<dyn ExternalFdPolicy>,
    ) -> StorageResult<Self> {
        let socket_lease = fd_policy.acquire()?;
        let stream = UnixStream::connect(socket_path)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(ClientConnection::new(
                stream,
                Some(socket_lease),
                Some(fd_policy),
            ))),
        })
    }

    /// Connect with bounded blocking read/write calls.
    ///
    /// This is intended for long-lived background consumers that must remain
    /// responsive to shutdown even if the storage service or provider stalls.
    /// A zero timeout is rejected by `UnixStream` and therefore surfaces as an
    /// I/O error.
    pub fn connect_with_timeout(
        socket_path: impl AsRef<Path>,
        timeout: std::time::Duration,
    ) -> StorageResult<Self> {
        let stream = UnixStream::connect(socket_path)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Ok(Self {
            inner: Rc::new(RefCell::new(ClientConnection::new(stream, None, None))),
        })
    }

    fn connection(&self) -> StorageResult<RefMut<'_, ClientConnection>> {
        let connection = self.inner.try_borrow_mut().map_err(|_| {
            StorageError::protocol(
                "client connection is already in use; StorageClient is single-threaded and \
                 does not support reentrant calls",
            )
        })?;
        if !connection.is_usable() {
            return Err(StorageError::protocol(
                "storage client connection is poisoned",
            ));
        }
        Ok(connection)
    }

    /// Returns whether this connection generation is not known to be poisoned.
    ///
    /// A currently borrowed connection is treated as healthy. The next operation
    /// still reports the existing non-reentrant-use error instead of causing a
    /// connection manager to replace a live generation.
    pub fn is_usable(&self) -> bool {
        self.inner
            .try_borrow()
            .map_or(true, |connection| connection.is_usable())
    }

    /// Permanently invalidates this connection generation.
    ///
    /// Existing clones and file handles remain associated with the poisoned
    /// generation and will reject subsequent wire operations.
    pub fn invalidate(&self) -> StorageResult<()> {
        let mut connection = self.inner.try_borrow_mut().map_err(|_| {
            StorageError::protocol(
                "cannot invalidate storage client connection while it is in use",
            )
        })?;
        connection.poison();
        Ok(())
    }

    fn reject_unexpected<T>(
        &self,
        operation: &str,
        response: &WireResponsePayload,
    ) -> StorageResult<T> {
        let error = unexpected_response(operation, response);
        let _ = self.invalidate();
        Err(error)
    }

    pub fn open(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<StorageFile> {
        let (response, direct_fd) = self.request(WireRequestPayload::Open {
            store_id: store_id.into(),
            bucket: bucket.into(),
            key: key.into(),
            flags: OpenFlags::READ_ONLY,
        })?;
        match response {
            WireResponsePayload::Open {
                handle,
                size,
                direct_io,
            } => {
                let read_path = if direct_io {
                    let fd = direct_fd.ok_or_else(|| {
                        StorageError::protocol(
                            "direct open response did not include fd",
                        )
                    })?;
                    ReadPath::Direct(DirectReader::new(
                        std::fs::File::from(fd.fd),
                        fd.lease,
                    ))
                } else {
                    ReadPath::Mediated
                };
                Ok(StorageFile::new(self.clone(), handle, size, read_path))
            }
            other => self.reject_unexpected("open", &other),
        }
    }

    /// Fetches object metadata without opening a server-side read handle or admitting the object
    /// into the cache.
    pub fn head(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<ObjectInfo> {
        let (response, _) = self.request(WireRequestPayload::Head {
            store_id: store_id.into(),
            bucket: bucket.into(),
            key: key.into(),
        })?;
        match response {
            WireResponsePayload::Head { size, etag } => Ok(ObjectInfo { size, etag }),
            other => self.reject_unexpected("head", &other),
        }
    }

    /// Returns whether an object exists. Only `NotFound` maps to `false`; all other backend or
    /// protocol errors are returned to the caller unchanged.
    pub fn exists(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<bool> {
        match self.head(store_id, bucket, key) {
            Ok(_) => Ok(true),
            Err(error)
                if error.kind() == crate::error::StorageErrorKind::NotFound =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Finalizes a previously staged write by asking the server to upload the staging file to
    /// the backend. Returns the size and (when available) the backend etag of the newly
    /// uploaded object.
    ///
    /// Upload is object-publication only: the staging file is left on disk regardless of whether the upload
    /// succeeds. The caller (database) is responsible for unlinking the staging file once it
    /// no longer needs the local bytes: after a successful upload, on transaction abort before
    /// upload, or during crash recovery on database restart.
    ///
    /// Upload does **not** invalidate any cached copy of `(store_id, bucket, key)`. If the
    /// caller wants new opens to observe the just-uploaded bytes they must call
    /// [`Self::invalidate_object_cache`] explicitly.
    pub fn upload(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<UploadInfo> {
        let (response, _) = self.request(WireRequestPayload::Upload {
            store_id: store_id.into(),
            bucket: bucket.into(),
            key: key.into(),
        })?;
        match response {
            WireResponsePayload::Upload { size, etag } => {
                Ok(UploadInfo { size, etag })
            }
            other => self.reject_unexpected("upload", &other),
        }
    }

    pub fn register_store(
        &self,
        store_id: impl Into<String>,
        config: StoreConfig,
    ) -> StorageResult<bool> {
        let (response, _) = self.request(WireRequestPayload::RegisterStore {
            store_id: store_id.into(),
            config,
        })?;
        match response {
            WireResponsePayload::RegisterStore { replaced } => Ok(replaced),
            other => self.reject_unexpected("register-store", &other),
        }
    }

    pub fn unregister_store(
        &self,
        store_id: impl Into<String>,
    ) -> StorageResult<bool> {
        let (response, _) = self.request(WireRequestPayload::UnregisterStore {
            store_id: store_id.into(),
        })?;
        match response {
            WireResponsePayload::UnregisterStore { removed } => Ok(removed),
            other => self.reject_unexpected("unregister-store", &other),
        }
    }

    pub fn purge_store_cache(
        &self,
        store_id: impl Into<String>,
    ) -> StorageResult<()> {
        let (response, _) = self.request(WireRequestPayload::PurgeStoreCache {
            store_id: store_id.into(),
        })?;
        match response {
            WireResponsePayload::PurgeStoreCache => Ok(()),
            other => self.reject_unexpected("purge-store-cache", &other),
        }
    }

    /// Runs an explicit end-to-end probe against an already-registered backend.
    ///
    /// The server checks listing, a create-only temporary write, metadata/read-back, and
    /// deletion under `root_prefix`. Backend operation failures are returned in the structured
    /// result; request/protocol failures remain [`StorageError`] values.
    pub fn probe_store(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        root_prefix: impl Into<String>,
    ) -> StorageResult<StorageProbeResult> {
        let (response, _) = self.request(WireRequestPayload::ProbeStore {
            store_id: store_id.into(),
            bucket: bucket.into(),
            root_prefix: root_prefix.into(),
        })?;
        match response {
            WireResponsePayload::ProbeStore { result } => Ok(result),
            other => self.reject_unexpected("probe-store", &other),
        }
    }

    pub fn invalidate_object_cache(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<bool> {
        let (response, _) =
            self.request(WireRequestPayload::InvalidateObjectCache {
                store_id: store_id.into(),
                bucket: bucket.into(),
                key: key.into(),
            })?;
        match response {
            WireResponsePayload::InvalidateObjectCache { removed } => Ok(removed),
            other => self.reject_unexpected("invalidate-object-cache", &other),
        }
    }

    /// Deletes a single object from the backend.
    ///
    /// Idempotent: deleting a missing key is `Ok(())` regardless of the backend's native
    /// missing-key behavior. The server best-effort invalidates any local cache row for the key
    /// (skipped if the cache entry is currently active; the janitor will reclaim it later).
    pub fn delete(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<()> {
        let (response, _) = self.request(WireRequestPayload::Delete {
            store_id: store_id.into(),
            bucket: bucket.into(),
            key: key.into(),
        })?;
        match response {
            WireResponsePayload::Delete => Ok(()),
            other => self.reject_unexpected("delete", &other),
        }
    }

    /// Deletes every object whose key begins with `prefix`. Returns the number of objects the
    /// backend acknowledged removing.
    ///
    /// `prefix` must be non-empty (the empty string is rejected with `InvalidPath` to avoid an
    /// accidental "wipe the whole bucket" call). The operation is idempotent and safe to retry
    /// — a subsequent call simply finds nothing left to delete.
    ///
    /// **Connection ownership**: this call is a single RPC that runs to completion before
    /// returning. Because one [`StorageClient`] owns one non-multiplexed connection state machine,
    /// the same client (or any clone of it) should not be reused for another request until
    /// `delete_prefix` finishes. For large prefixes that is the dominant cost.
    ///
    /// **Scaling out**: for prefixes large enough that the single-RPC duration matters
    /// (millions of objects, or interleaving with concurrent reads on the same client), prefer
    /// driving the deletion explicitly via [`Self::list_page`] + [`Self::delete_objects`]
    /// in bounded batches. `delete_prefix` is a convenience method, not a bulk-throughput tool.
    pub fn delete_prefix(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> StorageResult<u64> {
        let (response, _) = self.request(WireRequestPayload::DeletePrefix {
            store_id: store_id.into(),
            bucket: bucket.into(),
            prefix: prefix.into(),
        })?;
        match response {
            WireResponsePayload::DeletePrefix { deleted } => Ok(deleted),
            other => self.reject_unexpected("delete-prefix", &other),
        }
    }

    /// Deletes one bounded group of object keys through the backend bulk-delete path.
    pub fn delete_objects(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        keys: Vec<String>,
    ) -> StorageResult<u32> {
        let (response, _) = self.request(WireRequestPayload::DeleteObjects {
            store_id: store_id.into(),
            bucket: bucket.into(),
            keys,
        })?;
        match response {
            WireResponsePayload::DeleteObjects { deleted } => Ok(deleted),
            other => self.reject_unexpected("delete-objects", &other),
        }
    }

    /// Fetches a single page of a `list` operation.
    ///
    /// Most callers should prefer [`Self::list`], which wraps this in an `Iterator` that pages
    /// transparently. `list_page` is the lower-level handle for callers that need to drive
    /// pagination explicitly (e.g. to persist cursors across process restarts or to interleave
    /// list with other work without blocking on a long iteration).
    ///
    /// `page_size = 0` lets the server pick a default. The server may clamp very large page
    /// sizes downwards.
    pub fn list_page(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        prefix: Option<&str>,
        cursor: Option<ListCursor>,
        page_size: u32,
    ) -> StorageResult<ListPage> {
        let (response, _) = self.request(WireRequestPayload::List {
            store_id: store_id.into(),
            bucket: bucket.into(),
            prefix: prefix.map(str::to_string),
            page_size,
            cursor,
        })?;
        match response {
            WireResponsePayload::List {
                entries,
                next_cursor,
            } => Ok(ListPage {
                entries: entries
                    .into_iter()
                    .map(|entry| ListEntry {
                        key: entry.key,
                        size: entry.size,
                        etag: entry.etag,
                        last_modified_ms: entry.last_modified_ms,
                    })
                    .collect(),
                next_cursor,
            }),
            other => self.reject_unexpected("list", &other),
        }
    }

    /// Releases a retained list cursor. Closing an expired cursor is idempotent.
    pub fn close_list_cursor(&self, cursor: ListCursor) -> StorageResult<()> {
        let (response, _) = self.request(WireRequestPayload::CloseList { cursor })?;
        match response {
            WireResponsePayload::CloseList => Ok(()),
            other => self.reject_unexpected("close-list", &other),
        }
    }

    /// Returns an iterator over every object whose key starts with `prefix` (or the entire
    /// `(store_id, bucket)` namespace when `prefix` is `None`).
    ///
    /// The iterator buffers one page at a time and refills transparently when the buffer
    /// drains. Pages are fetched at the server's default page size; for explicit control use
    /// [`Self::list_page`].
    ///
    /// Per-entry iteration: `Item = StorageResult<ListEntry>`. Iteration stops at the first
    /// `Err` (the underlying server-side cursor is dropped on the next refill); callers that
    /// want to keep going after a transient backend error should use [`Self::list_page`]
    /// directly so they can inspect and resume.
    pub fn list(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        prefix: Option<&str>,
    ) -> ListIter<'_> {
        ListIter::new(
            self,
            store_id.into(),
            bucket.into(),
            prefix.map(str::to_string),
        )
    }

    fn read_into(
        &self,
        handle: FileHandle,
        offset: u64,
        len: u32,
        buf: &mut [u8],
    ) -> StorageResult<usize> {
        let mut connection = self.connection()?;
        connection.read_into(handle, offset, len, buf)
    }

    /// Allocating read: sends a READ request, decodes the response header to learn the actual
    /// body size, allocates exactly that size, then streams the body into the new buffer.
    fn read_alloc(
        &self,
        handle: FileHandle,
        offset: u64,
        len: u32,
    ) -> StorageResult<Vec<u8>> {
        let mut connection = self.connection()?;
        connection.read_alloc(handle, offset, len)
    }

    fn request(
        &self,
        payload: WireRequestPayload,
    ) -> StorageResult<(WireResponsePayload, Option<ReceivedFd>)> {
        let mut connection = self.connection()?;
        connection.request(payload)
    }
}
