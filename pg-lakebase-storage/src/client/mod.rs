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
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::{BackendDataIdentity, StorageProbeResult, StoreConfig};
use crate::error::{StorageError, StorageResult};
use crate::handle::{FileHandle, OpenFlags};
use crate::object::ObjectInfo;
use crate::protocol::{ListCursor, WireRequestPayload, WireResponsePayload};

mod attach;
mod client_builder;
mod connection;
mod fd;
mod list;
mod socket;
mod socket_wait;
mod staging_file;
mod storage_file;

#[cfg(test)]
mod tests;

pub use client_builder::{DEFAULT_CLIENT_CLEANUP_TIMEOUT, StorageClientBuilder};
pub use fd::{ExternalFdLease, ExternalFdPolicy};
pub use list::ListIter;
pub use socket_wait::{SocketInterest, SocketWait, SocketWaitContext};
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
    backend_identity: Arc<BackendDataIdentity>,
}

// SAFETY: this implementation relies on the embedding PostgreSQL extension's
// closed-world execution invariant. `iceberg-lite` retains upstream `Send +
// Sync` storage-trait bounds, while the adapted synchronous execution model
// performs every operation, clone, and drop on one backend main thread.
// Moving or sharing a client across threads violates this contract. No runtime
// owner-thread check is placed in READ or other object-operation hot paths.
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
    pub fn builder(socket_path: impl AsRef<Path>) -> StorageClientBuilder {
        StorageClientBuilder::new(socket_path)
    }

    pub fn connect_managed(
        socket_path: impl AsRef<Path>,
        volume_id: u64,
    ) -> StorageResult<Self> {
        Self::builder(socket_path)
            .managed_volume(volume_id)
            .connect()
    }

    pub fn connect_configured(
        socket_path: impl AsRef<Path>,
        config: Arc<StoreConfig>,
    ) -> StorageResult<Self> {
        Self::builder(socket_path).configured(config).connect()
    }

    /// Connect with descriptor accounting supplied by the embedding runtime.
    ///
    /// The policy reserves one descriptor before the socket is opened and is
    /// retained by the shared connection state. It is also used before each
    /// direct-I/O descriptor is received.
    pub fn connect_managed_with_fd_policy(
        socket_path: impl AsRef<Path>,
        volume_id: u64,
        fd_policy: Box<dyn ExternalFdPolicy>,
    ) -> StorageResult<Self> {
        Self::builder(socket_path)
            .managed_volume(volume_id)
            .fd_policy(fd_policy)
            .connect()
    }

    /// Connect synchronously, then use bounded nonblocking socket operations.
    ///
    /// This is intended for long-lived background consumers that must remain
    /// responsive to shutdown even if the storage service or provider stalls.
    /// One absolute deadline covers each complete request/response exchange;
    /// the initial Unix-socket connect remains blocking. A zero timeout is
    /// rejected as invalid configuration.
    pub fn connect_managed_with_timeout(
        socket_path: impl AsRef<Path>,
        volume_id: u64,
        timeout: Duration,
    ) -> StorageResult<Self> {
        Self::builder(socket_path)
            .managed_volume(volume_id)
            .operation_timeout(timeout)
            .cleanup_timeout(timeout)
            .connect()
    }

    fn from_builder(builder: StorageClientBuilder) -> StorageResult<Self> {
        let (transport, fd_policy, attach) = builder.into_parts()?;
        let mut connection = ClientConnection::new(transport, fd_policy);
        let backend_identity = attach::attach(&mut connection, attach)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(connection)),
            backend_identity: Arc::new(backend_identity),
        })
    }

    pub fn backend_identity(&self) -> &BackendDataIdentity {
        &self.backend_identity
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

    /// Returns whether this usable client is owned only by a connection cache.
    ///
    /// This is intended for cold-path connection-cache eviction. Open
    /// [`StorageFile`] values retain a client clone, so they make the result
    /// false without adding any check to READ operations.
    pub fn is_unshared_and_usable(&self) -> bool {
        Rc::strong_count(&self.inner) == 1
            && self
                .inner
                .try_borrow()
                .is_ok_and(|connection| connection.is_usable())
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
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<StorageFile> {
        let (response, direct_fd) = self.request(WireRequestPayload::Open {
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
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<ObjectInfo> {
        let (response, _) = self.request(WireRequestPayload::Head {
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
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<bool> {
        match self.head(bucket, key) {
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
    /// Upload does **not** invalidate the cached physical object. If the
    /// caller wants new opens to observe the just-uploaded bytes they must call
    /// [`Self::invalidate_object_cache`] explicitly.
    pub fn upload(
        &self,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<UploadInfo> {
        let (response, _) = self.request(WireRequestPayload::Upload {
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

    /// Runs an explicit end-to-end probe against the attached backend.
    ///
    /// The server checks listing, a create-only temporary write, metadata/read-back, and
    /// deletion under `root_prefix`. Backend operation failures are returned in the structured
    /// result; request/protocol failures remain [`StorageError`] values.
    pub fn probe_store(
        &self,
        bucket: impl Into<String>,
        root_prefix: impl Into<String>,
    ) -> StorageResult<StorageProbeResult> {
        let (response, _) = self.request(WireRequestPayload::ProbeStore {
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
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<bool> {
        let (response, _) =
            self.request(WireRequestPayload::InvalidateObjectCache {
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
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<()> {
        let (response, _) = self.request(WireRequestPayload::Delete {
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
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> StorageResult<u64> {
        let (response, _) = self.request(WireRequestPayload::DeletePrefix {
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
        bucket: impl Into<String>,
        keys: Vec<String>,
    ) -> StorageResult<u32> {
        let (response, _) = self.request(WireRequestPayload::DeleteObjects {
            bucket: bucket.into(),
            keys,
        })?;
        match response {
            WireResponsePayload::DeleteObjects { deleted } => Ok(deleted),
            other => self.reject_unexpected("delete-objects", &other),
        }
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
        connection.request(payload, SocketWaitContext::Foreground)
    }

    fn request_cleanup(
        &self,
        payload: WireRequestPayload,
    ) -> StorageResult<(WireResponsePayload, Option<ReceivedFd>)> {
        let mut connection = self.connection()?;
        connection.request(payload, SocketWaitContext::Cleanup)
    }
}
