//! Blocking Unix peer for tests / tools / foreign-language wrappers.
//!
//! The client exposes three surfaces:
//!
//! * [`StorageClient`] — connection-bound request/response primitive. Cloneable; all clones
//!   share one underlying Unix stream protected by a mutex, so concurrent calls from different
//!   clones are safe but serialize on the lock.
//! * [`StorageFile`]   — open read handle returned by [`StorageClient::open`]. Seek / read / close.
//! * [`StagingFile`]   — local file handle returned by [`StorageClient::stage`]. The server is not
//!   in the data path for writes: the client appends to the returned path directly. Finalization
//!   (commit / abort) is issued through [`StorageClient`] and is addressed by
//!   `(store_id, bucket, key)` — it does not need the same connection that created the staging
//!   file.
//!
//! That last property is the whole point of the staging surface: a database transaction can
//! write a staging file from one short-lived client connection, close the connection, and hours
//! later commit or abort from any other connection so long as it knows the identity tuple.

use std::fs::OpenOptions;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::backend::StoreConfig;
use crate::error::{StorageError, StorageResult};
use crate::handle::{FileHandle, OpenFlags};
use crate::object::ObjectInfo;
use crate::protocol::{
    decode_response, encode_read_request, encode_request, ListCursor, ReadResponsePrefix, ResponseFrameHeader,
    WireRequest, WireRequestPayload, WireResponsePayload,
};
use crate::transport::{read_fd_blocking, read_frame_blocking, write_frame_blocking, BlockingFrameCursor};

mod list;
mod staging_file;
mod storage_file;

#[cfg(test)]
mod tests;

pub use list::ListIter;
pub use staging_file::StagingFile;
pub use storage_file::{SeekFrom, StorageFile};

use storage_file::{DirectReader, ReadPath};

fn unexpected_response(operation: &str, got: &WireResponsePayload) -> StorageError {
    StorageError::protocol(format!("unexpected {operation} response: {got:?}"))
}

/// Connection-bound request/response handle to a storage server over a Unix socket.
///
/// Cloneable: all clones share one underlying stream protected by a mutex. Concurrent calls
/// from different clones are thread-safe but serialize on the internal lock — this is
/// intentional for a blocking test/tool client. For high-throughput concurrent access, open
/// multiple connections instead.
///
/// # Long-running calls
///
/// Some methods can occupy the connection for a long time (notably
/// [`Self::delete_prefix`], which lists and deletes every matching object before returning).
/// While such a call is in flight, every other call on the same `StorageClient` (or any
/// clone of it) blocks on the internal stream mutex. If a workload mixes long-running
/// admin calls with latency-sensitive reads, dedicate separate `StorageClient`
/// instances to each.
#[derive(Clone)]
pub struct StorageClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    stream: Mutex<UnixStream>,
    next_request_id: AtomicU64,
}

impl ClientInner {
    fn lock_stream(&self) -> StorageResult<MutexGuard<'_, UnixStream>> {
        self.stream.lock().map_err(|_| {
            StorageError::protocol("client stream mutex poisoned; client connection state is no longer trustworthy")
        })
    }
}

/// Reported outcome of a successful [`StorageClient::commit`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitInfo {
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

struct ReadIntoOutcome {
    bytes_read: usize,
}

impl StorageClient {
    pub fn connect(socket_path: impl AsRef<Path>) -> StorageResult<Self> {
        let stream = UnixStream::connect(socket_path)?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                stream: Mutex::new(stream),
                next_request_id: AtomicU64::new(1),
            }),
        })
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
                    let fd =
                        direct_fd.ok_or_else(|| StorageError::protocol("direct open response did not include fd"))?;
                    ReadPath::Direct(DirectReader::new(std::fs::File::from(fd)))
                } else {
                    ReadPath::Mediated
                };
                Ok(StorageFile::new(self.clone(), handle, size, read_path))
            },
            other => Err(unexpected_response("open", &other)),
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
            other => Err(unexpected_response("head", &other)),
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
            Err(error) if error.kind() == crate::error::StorageErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Creates a staging file for `(store_id, bucket, key)` and returns a [`StagingFile`] pointed
    /// at the server-supplied absolute path.
    ///
    /// The file is opened locally in append-only mode to match the documented single-writer
    /// semantic. The server never sees writes to the file — only the subsequent
    /// [`StorageClient::commit`] or [`StorageClient::abort`] for the same key.
    pub fn stage(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<StagingFile> {
        let (response, _) = self.request(WireRequestPayload::StageCreate {
            store_id: store_id.into(),
            bucket: bucket.into(),
            key: key.into(),
        })?;
        match response {
            WireResponsePayload::StageCreate { staging_path } => {
                let path = PathBuf::from(&staging_path);
                // O_APPEND guarantees the "append-only, single writer" semantic we document: even
                // a misbehaving caller cannot rewind over bytes it already wrote. `create(false)`
                // keeps the client honest about the fact that StageCreate already created the
                // file — if another process somehow removed the file between StageCreate and
                // here, we want to surface the error rather than silently re-creating it.
                let file = OpenOptions::new()
                    .append(true)
                    .read(false)
                    .create(false)
                    .custom_flags(libc::O_CLOEXEC)
                    .open(&path)
                    .map_err(|error| {
                        StorageError::io(format!("open staging file returned by server {}", path.display()), error)
                    })?;
                Ok(StagingFile::new(file, path))
            },
            other => Err(unexpected_response("stage create", &other)),
        }
    }

    /// Finalizes a previously staged write by asking the server to upload the staging file to
    /// the backend. Returns the size and (when available) the backend etag of the newly
    /// uploaded object.
    ///
    /// Commit does **not** invalidate any cached copy of `(store_id, bucket, key)`. If the
    /// caller wants new opens to observe the just-uploaded bytes they must call
    /// [`Self::invalidate_object_cache`] explicitly.
    pub fn commit(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<CommitInfo> {
        let (response, _) = self.request(WireRequestPayload::Commit {
            store_id: store_id.into(),
            bucket: bucket.into(),
            key: key.into(),
        })?;
        match response {
            WireResponsePayload::Commit { size, etag } => Ok(CommitInfo { size, etag }),
            other => Err(unexpected_response("commit", &other)),
        }
    }

    /// Deletes the staging file for `(store_id, bucket, key)` without uploading. Missing
    /// staging files are treated as success so aborting twice is safe.
    pub fn abort(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<()> {
        let (response, _) = self.request(WireRequestPayload::Abort {
            store_id: store_id.into(),
            bucket: bucket.into(),
            key: key.into(),
        })?;
        match response {
            WireResponsePayload::Abort => Ok(()),
            other => Err(unexpected_response("abort", &other)),
        }
    }

    pub fn register_store(&self, store_id: impl Into<String>, config: StoreConfig) -> StorageResult<bool> {
        let (response, _) = self.request(WireRequestPayload::RegisterStore {
            store_id: store_id.into(),
            config,
        })?;
        match response {
            WireResponsePayload::RegisterStore { replaced } => Ok(replaced),
            other => Err(unexpected_response("register-store", &other)),
        }
    }

    pub fn unregister_store(&self, store_id: impl Into<String>) -> StorageResult<bool> {
        let (response, _) = self.request(WireRequestPayload::UnregisterStore {
            store_id: store_id.into(),
        })?;
        match response {
            WireResponsePayload::UnregisterStore { removed } => Ok(removed),
            other => Err(unexpected_response("unregister-store", &other)),
        }
    }

    pub fn purge_store_cache(&self, store_id: impl Into<String>) -> StorageResult<()> {
        let (response, _) = self.request(WireRequestPayload::PurgeStoreCache {
            store_id: store_id.into(),
        })?;
        match response {
            WireResponsePayload::PurgeStoreCache => Ok(()),
            other => Err(unexpected_response("purge-store-cache", &other)),
        }
    }

    pub fn invalidate_object_cache(
        &self,
        store_id: impl Into<String>,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> StorageResult<bool> {
        let (response, _) = self.request(WireRequestPayload::InvalidateObjectCache {
            store_id: store_id.into(),
            bucket: bucket.into(),
            key: key.into(),
        })?;
        match response {
            WireResponsePayload::InvalidateObjectCache { removed } => Ok(removed),
            other => Err(unexpected_response("invalidate-object-cache", &other)),
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
            other => Err(unexpected_response("delete", &other)),
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
    /// returning. Because [`StorageClient`] serializes all requests on the underlying socket
    /// (see the type-level docs), every other call made through the same `StorageClient` (or any
    /// of its clones) will block until `delete_prefix` finishes. For large prefixes that is the
    /// dominant cost.
    ///
    /// **Scaling out**: for prefixes large enough that the single-RPC duration matters
    /// (millions of objects, or interleaving with concurrent reads on the same client), prefer
    /// driving the deletion explicitly via [`Self::list`] + [`Self::delete`] in parallel
    /// batches across multiple `StorageClient` connections. `delete_prefix` is a convenience
    /// method, not a bulk-throughput tool.
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
            other => Err(unexpected_response("delete-prefix", &other)),
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
            WireResponsePayload::List { entries, next_cursor } => Ok(ListPage {
                entries: entries
                    .into_iter()
                    .map(|entry| ListEntry {
                        key: entry.key,
                        size: entry.size,
                        etag: entry.etag,
                    })
                    .collect(),
                next_cursor,
            }),
            other => Err(unexpected_response("list", &other)),
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
    pub fn list(&self, store_id: impl Into<String>, bucket: impl Into<String>, prefix: Option<&str>) -> ListIter<'_> {
        ListIter::new(self, store_id.into(), bucket.into(), prefix.map(str::to_string))
    }

    /// Sends a READ request and decodes the response header/prefix, returning the cursor
    /// positioned at the body start plus the decoded prefix.
    fn send_read_request<'a>(
        &self,
        handle: FileHandle,
        offset: u64,
        len: u32,
        stream: &'a mut std::os::unix::net::UnixStream,
    ) -> StorageResult<(BlockingFrameCursor<'a, std::os::unix::net::UnixStream>, ReadResponsePrefix)> {
        let request_id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        let frame = encode_read_request(request_id, handle, offset, len);
        write_frame_blocking(&mut *stream, &frame)?;

        let mut response_frame =
            BlockingFrameCursor::read_from(&mut *stream)?.ok_or_else(|| StorageError::protocol("connection closed"))?;
        let mut header_bytes = [0_u8; ResponseFrameHeader::ENCODED_LEN];
        response_frame.read_exact(&mut header_bytes)?;
        let header = ResponseFrameHeader::decode(&header_bytes)?;
        if header.request_id != request_id {
            response_frame.discard_remaining()?;
            return Err(StorageError::protocol(format!(
                "response id {} did not match request id {request_id}",
                header.request_id
            )));
        }

        if !header.is_read() {
            let response_frame = response_frame.read_remaining_after(&header_bytes)?;
            let response = decode_response(&response_frame)?;
            let other = response.into_result()?;
            return Err(unexpected_response("read", &other));
        }

        let mut read_tail = [0_u8; ReadResponsePrefix::TAIL_LEN];
        response_frame.read_exact(&mut read_tail)?;
        let prefix = ReadResponsePrefix::decode_tail(header, &read_tail)?;
        if response_frame.remaining() != prefix.data_len {
            let remaining = response_frame.remaining();
            response_frame.discard_remaining()?;
            return Err(StorageError::protocol(format!(
                "read response frame length mismatch: header announced {} data bytes, frame has {remaining}",
                prefix.data_len
            )));
        }
        Ok((response_frame, prefix))
    }

    fn read_into(&self, handle: FileHandle, offset: u64, len: u32, buf: &mut [u8]) -> StorageResult<ReadIntoOutcome> {
        let mut stream = self.inner.lock_stream()?;
        let (mut response_frame, prefix) = self.send_read_request(handle, offset, len, &mut stream)?;
        if prefix.data_len > buf.len() {
            response_frame.discard_remaining()?;
            return Err(StorageError::protocol(format!(
                "read response data length {} exceeds caller buffer length {}",
                prefix.data_len,
                buf.len()
            )));
        }
        response_frame.read_exact(&mut buf[..prefix.data_len])?;
        Ok(ReadIntoOutcome {
            bytes_read: prefix.data_len,
        })
    }

    /// Allocating read: sends a READ request, decodes the response header to learn the actual
    /// body size, allocates exactly that size, then streams the body into the new buffer.
    fn read_alloc(&self, handle: FileHandle, offset: u64, len: u32) -> StorageResult<Vec<u8>> {
        let mut stream = self.inner.lock_stream()?;
        let (mut response_frame, prefix) = self.send_read_request(handle, offset, len, &mut stream)?;
        let mut data = vec![0u8; prefix.data_len];
        response_frame.read_exact(&mut data)?;
        Ok(data)
    }

    fn request(
        &self,
        payload: WireRequestPayload,
    ) -> StorageResult<(WireResponsePayload, Option<std::os::fd::OwnedFd>)> {
        let request_id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        let request = WireRequest { request_id, payload };
        let frame = encode_request(&request)?;
        let mut stream = self.inner.lock_stream()?;
        write_frame_blocking(&mut *stream, &frame)?;
        let response_frame =
            read_frame_blocking(&mut *stream)?.ok_or_else(|| StorageError::protocol("connection closed"))?;
        let response = decode_response(&response_frame)?;
        if response.request_id != request_id {
            return Err(StorageError::protocol(format!(
                "response id {} did not match request id {request_id}",
                response.request_id
            )));
        }
        let payload = response.into_result()?;
        let fd = if matches!(payload, WireResponsePayload::Open { direct_io: true, .. }) {
            Some(read_fd_blocking(&mut stream)?)
        } else {
            None
        };
        Ok((payload, fd))
    }
}
