use std::sync::Arc;

use crate::backend::{StorageProbeResult, StoreConfig};
use crate::error::{StorageError, StorageErrorKind, StorageResult};
use crate::handle::{FileHandle, OpenFlags};

/// Opaque server-issued cursor used to fetch the next page of a `List` request. The bytes inside
/// are server-private and the client must round-trip the value unchanged.
///
/// A `ListCursor` is bound to the server-side iterator that produced it; it has a
/// connection-local lifetime. Closing the socket releases the cursor and its backend stream.
///
/// The inner string is intentionally not exposed: a cursor must come from a server response,
/// never be hand-crafted by the client. The codec layer reaches the inner bytes through
/// [`crate::protocol::cursor_codec`] which is `pub(crate)` to the protocol module.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ListCursor(String);

impl ListCursor {
    /// Construct a cursor from the wire bytes. Crate-private so only the server-side issue path
    /// (in [`crate::service::list_session`]) and the codec can build cursors; clients can only
    /// receive them in a [`crate::protocol::WireResponsePayload::List`] response.
    pub(crate) fn from_wire(value: String) -> Self {
        Self(value)
    }

    /// Borrow the cursor bytes for codec use.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// One element of a `List` response: an object key under the attached context's bucket plus
/// the same `(size, etag)` facts that `head` would have surfaced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireListEntry {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified_ms: Option<i64>,
}

/// Client→server operation body after decode ([`crate::protocol::decode_request`]).
///
/// Staging is invisible on the wire: the database (caller) writes its own files into the staging
/// tree using [`crate::staging::StagingPathResolver`]. `Upload` is the only staging-related verb
/// the server exposes; it carries `(bucket, key)` instead of a [`FileHandle`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireRequestPayload {
    AttachManaged {
        volume_id: u64,
    },
    AttachConfigured {
        config: Arc<StoreConfig>,
    },
    Open {
        bucket: String,
        key: String,
        flags: OpenFlags,
    },
    Head {
        bucket: String,
        key: String,
    },
    Read {
        handle: FileHandle,
        offset: u64,
        len: u32,
    },
    Close {
        handle: FileHandle,
    },
    Upload {
        bucket: String,
        key: String,
    },
    /// Exercises the attached backend under `root_prefix` without involving cache or staging.
    ProbeStore {
        bucket: String,
        root_prefix: String,
    },
    InvalidateObjectCache {
        bucket: String,
        key: String,
    },
    /// Deletes a single object from the backend and best-effort invalidates the local cache.
    Delete {
        bucket: String,
        key: String,
    },
    /// Deletes every object whose key starts with `prefix`. The prefix is required to be
    /// non-empty so a stray `""` does not become a "wipe the whole bucket" request.
    DeletePrefix {
        bucket: String,
        prefix: String,
    },
    /// Deletes one bounded set of keys using the backend bulk-delete path.
    DeleteObjects {
        bucket: String,
        keys: Vec<String>,
    },
    /// Lists objects in a bucket whose key starts with `prefix`. `cursor` is `None`
    /// for the first page; subsequent calls echo the `next_cursor` returned by the server.
    /// `page_size = 0` means "let the server pick a default".
    List {
        bucket: String,
        prefix: Option<String>,
        page_size: u32,
        cursor: Option<ListCursor>,
    },
    /// Releases a retained list cursor without draining the remaining stream.
    CloseList {
        cursor: ListCursor,
    },
}

/// Correlates multiplexed frames on one Unix connection (`request_id` echoes in responses).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireRequest {
    pub request_id: u64,
    pub payload: WireRequestPayload,
}

/// Server→client body; [`WireResponsePayload::Error`] maps [`StorageErrorKind`] + message for clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireResponsePayload {
    Attach {
        backend_identity: String,
    },
    Open {
        handle: FileHandle,
        size: u64,
        direct_io: bool,
    },
    Head {
        size: u64,
        etag: Option<String>,
    },
    Read {
        data: Vec<u8>,
        eof: bool,
    },
    Close,
    /// Reply to `Upload`: the upload size and backend etag (when the backend reported one). A
    /// successful `Upload` does **not** touch cache state; if a cached entry for the same key
    /// already exists, callers who want to observe the newly uploaded bytes must follow up with
    /// `InvalidateObjectCache`. The staging file is left on disk; the database (caller) owns
    /// staging-directory cleanup.
    Upload {
        size: u64,
        etag: Option<String>,
    },
    ProbeStore {
        result: StorageProbeResult,
    },
    InvalidateObjectCache {
        removed: bool,
    },
    /// Reply to `Delete`. Carries no fields: backends disagree on whether deleting a missing key
    /// is success or `NotFound`, so synthesising an `existed: bool` would mislead the caller.
    Delete,
    /// Reply to `DeletePrefix`: number of objects the backend acknowledged removing.
    DeletePrefix {
        deleted: u64,
    },
    DeleteObjects {
        deleted: u32,
    },
    /// Reply to `List`. `next_cursor = None` indicates the listing is complete; otherwise the
    /// client must echo the cursor on its next `List` call to fetch the next page.
    List {
        entries: Vec<WireListEntry>,
        next_cursor: Option<ListCursor>,
    },
    CloseList,
    Error {
        kind: StorageErrorKind,
        message: String,
    },
}

/// Paired with [`WireRequest::request_id`] for correlation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireResponse {
    pub request_id: u64,
    pub payload: WireResponsePayload,
}

impl WireResponse {
    pub fn error(request_id: u64, error: StorageError) -> Self {
        Self {
            request_id,
            payload: WireResponsePayload::Error {
                kind: error.kind(),
                message: error.wire_message(),
            },
        }
    }

    pub fn into_result(self) -> StorageResult<WireResponsePayload> {
        match self.payload {
            WireResponsePayload::Error { kind, message } => {
                Err(StorageError::from_wire(kind, message))
            }
            payload => Ok(payload),
        }
    }
}
