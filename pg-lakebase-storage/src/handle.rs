//! File-handle identity types shared across the wire protocol, session layer, and service.
//!
//! [`FileHandle`] and [`OpenFlags`] are pure data types that cross the wire inside
//! [`crate::protocol`]. The runtime open-file state ([`OpenFileState`]) lives here as well
//! because it is consumed by both [`crate::session::handle_table`] and [`crate::service`].

use std::fmt;
use std::sync::Arc;

use crate::object::ObjectLocation;

/// Numeric file handle identifying an open object on a connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct FileHandle(pub u64);

/// Open-mode flags sent with [`crate::protocol::WireRequestPayload::Open`].
///
/// Open is **read-only** in this service: write-side flows do not open a server-side handle at
/// all (they create local staging files and later call `Upload`). `OpenFlags` exists for
/// symmetry with typical file APIs and to leave room for future read-side mode bits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenFlags {
    pub read: bool,
}

impl OpenFlags {
    pub const READ_ONLY: Self = Self { read: true };
}

/// Per-connection open handle binding returned by [`crate::session::handle_table::HandleTable`] for
/// open, read, and close flows.
///
/// Carries the backend identity snapshot (size / etag) plus everything `READ` needs to serve
/// bytes without a further KV round trip. The residency ([`crate::cache::Residency`]) — when
/// present — embeds:
///
/// * the cache activity lease keeping the residency alive,
/// * the frozen metadata (for `SmallKv` and `CompleteFile`) or live fill session (for large
///   fills),
/// * the small-object payload (for `SmallKv`), pre-read at OPEN.
///
/// A `None` residency is only valid on a handle that was opened through the direct
/// [`crate::session::handle_table::HandleTable::open`] helper (tests). Production `handle_open` always
/// attaches a `Residency`.
#[derive(Clone)]
pub struct OpenFileState {
    pub handle: FileHandle,
    pub key: ObjectLocation,
    pub store: Arc<crate::backend::RegisteredStore>,
    pub size: u64,
    pub etag: Option<String>,
    pub flags: OpenFlags,
    pub(crate) residency: Option<Arc<crate::cache::Residency>>,
}

impl fmt::Debug for OpenFileState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenFileState")
            .field("handle", &self.handle)
            .field("key", &self.key)
            .field("store", &self.store)
            .field("size", &self.size)
            .field("etag", &self.etag)
            .field("flags", &self.flags)
            .field(
                "residency",
                &self.residency.as_ref().map(|r| r.state_hint()),
            )
            .finish()
    }
}
