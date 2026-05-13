//! Typed command verbs decoded from [`crate::protocol`].
//!
//! One [`StorageCommand`] variant exists per inbound wire operation. The corresponding *outputs*
//! (and their attachments) live in [`crate::service::reply`] so the input and result vocabularies
//! stay on opposite sides of the service boundary.

use crate::backend::StoreConfig;
use crate::handle::{FileHandle, OpenFlags};
use crate::protocol::ListCursor;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StorageCommand {
    Open(OpenCommand),
    Head(HeadCommand),
    Read(ReadCommand),
    Close(CloseCommand),
    StageCreate(StageCreateCommand),
    Commit(CommitCommand),
    Abort(AbortCommand),
    RegisterStore(RegisterStoreCommand),
    UnregisterStore(UnregisterStoreCommand),
    PurgeStoreCache(PurgeStoreCacheCommand),
    InvalidateObjectCache(InvalidateObjectCacheCommand),
    Delete(DeleteCommand),
    DeletePrefix(DeletePrefixCommand),
    List(ListCommand),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpenCommand {
    pub store_id: String,
    pub bucket: String,
    pub key: String,
    pub flags: OpenFlags,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HeadCommand {
    pub store_id: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegisterStoreCommand {
    pub store_id: String,
    pub config: StoreConfig,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnregisterStoreCommand {
    pub store_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PurgeStoreCacheCommand {
    pub store_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InvalidateObjectCacheCommand {
    pub store_id: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadCommand {
    pub handle: FileHandle,
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CloseCommand {
    pub handle: FileHandle,
}

/// StageCreate / Commit / Abort are all addressed by `(store_id, bucket, key)`. They never carry
/// a server-side handle because staging is intentionally stateless on the server: the client
/// writes to the returned path directly, and commit/abort are identity-addressable from any
/// future connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StageCreateCommand {
    pub store_id: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitCommand {
    pub store_id: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AbortCommand {
    pub store_id: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeleteCommand {
    pub store_id: String,
    pub bucket: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeletePrefixCommand {
    pub store_id: String,
    pub bucket: String,
    pub prefix: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListCommand {
    pub store_id: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub page_size: u32,
    pub cursor: Option<ListCursor>,
}
