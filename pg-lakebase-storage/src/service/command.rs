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
    Upload(UploadCommand),
    RegisterStore(RegisterStoreCommand),
    UnregisterStore(UnregisterStoreCommand),
    PurgeStoreCache(PurgeStoreCacheCommand),
    ProbeStore(ProbeStoreCommand),
    InvalidateObjectCache(InvalidateObjectCacheCommand),
    Delete(DeleteCommand),
    DeletePrefix(DeletePrefixCommand),
    DeleteObjects(DeleteObjectsCommand),
    List(ListCommand),
    CloseList(CloseListCommand),
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
pub(crate) struct ProbeStoreCommand {
    pub store_id: String,
    pub bucket: String,
    pub root_prefix: String,
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

/// `Upload` is addressed by `(store_id, bucket, key)` rather than a server-side handle: staging
/// is intentionally stateless on the server. The database (caller) wrote the file directly into
/// the staging directory through the filesystem (paths derived via
/// [`crate::staging::StagingPathResolver`]) and Upload just asks the server to upload that local
/// file to the backend. Cleanup of the staging directory is the database's responsibility — the
/// server has neither a stage-create nor an abort verb.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UploadCommand {
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
pub(crate) struct DeleteObjectsCommand {
    pub store_id: String,
    pub bucket: String,
    pub keys: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ListCommand {
    pub store_id: String,
    pub bucket: String,
    pub prefix: Option<String>,
    pub page_size: u32,
    pub cursor: Option<ListCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CloseListCommand {
    pub cursor: ListCursor,
}
