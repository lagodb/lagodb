//! PostgreSQL-aware FileIO infrastructure shared by Iceberg frontends.

mod injection_points;
pub(crate) mod local;
pub(crate) mod local_file_wal;
pub(crate) mod object;
mod object_uri;
mod post_commit_delete;
pub(crate) mod transaction_resources;
mod wait_event;

pub(crate) use local::LocalStorage;
pub(crate) use object::ObjectStorage;
pub(crate) use object::{ObjectReader, ObjectWriter, storage_err};
pub(crate) use post_commit_delete::{
    PostCommitDeletePurpose, PostCommitFileDeleteBatch,
};
pub(crate) use wait_event::{StorageWaitEvent, StorageWaitGuard};
