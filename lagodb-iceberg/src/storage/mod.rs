//! PostgreSQL-aware FileIO infrastructure shared by Iceberg frontends.

mod injection_points;
pub(crate) mod local;
pub(crate) mod local_file_wal;
mod local_reservation;
mod local_retirement;
pub(crate) mod object;
pub(crate) mod object_uri;
mod post_commit_delete;
pub(crate) mod transaction_resources;
mod wait_event;

pub(crate) use local::LocalStorage;
pub(crate) use local_reservation::LocalTableReservation;
pub(crate) use local_retirement::LocalTableRetirement;
pub(crate) use object::ObjectStorage;
pub(crate) use object::{ObjectReader, ObjectWriter, storage_err};
pub(crate) use post_commit_delete::{
    PostCommitDeletePurpose, PostCommitFileDeleteBatch,
};
pub(crate) use wait_event::{StorageWaitEvent, StorageWaitGuard};
