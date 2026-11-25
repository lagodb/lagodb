//! PostgreSQL-aware FileIO infrastructure shared by Iceberg frontends.

mod local;
mod local_file_wal;
mod local_retirement;
mod object;
mod post_commit_delete;
mod transaction_resources;

pub(crate) use local::LocalStorage;
pub(crate) use local_file_wal::init_wal_rmgr;
pub(crate) use local_retirement::LocalTableRetirement;
pub(crate) use object::{ObjectBucket, ObjectStorage};
pub(crate) use post_commit_delete::PostCommitFileDeleteBatch;
pub(crate) use transaction_resources::{
    MetadataMaterializationAttempt, register_table_dir_created,
};
