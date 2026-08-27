//! Storage-volume binding options and syscache-backed lookup.

mod cache;
mod defs;
mod options;

pub use cache::{
    CachedTablespaceOpts, TablespaceCacheError, get_tablespace,
    is_distributed_tablespace,
};
pub use defs::{
    INTERNAL_STORAGE_VOLUME_ID_OPTION, PUBLIC_STORAGE_VOLUME_OPTION,
    is_lagodb_tablespace_option,
};
pub use options::{
    CreateTablespaceStorageOptions, TablespaceBinding, TablespaceError,
};
