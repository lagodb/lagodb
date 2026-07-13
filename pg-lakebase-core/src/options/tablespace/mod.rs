//! Tablespace option schema, storage conversion, and syscache-backed lookup.

mod cache;
mod defs;
mod options;
mod storage;

pub use cache::{
    CachedTablespaceOpts, TablespaceCacheError, get_tablespace,
    is_distributed_tablespace,
};
pub use options::{TablespaceError, TablespaceOptions};
pub use storage::TablespaceStorageError;
