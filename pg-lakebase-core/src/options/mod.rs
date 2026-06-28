//! PostgreSQL catalog option extraction, persistence, and caches.
//!
//! This module is organized by catalog owner:
//! - table options are persisted in `lakebase.table_options` and cached in `rd_amcache`
//! - tablespace options are persisted in `pg_tablespace.spcoptions` and cached from syscache

mod schema;
pub mod table;
pub mod tablespace;

pub use schema::{
    OptionDef, OptionKind, OptionSchemaError, extract_and_remove_options,
};
pub use table::{
    AmCache, AmCacheRef, AmCacheString, AmCacheValue, AmCacheValueBuilder,
    AmCacheable, TableOptionError, TableOptions,
};
pub use tablespace::{
    CachedTablespaceOpts, TablespaceCacheError, TablespaceError, TablespaceOptions,
    TablespaceStorageError, get_tablespace, is_distributed_tablespace,
};
