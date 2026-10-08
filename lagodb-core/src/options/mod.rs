//! PostgreSQL catalog option extraction, persistence, and caches.
//!
//! This module is organized by catalog owner:
//! - table options are persisted in `lagodb.table_options` and cached in `rd_amcache`

mod schema;
pub mod table;

pub use schema::{
    OptionAccess, OptionDef, OptionKind, OptionSchemaError,
    extract_and_remove_options,
};
pub use table::{
    AmCache, AmCacheRef, AmCacheString, AmCacheValue, AmCacheValueBuilder,
    AmCacheable, TableOptionAlterations, TableOptionError, TableOptions,
};
