//! Table option persistence and relation-local caching.

mod cache;
mod options;

pub use cache::{
    AmCache, AmCacheRef, AmCacheString, AmCacheValue, AmCacheValueBuilder,
    AmCacheable,
};
pub use options::{TableOptionError, TableOptions};
