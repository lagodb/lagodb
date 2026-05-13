//! Table option persistence and relation-local caching.

mod cache;
mod layout;
mod options;

pub use cache::{AmCache, AmCacheable};
pub use layout::{AmCacheLayout, AmCacheLayoutBuilder, AmCacheStringOffset};
pub use options::{TableOptionError, TableOptions};
