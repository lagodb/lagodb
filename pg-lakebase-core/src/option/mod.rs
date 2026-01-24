//! Storage and table options handling module
//!
//! This module provides types and utilities for handling options in:
//! - `CREATE TABLESPACE` statements
//! - `CREATE TABLE` statements (with custom access methods)
//!
pub mod am_cache;
mod storage_option;
mod table_option;
pub mod tablespace_cache;
mod tablespace_option;
pub mod utils;

// Re-export commonly used types
pub use am_cache::{AmCache, AmCacheable};
pub use storage_option::{
    OptionKind, StorageCategory, TamOptionDef, extract_and_remove_options,
};
pub use table_option::{TableOptionError, TableOptions};
pub use tablespace_option::{TablespaceError, TablespaceOptions};
pub use utils::{append_string, get_string_at_offset};
