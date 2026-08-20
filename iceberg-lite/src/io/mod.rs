mod file_io;
mod storage_config;
mod storage_factory;
mod storage_fs;
mod storage_memory;

pub use file_io::*;
pub use storage_config::*;
pub use storage_factory::*;
pub use storage_fs::*;
pub use storage_memory::*;
pub(crate) mod object_cache;

pub(crate) fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}
