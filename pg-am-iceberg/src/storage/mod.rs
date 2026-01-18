pub mod local;
pub mod object;

pub use local::LocalStorage;
pub use object::ObjectStorage;

use crate::error::{IcebergError, IcebergResult};
use iceberg_lite::io::{FileIO, Storage};
use pg_lakehouse_core::option::tablespace_cache::{get_tablespace, is_distributed_tablespace};
use pgrx::pg_sys;
use std::ffi::CStr;
use std::sync::Arc;

/// Storage context containing FileIO and base path information.
///
/// This struct encapsulates all the information needed to interact with
/// Iceberg table storage, whether local or distributed (S3, Azure, etc.).
pub struct StorageContext {
    /// The FileIO instance for reading/writing files
    pub file_io: FileIO,
    /// Base path for table storage (DataDir for local, base URL for distributed)
    pub base_path: String,
    /// Whether this is a distributed storage (S3, Azure, etc.)
    pub is_distributed: bool,
}

/// Create a StorageContext based on the tablespace OID.
///
/// For distributed tablespaces (S3, Azure, etc.), creates an ObjectStorage-based FileIO
/// and returns the configured base URL.
///
/// For local tablespaces (pg_default, pg_global, etc.), creates a LocalStorage-based FileIO
/// and returns the PostgreSQL data directory as base path.
///
/// # Arguments
/// * `spc_oid` - The tablespace OID to create storage context for
///
/// # Returns
/// A `StorageContext` containing the FileIO, base path, and distributed flag
///
/// # Errors
/// Returns an error if the tablespace is distributed but not found in cache
pub fn create_storage_context(spc_oid: pg_sys::Oid) -> IcebergResult<StorageContext> {
    let is_distributed = is_distributed_tablespace(spc_oid);

    if is_distributed {
        let opts =
            get_tablespace(spc_oid)?.ok_or(IcebergError::TablespaceNotFound)?;
        let mut storage = ObjectStorage::new(opts.storage.protocol());
        let props = opts.storage.to_props();

        // Initialize the storage with properties (e.g., credentials, region)
        storage.initialize(props)?;

        let base_url = opts.storage.to_base_url();

        Ok(StorageContext {
            file_io: FileIO::new(Arc::new(storage)),
            base_path: base_url,
            is_distributed: true,
        })
    } else {
        let data_dir = unsafe {
            CStr::from_ptr(pg_sys::DataDir)
                .to_string_lossy()
                .to_string()
        };

        Ok(StorageContext {
            file_io: FileIO::new(Arc::new(LocalStorage::default())),
            base_path: data_dir,
            is_distributed: false,
        })
    }
}
