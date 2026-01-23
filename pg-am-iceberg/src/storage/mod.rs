pub mod local;
pub mod object;

pub use local::LocalStorage;
pub use object::ObjectStorage;

use crate::error::{IcebergError, IcebergResult};
use iceberg_lite::io::{FileIO, Storage};
use pg_lakehouse_core::handles::RelationHandle;
use pg_lakehouse_core::option::tablespace_cache::{
    get_tablespace, is_distributed_tablespace,
};
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
    /// Whether WAL logging is needed for this storage context
    pub needs_wal: bool,
}

/// Create a StorageContext based on the tablespace OID.
///
/// For distributed tablespaces (S3, Azure, etc.), creates an ObjectStorage-based FileIO
/// and returns the configured base URL. WAL is not needed for distributed storage.
///
/// For local tablespaces (pg_default, pg_global, etc.), creates a LocalStorage-based FileIO
/// and returns the PostgreSQL data directory as base path. WAL is enabled if the system
/// requires it (i.e., `wal_level >= archive`).
///
/// # Arguments
/// * `spc_oid` - The tablespace OID to create storage context for
///
/// # Returns
/// A `StorageContext` containing the FileIO, base path, distributed flag, and WAL flag
///
/// # Errors
/// Returns an error if the tablespace is distributed but not found in cache
pub fn create_storage_context(spc_oid: pg_sys::Oid) -> IcebergResult<StorageContext> {
    // For read-only context, WAL is not needed
    create_storage_context_with_wal(spc_oid, false)
}

/// Create a StorageContext with WAL support based on tablespace OID and relation requirements.
///
/// This function is similar to `create_storage_context`, but allows specifying whether
/// the relation needs WAL logging. WAL is only enabled for local storage when:
/// - The caller indicates WAL is needed (typically based on `RelationNeedsWAL`)
///
/// # Arguments
/// * `spc_oid` - The tablespace OID to create storage context for
/// * `relation_needs_wal` - Whether the relation requires WAL logging
///
/// # Returns
/// A `StorageContext` with appropriate WAL configuration
///
/// # Errors
/// Returns an error if the tablespace is distributed but not found in cache
pub fn create_storage_context_with_wal(
    spc_oid: pg_sys::Oid,
    relation_needs_wal: bool,
) -> IcebergResult<StorageContext> {
    let is_distributed = is_distributed_tablespace(spc_oid);

    if is_distributed {
        // Distributed storage (S3, Azure, etc.) doesn't need WAL
        // These systems provide their own durability guarantees
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
            needs_wal: false,
        })
    } else {
        // Local storage - determine if WAL is needed
        // WAL is needed only if:
        // 1. The relation needs WAL (not unlogged/temp table)
        let needs_wal = relation_needs_wal;

        let data_dir = unsafe {
            CStr::from_ptr(pg_sys::DataDir)
                .to_string_lossy()
                .to_string()
        };

        Ok(StorageContext {
            file_io: FileIO::new(Arc::new(LocalStorage::with_wal(needs_wal))),
            base_path: data_dir,
            is_distributed: false,
            needs_wal,
        })
    }
}

/// Create a StorageContext for a specific relation, automatically determining WAL requirements.
///
/// This is a convenience function that combines tablespace lookup with WAL requirement
/// detection. It checks both the tablespace type and the relation's persistence settings
/// to determine if WAL logging should be enabled.
///
/// # Arguments
/// * `spc_oid` - The tablespace OID
/// * `rel` - The relation pointer (used to check if WAL is needed)
///
/// # Returns
/// A `StorageContext` with appropriate configuration for the relation
///
/// # Errors
/// Returns an error if the tablespace is distributed but not found in cache
///
/// # Safety
/// The caller must ensure the relation pointer is valid.
pub fn create_storage_context_for_relation(
    spc_oid: pg_sys::Oid,
    rel: pg_sys::Relation,
) -> IcebergResult<StorageContext> {
    let relation_needs_wal = unsafe { RelationHandle::from_raw(rel).needs_wal() };
    create_storage_context_with_wal(spc_oid, relation_needs_wal)
}
