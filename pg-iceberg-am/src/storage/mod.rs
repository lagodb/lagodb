pub mod local;
pub mod object;
pub mod transactional_artifacts;

pub use local::LocalStorage;
pub use object::ObjectStorage;

use crate::error::IcebergResult;
use iceberg_lite::io::FileIO;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::options::get_tablespace;
use pg_lakebase_core::worker::storage as storage_worker;
use pg_lakebase_storage::{StagingPathResolver, StorageClient};
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
    if get_tablespace(spc_oid)?.is_some() {
        let socket_path = storage_worker::resolved_socket_path();
        let cache_dir = storage_worker::resolved_cache_dir();
        let client = StorageClient::connect(&socket_path)?;
        let resolver = StagingPathResolver::new(cache_dir);
        return create_storage_context_with_client(
            spc_oid,
            relation_needs_wal,
            client,
            resolver,
        );
    }

    create_local_storage_context(relation_needs_wal)
}

pub fn create_storage_context_with_client(
    spc_oid: pg_sys::Oid,
    relation_needs_wal: bool,
    storage_client: StorageClient,
    staging_resolver: StagingPathResolver,
) -> IcebergResult<StorageContext> {
    if let Some(opts) = get_tablespace(spc_oid)? {
        let store_id = opts.store_id();
        let object_namespace = opts.object_namespace();
        let storage = ObjectStorage::new(
            opts.url_scheme(),
            store_id,
            object_namespace,
            storage_client,
            staging_resolver,
        )?;

        // Distributed stores are registered by the storage bgworker's
        // tablespace reconciler at startup and kept in sync via
        // pg_tablespace syscache invalidation, so we deliberately do not
        // register from this on-demand path.

        Ok(StorageContext {
            file_io: FileIO::new(Arc::new(storage)),
            base_path: opts.base_url(),
            is_distributed: true,
            needs_wal: false,
        })
    } else {
        create_local_storage_context(relation_needs_wal)
    }
}

fn create_local_storage_context(
    relation_needs_wal: bool,
) -> IcebergResult<StorageContext> {
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
pub unsafe fn create_storage_context_for_relation(
    spc_oid: pg_sys::Oid,
    rel: pg_sys::Relation,
) -> IcebergResult<StorageContext> {
    let relation_needs_wal = unsafe { RelationHandle::from_raw(rel).needs_wal() };
    create_storage_context_with_wal(spc_oid, relation_needs_wal)
}
