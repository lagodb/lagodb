//! AM-owned tablespace and WAL policy for Iceberg FileIO.

use std::ffi::CStr;
use std::sync::Arc;

use iceberg_lite::io::FileIO;
use pg_lakebase_core::options::get_tablespace;
use pg_lakebase_core::storage::service::{BackendStorageService, StorageEndpoint};
use pg_lakebase_storage::StagingPathResolver;
use pgrx::pg_sys;

use crate::error::IcebergResult;
use crate::storage::{LocalStorage, ObjectStorage};

/// Storage context for an AM relation and its PostgreSQL tablespace policy.
///
/// Distributed/object storage never uses Iceberg file WAL. Local storage uses
/// the owning relation's `RelationNeedsWAL` result on write and lifecycle paths.
pub(crate) struct StorageContext {
    file_io: FileIO,
    base_path: String,
    is_distributed: bool,
}

impl StorageContext {
    /// Resolve a tablespace for a read-only or generic operation.
    pub(crate) fn for_tablespace(spc_oid: pg_sys::Oid) -> IcebergResult<Self> {
        Self::for_tablespace_with_wal(spc_oid, false)
    }

    /// Resolve a tablespace with the owning relation's WAL policy.
    pub(crate) fn for_tablespace_with_wal(
        spc_oid: pg_sys::Oid,
        relation_needs_wal: bool,
    ) -> IcebergResult<Self> {
        let Some(opts) = get_tablespace(spc_oid)? else {
            return Self::local(relation_needs_wal);
        };

        let endpoint = StorageEndpoint::from_pg_gucs()?.require_enabled()?;
        let service =
            BackendStorageService::for_managed(&endpoint, opts.volume_id().get())?;
        let resolver = StagingPathResolver::new(endpoint.cache_dir());
        Self::distributed(&opts, service, resolver)
    }

    pub(crate) fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    pub(crate) fn into_file_io(self) -> FileIO {
        self.file_io
    }

    pub(crate) fn base_path(&self) -> &str {
        &self.base_path
    }

    pub(crate) fn is_distributed(&self) -> bool {
        self.is_distributed
    }

    fn distributed(
        opts: &pg_lakebase_core::options::CachedTablespaceOpts,
        storage_service: BackendStorageService,
        staging_resolver: StagingPathResolver,
    ) -> IcebergResult<Self> {
        let base_path = opts.effective_base_uri().to_owned();
        let storage = ObjectStorage::new(
            base_path.clone(),
            opts.volume_id(),
            opts.object_namespace(),
            storage_service,
            staging_resolver,
        );

        Ok(Self {
            file_io: FileIO::new(Arc::new(storage)),
            base_path,
            is_distributed: true,
        })
    }

    fn local(relation_needs_wal: bool) -> IcebergResult<Self> {
        let data_dir = unsafe {
            CStr::from_ptr(pg_sys::DataDir)
                .to_string_lossy()
                .to_string()
        };

        Ok(Self {
            file_io: FileIO::new(Arc::new(LocalStorage::with_wal(
                relation_needs_wal,
            ))),
            base_path: data_dir,
            is_distributed: false,
        })
    }
}
