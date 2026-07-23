pub mod local;
pub mod object;
mod object_uri;
mod post_commit_delete;
pub(crate) mod transaction_resources;
mod wait_event;

pub use local::LocalStorage;
pub use object::ObjectStorage;
pub(crate) use post_commit_delete::{
    PostCommitDeletePurpose, PostCommitFileDeleteBatch,
};

use crate::error::IcebergResult;
use iceberg_lite::io::FileIO;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::options::get_tablespace;
use pg_lakebase_core::storage_service::{BackendStorageService, StorageEndpoint};
use pg_lakebase_storage::StagingPathResolver;
use pgrx::pg_sys;
use std::ffi::CStr;
use std::sync::Arc;

/// Storage context containing FileIO and base path information.
///
/// This struct encapsulates all the information needed to interact with
/// Iceberg table storage, whether local or distributed (S3, Azure, etc.).
///
/// # WAL invariants
///
/// - Distributed/object storage always has `needs_wal = false`; durability is
///   owned by the object store and orphan cleanup is separate from PostgreSQL
///   redo.
/// - Local storage defaults to `needs_wal = false`. Reads, scans, and generic
///   cleanup paths should not emit Iceberg WAL. Writes and post-commit cleanup
///   tied to a WAL-logged PostgreSQL relation must opt in via
///   [`StorageContext::for_tablespace_with_wal`] using `RelationNeedsWAL`.
/// - Local Iceberg file WAL is for best-effort, lossy reconstruction during
///   standby WAL replay or archive recovery. Local crash recovery relies on
///   explicit writer close performing `FileSync`, so crash-only redo skips
///   `WRITE_FILE` records.
pub struct StorageContext {
    /// The FileIO instance for reading/writing files
    file_io: FileIO,
    /// Base path for table storage (DataDir for local, base URL for distributed)
    base_path: String,
    /// Whether this is a distributed storage (S3, Azure, etc.)
    is_distributed: bool,
    /// Whether Iceberg file WAL is needed for this storage context.
    needs_wal: bool,
}

impl StorageContext {
    /// Create a storage context for a tablespace.
    ///
    /// Distributed tablespaces use object storage and always disable Iceberg
    /// file WAL. Native PostgreSQL tablespaces use local VFD-backed storage
    /// with WAL disabled by default; relation-owned write and lifecycle paths
    /// should use [`Self::for_tablespace_with_wal`].
    pub fn for_tablespace(spc_oid: pg_sys::Oid) -> IcebergResult<Self> {
        Self::for_tablespace_with_wal(spc_oid, false)
    }

    /// Create a storage context with relation-aware Iceberg file WAL policy.
    ///
    /// WAL is only enabled for local storage when the caller indicates that
    /// the owning relation needs WAL, typically from PostgreSQL's
    /// `RelationNeedsWAL`. Distributed/object storage ignores this flag and
    /// keeps `needs_wal = false`.
    pub fn for_tablespace_with_wal(
        spc_oid: pg_sys::Oid,
        relation_needs_wal: bool,
    ) -> IcebergResult<Self> {
        let Some(opts) = get_tablespace(spc_oid)? else {
            return Self::local(relation_needs_wal);
        };

        let endpoint = StorageEndpoint::from_pg_gucs()?.require_enabled()?;
        let service = BackendStorageService::from_endpoint(&endpoint);
        let resolver = StagingPathResolver::new(endpoint.cache_dir());
        Self::distributed(&opts, service, resolver)
    }

    /// Create a storage context for a specific relation.
    ///
    /// This combines tablespace lookup with `RelationNeedsWAL`. Local storage
    /// uses the relation policy to decide whether to emit Iceberg file WAL for
    /// standby WAL replay or archive recovery; distributed/object storage keeps
    /// WAL disabled.
    ///
    /// # Safety
    /// The caller must ensure the relation pointer is valid.
    pub unsafe fn for_relation(
        spc_oid: pg_sys::Oid,
        rel: pg_sys::Relation,
    ) -> IcebergResult<Self> {
        let relation_needs_wal = unsafe { RelationHandle::from_raw(rel).needs_wal() };
        Self::for_tablespace_with_wal(spc_oid, relation_needs_wal)
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    pub fn into_file_io(self) -> FileIO {
        self.file_io
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    pub fn is_distributed(&self) -> bool {
        self.is_distributed
    }

    pub fn needs_wal(&self) -> bool {
        self.needs_wal
    }

    fn distributed(
        opts: &pg_lakebase_core::options::CachedTablespaceOpts,
        storage_service: BackendStorageService,
        staging_resolver: StagingPathResolver,
    ) -> IcebergResult<Self> {
        let base_path = opts.effective_base_uri().to_owned();
        let storage = ObjectStorage::new(
            base_path.clone(),
            opts.store_id().clone(),
            opts.object_namespace(),
            storage_service,
            staging_resolver,
        );

        Ok(Self {
            file_io: FileIO::new(Arc::new(storage)),
            base_path,
            is_distributed: true,
            needs_wal: false,
        })
    }

    fn local(relation_needs_wal: bool) -> IcebergResult<Self> {
        // Local storage is the only backend that can emit Iceberg file WAL.
        // The caller owns the relation-aware decision; the default helper
        // passes false, and mutation/write paths pass RelationNeedsWAL.
        let needs_wal = relation_needs_wal;

        let data_dir = unsafe {
            CStr::from_ptr(pg_sys::DataDir)
                .to_string_lossy()
                .to_string()
        };

        Ok(Self {
            file_io: FileIO::new(Arc::new(LocalStorage::with_wal(needs_wal))),
            base_path: data_dir,
            is_distributed: false,
            needs_wal,
        })
    }
}
