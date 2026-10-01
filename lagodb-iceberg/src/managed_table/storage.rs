//! AM-owned tablespace and WAL policy for Iceberg FileIO.

use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::io::FileIO;
use lagodb_core::handles::RelationHandle;
use lagodb_core::options::{CachedTablespaceOpts, get_tablespace};
use lagodb_core::storage::service::{BackendStorageService, StorageEndpoint};
use lagodb_storage::StagingPathResolver;

use crate::error::{IcebergError, IcebergResult};
use crate::storage::{LocalStorage, ObjectStorage};

/// Storage context for an AM relation and its PostgreSQL tablespace policy.
///
/// Distributed/object storage never uses Iceberg file WAL. Local storage uses
/// the owning relation's `RelationNeedsWAL` result on write and lifecycle paths.
///
/// # UNLOGGED contract
///
/// `CREATE UNLOGGED TABLE` disables local Iceberg file WAL through PostgreSQL's
/// `RelationNeedsWAL`, including writes, partitioned-table reservations,
/// TRUNCATE and post-commit deletion. File sync, transaction-local
/// overlays, catalog publication and commit/abort cleanup keep their ordinary
/// behavior. The shared `iceberg.iceberg_metadata` table remains logged, so
/// UNLOGGED does not mean that the whole operation produces no PostgreSQL WAL.
///
/// This intentionally differs from PostgreSQL heap UNLOGGED semantics. PostgreSQL
/// `ResetUnloggedRelations` identifies storage by native init forks and copies
/// them to main forks; it neither calls the table AM nor reads our catalog.
/// Iceberg creates no init fork or auxiliary reset table. Its directories and
/// logged metadata pointer are not automatically reset to an empty table after
/// a crash. Recovery does not deliberately invalidate the table either:
/// synced local files can survive, but post-recovery availability is not part
/// of this contract. Missing files cannot be reconstructed from file WAL on a
/// standby or during archive recovery because no such WAL was emitted.
///
/// Object-backed tables accept the same syntax and retain their existing
/// storage policy, which already emits no Iceberg file WAL. PostgreSQL still
/// applies its own persistence rules, including restrictions during recovery
/// and persistence of implicit sequences. Changing an existing Iceberg table
/// with `ALTER TABLE SET LOGGED/UNLOGGED` remains unsupported: PostgreSQL uses
/// a storage rewrite for that command rather than a simple WAL-policy toggle.
pub(crate) struct StorageContext {
    file_io: FileIO,
    backend: ManagedStorageBackend,
    backend_thread: PhantomData<Rc<()>>,
}

enum ManagedStorageBackend {
    Local(Arc<LocalStorage>),
    Object {
        storage: Arc<ObjectStorage>,
        tablespace: Rc<CachedTablespaceOpts>,
    },
}

impl StorageContext {
    /// Resolve storage placement for a read-only operation.
    pub(crate) fn for_read(rel: &RelationHandle<'_>) -> IcebergResult<Self> {
        Self::for_relation(rel, false)
    }

    /// Resolve storage placement and WAL policy for a write operation.
    pub(crate) fn for_write(rel: &RelationHandle<'_>) -> IcebergResult<Self> {
        Self::for_relation(rel, rel.needs_wal())
    }

    fn for_relation(
        rel: &RelationHandle<'_>,
        relation_needs_wal: bool,
    ) -> IcebergResult<Self> {
        let Some(opts) = get_tablespace(rel.tablespace().resolved_oid())? else {
            return Self::local(relation_needs_wal);
        };

        let endpoint = StorageEndpoint::from_pg_gucs()?.require_enabled()?;
        let service =
            BackendStorageService::for_managed(&endpoint, opts.volume_id().get())?;
        let resolver = StagingPathResolver::new(endpoint.cache_dir());
        Self::object_backed(opts, service, resolver)
    }

    pub(crate) fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    pub(crate) fn into_file_io(self) -> FileIO {
        self.file_io
    }

    pub(crate) fn object_tablespace(&self) -> Option<&CachedTablespaceOpts> {
        match &self.backend {
            ManagedStorageBackend::Local(_) => None,
            ManagedStorageBackend::Object { tablespace, .. } => Some(tablespace),
        }
    }

    pub(crate) fn local_storage(&self) -> Option<&LocalStorage> {
        match &self.backend {
            ManagedStorageBackend::Local(storage) => Some(storage),
            ManagedStorageBackend::Object { .. } => None,
        }
    }

    /// Reject reuse of an existing managed-table root before bootstrap.
    pub(crate) fn ensure_location_is_empty(
        &self,
        location: &str,
    ) -> IcebergResult<()> {
        let empty = match &self.backend {
            ManagedStorageBackend::Local(storage) => {
                storage.location_is_empty(location)?
            }
            ManagedStorageBackend::Object { storage, .. } => {
                storage.location_is_empty(location)?
            }
        };

        if empty {
            Ok(())
        } else {
            Err(IcebergError::ManagedTableLocationNotEmpty {
                location: location.to_owned(),
            })
        }
    }

    fn object_backed(
        opts: Rc<CachedTablespaceOpts>,
        storage_service: BackendStorageService,
        staging_resolver: StagingPathResolver,
    ) -> IcebergResult<Self> {
        let storage = Arc::new(ObjectStorage::new(
            opts.effective_base_uri(),
            opts.volume_id(),
            opts.object_namespace(),
            storage_service,
            staging_resolver,
        ));

        Ok(Self {
            file_io: FileIO::new(storage.clone()),
            backend: ManagedStorageBackend::Object {
                storage,
                tablespace: opts,
            },
            backend_thread: PhantomData,
        })
    }

    fn local(relation_needs_wal: bool) -> IcebergResult<Self> {
        let storage = Arc::new(LocalStorage::with_wal(relation_needs_wal));
        Ok(Self {
            file_io: FileIO::new(storage.clone()),
            backend: ManagedStorageBackend::Local(storage),
            backend_thread: PhantomData,
        })
    }
}
