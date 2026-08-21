use std::error::Error;
use std::sync::Arc;

use pg_lakebase_storage::{
    StagingPathResolver, StorageError, StorageResult, StoreConfig,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::SqlStateError;
use crate::storage::service::{BackendStorageService, StorageEndpoint};

use super::access::{ObjectAccess, ObjectPrefixAccess};
use super::cache::{StorageCache, StorageCacheEntry, initialize_callbacks};
use super::catalog::{ForeignCatalog, StorageOptions};
use super::handle::StorageHandle;

/// Provider-owned translation from borrowed server/mapping options to an owned
/// storage configuration.
///
/// The returned configuration is shared by the effective user mapping and must
/// therefore be identical for every table using that mapping. Table-specific
/// options are not accepted by this builder; the
/// provider keeps that schema in its own table state.
pub trait StorageConfigProvider {
    type Error: SqlStateError;

    fn build_store_config(
        options: StorageOptions<'_>,
    ) -> Result<StoreConfig, Self::Error>;
}

#[derive(Debug, Error)]
pub enum StorageAcquireError<E: Error + 'static> {
    #[error("storage provider configuration failed: {0}")]
    Provider(#[source] E),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl<E> SqlStateError for StorageAcquireError<E>
where
    E: SqlStateError,
{
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Provider(error) => error.sql_error_code(),
            Self::Storage(error) => error.sql_error_code(),
        }
    }
}

/// Backend-local owner of the shared object-storage service endpoint.
#[derive(Clone, Debug)]
pub struct StorageManager {
    endpoint: StorageEndpoint,
}

impl StorageManager {
    pub fn new(endpoint: StorageEndpoint) -> Self {
        Self { endpoint }
    }

    pub fn from_pg_gucs() -> StorageResult<Self> {
        let endpoint = StorageEndpoint::from_pg_gucs()?.require_enabled()?;
        Ok(Self::from_endpoint(&endpoint))
    }

    pub fn from_endpoint(endpoint: &StorageEndpoint) -> Self {
        Self::new(endpoint.clone())
    }

    /// Loads one foreign catalog view and acquires the corresponding cached
    /// store. This is intended for FDW begin callbacks, not row callbacks.
    pub fn acquire<P>(
        &self,
        relation_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<StorageHandle, StorageAcquireError<P::Error>>
    where
        P: StorageConfigProvider,
    {
        self.acquire_catalog::<P>(ForeignCatalog::load(relation_oid, effective_user))
    }

    /// Acquires a configured store from an explicit foreign server and the
    /// effective user mapping. This is the operation-level entry point used
    /// by direct COPY object URIs; it does not fabricate a foreign relation in
    /// order to reuse the relation-based FDW path.
    pub fn acquire_server<P>(
        &self,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<StorageHandle, StorageAcquireError<P::Error>>
    where
        P: StorageConfigProvider,
    {
        self.acquire_catalog::<P>(ForeignCatalog::load_server(
            server_oid,
            effective_user,
        ))
    }

    /// Acquires the same cached storage context as [`Self::acquire_server`] and
    /// binds the caller-owned staging path used by COPY and FDW writes.
    pub fn acquire_object_access<P>(
        &self,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectAccess, StorageAcquireError<P::Error>>
    where
        P: StorageConfigProvider,
    {
        let store = self.acquire_server::<P>(server_oid, effective_user)?;
        let staging = StagingPathResolver::new(self.endpoint.cache_dir().to_owned());
        Ok(ObjectAccess::new(store, staging, bucket, key))
    }

    /// Acquires the configured store and binds it to one caller-authorized key
    /// prefix. Child exact-object capabilities remain constrained to that
    /// prefix.
    pub fn acquire_prefix_access<P>(
        &self,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        bucket: &str,
        prefix: &str,
    ) -> Result<ObjectPrefixAccess, StorageAcquireError<P::Error>>
    where
        P: StorageConfigProvider,
    {
        let store = self.acquire_server::<P>(server_oid, effective_user)?;
        let staging = StagingPathResolver::new(self.endpoint.cache_dir().to_owned());
        Ok(ObjectPrefixAccess::new(store, staging, bucket, prefix))
    }

    pub(crate) fn acquire_catalog<P>(
        &self,
        catalog: ForeignCatalog,
    ) -> Result<StorageHandle, StorageAcquireError<P::Error>>
    where
        P: StorageConfigProvider,
    {
        initialize_callbacks();

        let identity = catalog.identity().clone();
        let umid = identity.umid();
        if let Some(entry) =
            StorageCache::with_current(|cache| cache.find_matching(umid, &identity))
        {
            return Ok(StorageHandle::new(entry));
        }

        let config = Arc::new(
            P::build_store_config(catalog.options())
                .map_err(StorageAcquireError::Provider)?,
        );
        let service = BackendStorageService::for_foreign(
            &self.endpoint,
            umid,
            Arc::clone(&config),
        )?;

        let entry = StorageCache::with_current(|cache| {
            cache.insert(StorageCacheEntry {
                umid,
                server_hashvalue: catalog.server_hashvalue(),
                mapping_hashvalue: catalog.mapping_hashvalue(),
                identity,
                config,
                service,
            })
        });
        Ok(StorageHandle::new(entry))
    }
}
