use std::error::Error;
use std::sync::Arc;

use pg_lakebase_storage::{
    StorageError, StorageErrorKind, StorageResult, StoreConfig,
};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

use crate::diag::SqlStateError;
use crate::storage::service::{BackendStorageService, StorageEndpoint};

use super::cache::{ForeignStoreCache, ForeignStoreCacheEntry, initialize_callbacks};
use super::catalog::{ForeignCatalog, ForeignStoreOptions};
use super::handle::ForeignStoreHandle;

/// Provider-owned translation from borrowed server/mapping options to an owned
/// storage configuration.
///
/// The returned configuration is shared by the effective user mapping and must
/// therefore be identical for every table using that mapping. Table-specific
/// options are not accepted by this builder; the
/// provider keeps that schema in its own table state.
pub trait ForeignStoreConfigProvider {
    type Error: SqlStateError;

    fn build_store_config(
        options: ForeignStoreOptions<'_>,
    ) -> Result<StoreConfig, Self::Error>;
}

#[derive(Debug, Error)]
pub enum ForeignStoreAcquireError<E: Error + 'static> {
    #[error("foreign store provider configuration failed: {0}")]
    Provider(#[source] E),

    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl<E> SqlStateError for ForeignStoreAcquireError<E>
where
    E: SqlStateError,
{
    fn sql_error_code(&self) -> PgSqlErrorCode {
        match self {
            Self::Provider(error) => error.sql_error_code(),
            Self::Storage(error) => Self::storage_sql_error_code(error.kind()),
        }
    }
}

impl<E> ForeignStoreAcquireError<E>
where
    E: Error + 'static,
{
    fn storage_sql_error_code(kind: StorageErrorKind) -> PgSqlErrorCode {
        match kind {
            StorageErrorKind::InvalidPath | StorageErrorKind::Configuration => {
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
            }
            StorageErrorKind::NotFound => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            StorageErrorKind::Unsupported => {
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
            }
            StorageErrorKind::Busy => PgSqlErrorCode::ERRCODE_LOCK_NOT_AVAILABLE,
            StorageErrorKind::ResourceExhausted => {
                PgSqlErrorCode::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED
            }
            StorageErrorKind::Io
            | StorageErrorKind::Backend
            | StorageErrorKind::Cache
            | StorageErrorKind::CacheFillAborted
            | StorageErrorKind::Ambiguous => PgSqlErrorCode::ERRCODE_IO_ERROR,
            StorageErrorKind::Protocol
            | StorageErrorKind::ClosedHandle
            | StorageErrorKind::ExpiredCursor
            | StorageErrorKind::Conflict => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        }
    }
}

/// Backend-local owner of the shared storage-service endpoint.
#[derive(Clone, Debug)]
pub struct ForeignStoreManager {
    endpoint: StorageEndpoint,
}

impl ForeignStoreManager {
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
    ) -> Result<ForeignStoreHandle, ForeignStoreAcquireError<P::Error>>
    where
        P: ForeignStoreConfigProvider,
    {
        initialize_callbacks();

        let catalog = ForeignCatalog::load(relation_oid, effective_user);
        let identity = catalog.identity().clone();
        let umid = identity.umid();
        if let Some(entry) = ForeignStoreCache::with_current(|cache| {
            cache.find_matching(umid, &identity)
        }) {
            return Ok(ForeignStoreHandle::new(entry));
        }

        let config = Arc::new(
            P::build_store_config(catalog.options())
                .map_err(ForeignStoreAcquireError::Provider)?,
        );
        let service =
            BackendStorageService::for_foreign(&self.endpoint, umid, config)?;

        let entry = ForeignStoreCache::with_current(|cache| {
            cache.insert(ForeignStoreCacheEntry {
                umid,
                server_hashvalue: catalog.server_hashvalue(),
                mapping_hashvalue: catalog.mapping_hashvalue(),
                identity,
                service,
            })
        });
        Ok(ForeignStoreHandle::new(entry))
    }
}
