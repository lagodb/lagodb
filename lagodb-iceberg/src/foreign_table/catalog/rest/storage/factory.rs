use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::Result;
use iceberg_lite::io::{Storage, StorageConfig, StorageFactory};
use lagodb_core::storage::foreign::StorageManager;
use lagodb_core::storage::profile::{StorageProfileError, StorageProfiles};
use lagodb_core::storage::service::StorageEndpoint;
use lagodb_storage::StagingPathResolver;
use pgrx::pg_sys;

use super::cache::CatalogStorageIdentity;
use super::routes::CatalogStorage;

/// Builds table-bound storage from REST response properties and credentials.
#[derive(Clone)]
pub(crate) struct PgStorageFactory {
    endpoint: StorageEndpoint,
    staging_resolver: StagingPathResolver,
    catalog_identity: CatalogStorageIdentity,
    profiles: StorageProfiles,
    backend_thread: PhantomData<Rc<()>>,
}

// SAFETY: this private host adapter satisfies the upstream `StorageFactory:
// Send + Sync` bound. The PostgreSQL-owned catalog lifecycle constructs,
// clones, invokes, and drops it on one backend thread and never invokes it
// concurrently. The marker prevents this adaptation from becoming an
// accidental compiler-derived property.
unsafe impl Send for PgStorageFactory {}
unsafe impl Sync for PgStorageFactory {}

impl PgStorageFactory {
    pub(crate) fn new(
        endpoint: StorageEndpoint,
        catalog_name: Arc<str>,
        catalog_server_oid: pg_sys::Oid,
        owner_fdw_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> std::result::Result<Self, StorageProfileError> {
        let staging_resolver = StagingPathResolver::new(endpoint.cache_dir());
        let manager = StorageManager::from_endpoint(&endpoint);
        let profiles = StorageProfiles::load(
            &manager,
            owner_fdw_oid,
            c"storage",
            effective_user,
        )?;
        Ok(Self {
            endpoint,
            staging_resolver,
            catalog_identity: CatalogStorageIdentity::new(
                catalog_server_oid,
                effective_user,
                catalog_name,
            ),
            profiles,
            backend_thread: PhantomData,
        })
    }
}

impl fmt::Debug for PgStorageFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PgStorageFactory")
            .finish_non_exhaustive()
    }
}

impl StorageFactory for PgStorageFactory {
    fn build(&self, config: StorageConfig) -> Result<Arc<dyn Storage>> {
        let (location, properties, credentials) = config.into_parts();
        let storage = CatalogStorage::new(
            location,
            properties,
            credentials,
            &self.catalog_identity,
            &self.profiles,
            &self.endpoint,
            &self.staging_resolver,
        )?;
        Ok(Arc::new(storage))
    }
}
