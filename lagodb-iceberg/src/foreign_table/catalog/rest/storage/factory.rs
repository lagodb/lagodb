use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::Result;
use iceberg_lite::io::{Storage, StorageConfig, StorageFactory};
use lagodb_core::storage::profile::StorageProfiles;
use lagodb_core::storage::service::StorageEndpoint;
use lagodb_storage::StagingPathResolver;
use pgrx::pg_sys;

use super::routes::CatalogStorage;
use crate::foreign_table::options::ServerBindingKey;

/// Builds table-bound storage from REST response properties and credentials.
#[derive(Clone)]
pub(crate) struct PgStorageFactory {
    endpoint: StorageEndpoint,
    staging_resolver: StagingPathResolver,
    catalog_identity: ServerBindingKey,
    owner_fdw_oid: pg_sys::Oid,
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
        catalog_server_oid: pg_sys::Oid,
        owner_fdw_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Self {
        let staging_resolver = StagingPathResolver::new(endpoint.cache_dir());
        Self {
            endpoint,
            staging_resolver,
            catalog_identity: ServerBindingKey {
                server_oid: catalog_server_oid,
                effective_user,
            },
            owner_fdw_oid,
            backend_thread: PhantomData,
        }
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
        // A shared catalog must not share a prior table's local routing snapshot.
        // All routes in this table's storage retain this one lazy snapshot.
        let profiles = StorageProfiles::new(
            self.owner_fdw_oid,
            c"storage",
            self.catalog_identity.effective_user,
        );
        let storage = CatalogStorage::new(
            location,
            properties,
            credentials,
            &self.catalog_identity,
            &profiles,
            &self.endpoint,
            &self.staging_resolver,
        )?;
        Ok(Arc::new(storage))
    }
}
