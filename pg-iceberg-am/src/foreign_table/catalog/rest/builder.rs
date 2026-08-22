use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::catalog::rest::auth::AuthManager;
use iceberg_lite::catalog::rest::{RestCatalog, RestCatalogBuilder};
use iceberg_lite::catalog::{CatalogBuilder, SessionContext};
use iceberg_lite::encryption::kms::KmsClientFactory;
use iceberg_lite::{Error, ErrorKind, Result};
use pg_lakebase_core::storage::service::StorageEndpoint;
use pgrx::pg_sys;

use super::http::PgRestHttpTransport;
use super::storage::PgStorageFactory;

/// Composes the generic REST catalog with PostgreSQL network and storage adapters.
#[derive(Debug, Default)]
pub struct PgRestCatalogBuilder {
    session_context: Option<SessionContext>,
    auth_manager: Option<Arc<dyn AuthManager>>,
    kms_client_factory: Option<Arc<dyn KmsClientFactory>>,
}

impl PgRestCatalogBuilder {
    pub fn with_session_context(mut self, context: SessionContext) -> Self {
        self.session_context = Some(context);
        self
    }

    pub fn with_auth_manager(mut self, auth_manager: Arc<dyn AuthManager>) -> Self {
        self.auth_manager = Some(auth_manager);
        self
    }

    pub fn with_kms_client_factory(
        mut self,
        kms_client_factory: Arc<dyn KmsClientFactory>,
    ) -> Self {
        self.kms_client_factory = Some(kms_client_factory);
        self
    }

    pub fn load(
        self,
        name: impl Into<String>,
        properties: HashMap<String, String>,
        catalog_server_oid: pg_sys::Oid,
        owner_fdw_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<RestCatalog> {
        let name = name.into();
        let catalog_name: Arc<str> = Arc::from(name.as_str());
        let endpoint = StorageEndpoint::from_pg_gucs()
            .and_then(StorageEndpoint::require_enabled)
            .map_err(|error| {
                Error::new(
                    ErrorKind::IoError,
                    "REST catalog storage service is unavailable",
                )
                .with_source(error)
            })?;
        let storage_factory = PgStorageFactory::new(
            endpoint,
            catalog_name,
            catalog_server_oid,
            owner_fdw_oid,
            effective_user,
        )
        .map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "failed to load PostgreSQL storage profiles",
            )
            .with_source(error)
        })?;
        let mut builder = RestCatalogBuilder::default()
            .with_http_transport(Arc::new(PgRestHttpTransport::new()?))
            .with_storage_factory(Arc::new(storage_factory));
        if let Some(context) = self.session_context {
            builder = builder.with_session_context(context);
        }
        if let Some(auth_manager) = self.auth_manager {
            builder = builder.with_auth_manager(auth_manager);
        }
        if let Some(kms_client_factory) = self.kms_client_factory {
            builder = builder.with_kms_client_factory(kms_client_factory);
        }
        builder.load(name, properties)
    }
}
