use std::any::Any;
use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use iceberg_lite::io::{
    FileMetadata, FileWrite, OpenedFile, Storage, StorageCredential,
};
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_core::storage::profile::{
    ObjectScheme, ObjectUri, ObjectUriPrefix, StorageProfiles,
};
use lagodb_core::storage::service::StorageEndpoint;
use lagodb_storage::{StagingPathResolver, StoreConfig};
use pgrx::pg_sys;

use super::binding::{RouteBinding, RouteContext};
use super::cache::{ConfiguredStorageCache, ConfiguredStorageRouteId};
use crate::error::storage_error_to_iceberg_lite;
use crate::foreign_table::catalog::CatalogAccessIdentity;
use crate::storage::ObjectBucket;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum ObjectProvider {
    S3,
    Gcs,
    Azure,
}

impl From<ObjectScheme> for ObjectProvider {
    fn from(scheme: ObjectScheme) -> Self {
        match scheme {
            ObjectScheme::S3 => Self::S3,
            ObjectScheme::Gcs => Self::Gcs,
            ObjectScheme::Azure => Self::Azure,
        }
    }
}

/// One configured bucket a REST credential route resolved to.
pub(super) struct CatalogRoute {
    bucket: ObjectBucket,
}

impl CatalogRoute {
    pub(super) fn new(
        prefix: Option<&ObjectUriPrefix>,
        profile_server_oid: Option<pg_sys::Oid>,
        object: &ObjectUri,
        store_config: StoreConfig,
        catalog_identity: &CatalogAccessIdentity,
        endpoint: &StorageEndpoint,
        staging_resolver: &StagingPathResolver,
    ) -> Result<Self> {
        let provider = ObjectProvider::from(object.scheme());
        let azure_account = match &store_config {
            StoreConfig::Azure(config) => config.account.as_deref(),
            _ => None,
        };
        if provider == ObjectProvider::Azure
            && let Some(uri_account) = object.account()
            && azure_account != Some(uri_account)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Azure account in object URI conflicts with storage configuration",
            ));
        }
        let route_id = ConfiguredStorageRouteId::new(
            catalog_identity,
            endpoint,
            provider,
            object.bucket(),
            azure_account,
            prefix.and_then(ObjectUriPrefix::key_prefix),
            &store_config,
        )
        .with_profile_server(profile_server_oid);
        let service =
            ConfiguredStorageCache::acquire(route_id, endpoint, store_config)
                .map_err(storage_error_to_iceberg_lite)?;
        Ok(Self {
            bucket: ObjectBucket::new(
                object.bucket(),
                service,
                staging_resolver.clone(),
            ),
        })
    }

    fn bucket(&self) -> &ObjectBucket {
        &self.bucket
    }
}

/// Storage router that binds only selected REST/local configurations and
/// retains each service for subsequent file operations.
pub(super) struct CatalogStorage {
    scheme: String,
    default_location: ObjectUriPrefix,
    default_binding: RouteBinding,
    credential_routes: Vec<RouteBinding>,
}

// SAFETY: `CatalogStorage` is the private REST-catalog adapter for
// iceberg-lite's `Storage: Send + Sync` trait. All routes contain naturally
// thread-bound PostgreSQL storage services and are constructed, invoked, and
// dropped only by the owning backend's serial execution lifecycle.
unsafe impl Send for CatalogStorage {}
// SAFETY: the extension does not access the route cache or its services from
// concurrent threads; shared access exists only for the upstream trait bound.
unsafe impl Sync for CatalogStorage {}

impl CatalogStorage {
    pub(super) fn new(
        location: String,
        properties: HashMap<String, String>,
        credentials: Vec<StorageCredential>,
        catalog_identity: &CatalogAccessIdentity,
        profiles: &StorageProfiles,
        endpoint: &StorageEndpoint,
        staging_resolver: &StagingPathResolver,
    ) -> Result<Self> {
        let scheme = location
            .split_once("://")
            .map(|(scheme, _)| scheme.to_ascii_lowercase())
            .ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "invalid object storage URI")
            })?;
        let context = Rc::new(RouteContext {
            catalog_identity: catalog_identity.clone(),
            profiles: profiles.clone(),
            endpoint: endpoint.clone(),
            staging_resolver: staging_resolver.clone(),
            properties,
        });
        let mut credential_routes = Vec::with_capacity(credentials.len());
        for credential in credentials {
            let (prefix, overrides) = credential.into_parts();
            let prefix = ObjectUriPrefix::parse(&prefix).map_err(object_uri_err)?;
            credential_routes.push(RouteBinding::scoped(
                prefix,
                overrides,
                Rc::clone(&context),
            ));
        }
        credential_routes.sort_by_key(|route| Reverse(route.specificity()));
        let default_location =
            ObjectUriPrefix::parse(&location).map_err(object_uri_err)?;
        let default_binding = RouteBinding::fallback(context);
        Ok(Self {
            scheme,
            default_location,
            default_binding,
            credential_routes,
        })
    }

    fn select_binding(&self, object: &ObjectUri) -> &RouteBinding {
        self.credential_routes
            .iter()
            .find(|binding| binding.matches(object))
            .unwrap_or(&self.default_binding)
    }

    fn resolve<'a>(&self, path: &'a str) -> Result<(Rc<CatalogRoute>, Cow<'a, str>)> {
        if !path.contains("://") {
            let key = path.trim_start_matches('/');
            if key.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "object key is empty",
                ));
            }
            let object = self.default_location.object(key);
            return Ok((self.default_binding.route(&object)?, Cow::Borrowed(key)));
        }
        let object = ObjectUri::parse(path).map_err(object_uri_err)?;
        let route = self.select_binding(&object).route(&object)?;
        let key = object.key().to_owned();
        Ok((route, Cow::Owned(key)))
    }
}

fn object_uri_err(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::DataInvalid, "invalid object storage URI")
        .with_source(error)
}

impl fmt::Debug for CatalogStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogStorage")
            .field("scheme", &self.scheme)
            .field("credential_route_count", &self.credential_routes.len())
            .finish()
    }
}

impl Storage for CatalogStorage {
    fn resolve_uri(&self, _uri: &str) -> Result<usize> {
        // Preserve the full URI for per-object REST credential routing. Every
        // file operation calls resolve() to parse, validate and route the path
        // before accessing object data. InputFile/OutputFile construction only
        // retains the path; errors are reported when it is used, avoiding
        // duplicate parsing and binding selection before the actual operation.
        Ok(0)
    }

    fn delete(&self, path: &str) -> Result<()> {
        let (route, key) = self.resolve(path)?;
        route.bucket().delete(&key)
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        let (route, key) = self.resolve(path)?;
        route.bucket().delete_prefix(&key)
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        let (route, key) = self.resolve(path)?;
        route.bucket().status(&key)
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let (route, key) = self.resolve(path)?;
        route.bucket().open_reader(&key)
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (route, key) = self.resolve(path)?;
        route.bucket().writer(&key)
    }

    fn delete_cleanup(&self, path: &str) -> Result<()> {
        let (route, key) = self.resolve(path)?;
        route.bucket().delete_cleanup(&key)
    }

    fn remove_dir_all_cleanup(&self, path: &str) -> Result<()> {
        let (route, key) = self.resolve(path)?;
        route.bucket().delete_prefix_cleanup(&key)
    }

    fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
        Ok(())
    }

    fn scheme(&self) -> &str {
        &self.scheme
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn finalize_write(&self, path: &str) -> Result<()> {
        let (route, key) = self.resolve(path)?;
        route.bucket().finalize_write(&key)
    }
}
