use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use iceberg_lite::io::{
    FileMetadata, FileWrite, OpenedFile, Storage, StorageCredential,
};
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_core::storage::profile::{
    ObjectScheme, ObjectUri, ObjectUriPrefix, ScopedStorageProfile, StorageProfiles,
};
use lagodb_core::storage::service::{BackendStorageService, StorageEndpoint};
use lagodb_storage::{StagingPathResolver, StorageErrorKind, StoreConfig};

use super::cache::{
    CatalogStorageIdentity, ConfiguredStorageCache, ConfiguredStorageRouteId,
};
use super::config::ProviderConfig;
use crate::storage::transaction_resources::{
    ensure_object_file_staged, mark_object_file_uploaded, register_object_file_staged,
};
use crate::storage::{
    ObjectReader, ObjectWriter, StorageWaitEvent, StorageWaitGuard, storage_err,
};

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

struct CatalogRoute {
    prefix: Option<ObjectUriPrefix>,
    provider: ObjectProvider,
    bucket: Arc<str>,
    azure_account: Option<Arc<str>>,
    service: BackendStorageService,
    staging_resolver: StagingPathResolver,
}

impl CatalogRoute {
    fn new(
        prefix: Option<String>,
        location: &str,
        config: ProviderConfig<'_>,
        profile: Option<&ScopedStorageProfile>,
        catalog_identity: &CatalogStorageIdentity,
        endpoint: &StorageEndpoint,
        staging_resolver: &StagingPathResolver,
    ) -> Result<Self> {
        let location = ObjectUriPrefix::parse(location).map_err(object_uri_err)?;
        let prefix = prefix
            .map(|prefix| ObjectUriPrefix::parse(&prefix))
            .transpose()
            .map_err(object_uri_err)?;
        let provider = ObjectProvider::from(location.scheme());
        let store_config = config.resolve(provider, location.account())?;
        let azure_account = match store_config.as_ref() {
            StoreConfig::Azure(config) => config.account.as_deref().map(Arc::from),
            _ => None,
        };
        if provider == ObjectProvider::Azure
            && let Some(uri_account) = location.account()
            && azure_account.as_deref() != Some(uri_account)
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Azure account in object URI conflicts with storage configuration",
            ));
        }
        let service = match store_config {
            Cow::Borrowed(_) => profile
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "borrowed storage configuration has no PostgreSQL profile",
                    )
                })?
                .service(),
            Cow::Owned(store_config) => {
                let route_id = ConfiguredStorageRouteId::new(
                    catalog_identity,
                    endpoint,
                    provider,
                    location.bucket(),
                    azure_account.as_deref(),
                    prefix.as_ref().and_then(ObjectUriPrefix::key_prefix),
                    &store_config,
                );
                ConfiguredStorageCache::acquire(route_id, endpoint, store_config)
                    .map_err(storage_err)?
            }
        };
        Ok(Self {
            prefix,
            provider,
            bucket: Arc::from(location.bucket()),
            azure_account,
            service,
            staging_resolver: staging_resolver.clone(),
        })
    }

    fn matches(&self, uri: &ObjectUri) -> bool {
        self.prefix
            .as_ref()
            .is_some_and(|prefix| prefix.contains(uri))
    }

    fn key_for_object<'a>(&self, object: &'a ObjectUri) -> Result<&'a str> {
        let account_matches = self.provider != ObjectProvider::Azure
            || object
                .account()
                .is_none_or(|account| self.azure_account.as_deref() == Some(account));
        if ObjectProvider::from(object.scheme()) != self.provider
            || object.bucket() != self.bucket.as_ref()
            || !account_matches
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "object URI does not belong to the selected credential route",
            ));
        }
        Ok(object.key())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.service
            .delete(self.bucket.as_ref(), key)
            .map_err(storage_err)
    }

    fn delete_prefix(&self, key: &str) -> Result<()> {
        self.service
            .delete_prefix(self.bucket.as_ref(), key)
            .map(|_| ())
            .map_err(storage_err)
    }

    fn status(&self, key: &str) -> Result<Option<FileMetadata>> {
        match self.service.head(self.bucket.as_ref(), key) {
            Ok(info) => Ok(Some(FileMetadata { size: info.size })),
            Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(None),
            Err(error) => Err(storage_err(error)),
        }
    }

    fn open_reader(&self, key: &str) -> Result<OpenedFile> {
        let file = self
            .service
            .open(self.bucket.as_ref(), key)
            .map_err(storage_err)?;
        let metadata = FileMetadata { size: file.size() };
        Ok(OpenedFile {
            metadata,
            reader: Box::new(ObjectReader::new(
                self.service.clone(),
                Arc::clone(&self.bucket),
                Arc::from(key),
                file,
            )),
        })
    }

    fn writer(&self, key: &str) -> Result<Box<dyn FileWrite>> {
        let staging = self
            .service
            .create_staging_file(&self.staging_resolver, self.bucket.as_ref(), key)
            .map_err(storage_err)?;
        let location = self
            .service
            .object_location(self.bucket.as_ref(), key)
            .map_err(storage_err)?;
        register_object_file_staged(
            location,
            staging.path().to_path_buf(),
            self.service.clone(),
        );
        Ok(Box::new(ObjectWriter::new(staging)))
    }

    fn finalize_write(&self, key: &str) -> Result<()> {
        let location = self
            .service
            .object_location(self.bucket.as_ref(), key)
            .map_err(storage_err)?;
        ensure_object_file_staged(&location)
            .map_err(|message| Error::new(ErrorKind::Unexpected, message))?;
        {
            let _wait = StorageWaitGuard::start(StorageWaitEvent::ObjectUpload);
            self.service
                .upload(self.bucket.as_ref(), key)
                .map_err(storage_err)?;
        }
        mark_object_file_uploaded(&location)
            .map_err(|message| Error::new(ErrorKind::Unexpected, message))
    }
}

/// Storage router that binds stable PostgreSQL profiles or one response's
/// configured credential contexts before object operations begin.
pub(super) struct CatalogStorage {
    scheme: String,
    default_route: CatalogRoute,
    credential_routes: Vec<CatalogRoute>,
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
        catalog_identity: &CatalogStorageIdentity,
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
        let mut credential_routes = Vec::with_capacity(credentials.len());
        for credential in credentials {
            let (prefix, credential_properties) = credential.into_parts();
            let profile = Self::profile_for(profiles, &prefix)?;
            credential_routes.push(CatalogRoute::new(
                Some(prefix.clone()),
                &prefix,
                ProviderConfig::with_profile_overrides(
                    &properties,
                    profile.map(ScopedStorageProfile::config),
                    &credential_properties,
                ),
                profile,
                catalog_identity,
                endpoint,
                staging_resolver,
            )?);
        }
        credential_routes.sort_by(|left, right| {
            right
                .prefix
                .as_ref()
                .map_or(0, ObjectUriPrefix::specificity)
                .cmp(&left.prefix.as_ref().map_or(0, ObjectUriPrefix::specificity))
        });
        let profile = Self::profile_for(profiles, &location)?;
        let default_route = CatalogRoute::new(
            None,
            &location,
            ProviderConfig::with_profile(
                &properties,
                profile.map(ScopedStorageProfile::config),
            ),
            profile,
            catalog_identity,
            endpoint,
            staging_resolver,
        )?;
        Ok(Self {
            scheme,
            default_route,
            credential_routes,
        })
    }

    fn profile_for<'a>(
        profiles: &'a StorageProfiles,
        location: &str,
    ) -> Result<Option<&'a ScopedStorageProfile>> {
        profiles.resolve_optional(location).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "failed to resolve PostgreSQL storage profile",
            )
            .with_source(error)
        })
    }

    fn route(&self, uri: &ObjectUri) -> &CatalogRoute {
        self.credential_routes
            .iter()
            .find(|route| route.matches(uri))
            .unwrap_or(&self.default_route)
    }

    fn resolve<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<(&'a CatalogRoute, Cow<'a, str>)> {
        if !path.contains("://") {
            let key = path.trim_start_matches('/');
            if key.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "object key is empty",
                ));
            }
            return Ok((&self.default_route, Cow::Borrowed(key)));
        }
        let object = ObjectUri::parse(path).map_err(object_uri_err)?;
        let route = self.route(&object);
        let key = route.key_for_object(&object)?.to_owned();
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
    fn resolve_uri(&self, uri: &str) -> Result<usize> {
        self.resolve(uri)?;
        Ok(0)
    }

    fn delete(&self, path: &str) -> Result<()> {
        let (route, key) = self.resolve(path)?;
        route.delete(&key)
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        let (route, key) = self.resolve(path)?;
        route.delete_prefix(&key)
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        let (route, key) = self.resolve(path)?;
        route.status(&key)
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let (route, key) = self.resolve(path)?;
        route.open_reader(&key)
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (route, key) = self.resolve(path)?;
        route.writer(&key)
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
        route.finalize_write(&key)
    }
}
