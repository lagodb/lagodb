use std::ffi::CStr;

use lagodb_storage::StoreConfig;
use pgrx::pg_sys;

use crate::storage::foreign::{
    ForeignCatalog, StorageAcquireError, StorageHandle, StorageManager,
};
use crate::storage::service::BackendStorageService;

use super::{
    ObjectUri, StorageProfileConfig, StorageProfileError, StorageScope,
    StorageServerCatalog, StorageServerPolicy,
};

/// One validated, catalog-cached foreign-server profile available to the
/// effective user.
#[derive(Clone, Debug)]
pub struct ScopedStorageProfile {
    server_name: Box<str>,
    scope_text: Box<str>,
    scope: StorageScope,
    storage: StorageHandle,
}

impl ScopedStorageProfile {
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    pub fn scope(&self) -> &str {
        &self.scope_text
    }

    pub fn config(&self) -> &StoreConfig {
        self.storage.config()
    }

    /// Returns the catalog-backed service shared with other users of this
    /// effective foreign-server/user-mapping profile.
    pub fn service(&self) -> BackendStorageService {
        self.storage.service()
    }

    fn contains(&self, object: &ObjectUri) -> bool {
        self.scope.contains(object)
    }
}

/// Cold-path snapshot of scoped storage profiles visible to one effective user.
/// Each route retains the same catalog-backed storage handle used by ordinary
/// foreign storage access.
#[derive(Clone, Debug, Default)]
pub struct StorageProfiles {
    routes: Vec<ScopedStorageProfile>,
}

impl StorageProfiles {
    /// Loads scoped profiles owned by one foreign-data wrapper and server type.
    /// Catalog access occurs only here; callers retain the resulting owned
    /// routes for operation-level lookup.
    pub fn load(
        manager: &StorageManager,
        owner_fdw_oid: pg_sys::Oid,
        server_type: &CStr,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, StorageProfileError> {
        let catalog = StorageServerCatalog::load(
            StorageServerPolicy::new(owner_fdw_oid, Some(server_type)),
            effective_user,
        )?;
        let mut routes = Vec::new();
        for route in catalog.scoped_routes() {
            let foreign_catalog =
                ForeignCatalog::load_server(route.oid(), effective_user);
            let storage = match manager
                .acquire_catalog::<StorageProfileConfig>(foreign_catalog)
            {
                Ok(storage) => storage,
                Err(StorageAcquireError::Provider(error)) => return Err(error),
                Err(StorageAcquireError::Storage(error)) => return Err(error.into()),
            };
            routes.push(ScopedStorageProfile {
                server_name: route.name().into(),
                scope_text: route
                    .scope_text()
                    .expect("scoped_routes only returns scoped servers")
                    .into(),
                scope: route
                    .scope()
                    .expect("scoped_routes only returns scoped servers")
                    .clone(),
                storage,
            });
        }
        routes.sort_by(|left, right| {
            right
                .scope
                .specificity()
                .cmp(&left.scope.specificity())
                .then_with(|| left.server_name.cmp(&right.server_name))
        });
        Ok(Self { routes })
    }

    /// Selects the longest matching explicit scope.
    pub fn resolve(
        &self,
        location: &str,
    ) -> Result<&ScopedStorageProfile, StorageProfileError> {
        self.resolve_optional(location)?.ok_or_else(|| {
            StorageProfileError::NoMatchingProfile {
                location: location.into(),
            }
        })
    }

    pub fn resolve_optional(
        &self,
        location: &str,
    ) -> Result<Option<&ScopedStorageProfile>, StorageProfileError> {
        let object = ObjectUri::parse(location)?;
        let mut matches = self.routes.iter().filter(|route| route.contains(&object));
        let Some(selected) = matches.next() else {
            return Ok(None);
        };
        if let Some(other) = matches.next()
            && other.scope.specificity() == selected.scope.specificity()
        {
            return Err(StorageProfileError::AmbiguousScope {
                server: selected.server_name.clone(),
                other: other.server_name.clone(),
                scope: selected.scope_text.clone(),
            });
        }
        Ok(Some(selected))
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}
