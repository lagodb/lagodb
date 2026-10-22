//! Deferred materialization of storage routes from REST credential bindings.

mod route_cache;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_core::storage::profile::{
    ObjectUri, ObjectUriPrefix, SelectedStorageProfile, StorageProfiles,
};
use lagodb_core::storage::service::StorageEndpoint;
use lagodb_storage::{StagingPathResolver, StoreConfig};

use self::route_cache::{FallbackRoutes, ScopedRoutes};
use super::config::ProviderConfig;
use super::routes::{CatalogRoute, ObjectProvider};
use crate::foreign_table::catalog::CatalogAccessIdentity;

pub(super) struct RouteContext {
    pub catalog_identity: CatalogAccessIdentity,
    pub profiles: StorageProfiles,
    pub endpoint: StorageEndpoint,
    pub staging_resolver: StagingPathResolver,
    pub properties: HashMap<String, String>,
}

/// Retains one REST credential binding and lazily materializes only the
/// storage routes selected by file operations.
pub(super) struct RouteBinding {
    kind: BindingKind,
    context: Rc<RouteContext>,
}

enum BindingKind {
    Scoped(ScopedBinding),
    Fallback(RefCell<FallbackRoutes>),
}

struct ScopedBinding {
    prefix: ObjectUriPrefix,
    overrides: HashMap<String, String>,
    has_response_identity_source: bool,
    routes: ScopedRoutes,
}

impl RouteBinding {
    pub fn scoped(
        prefix: ObjectUriPrefix,
        overrides: HashMap<String, String>,
        context: Rc<RouteContext>,
    ) -> Self {
        let response = ProviderConfig::with_profile_overrides(
            &context.properties,
            None,
            &overrides,
        );
        let provider = ObjectProvider::from(prefix.scheme());
        let has_response_identity_source = response.has_response_identity(provider);
        let needs_local_config = !has_response_identity_source
            || !response.has_connection(provider, prefix.account());
        let routes = ScopedRoutes::new(needs_local_config);
        Self {
            kind: BindingKind::Scoped(ScopedBinding {
                prefix,
                overrides,
                has_response_identity_source,
                routes,
            }),
            context,
        }
    }

    pub fn fallback(context: Rc<RouteContext>) -> Self {
        Self {
            kind: BindingKind::Fallback(RefCell::new(FallbackRoutes::default())),
            context,
        }
    }

    pub fn matches(&self, object: &ObjectUri) -> bool {
        match &self.kind {
            BindingKind::Scoped(binding) => binding.prefix.contains(object),
            BindingKind::Fallback(_) => false,
        }
    }

    pub fn specificity(&self) -> usize {
        match &self.kind {
            BindingKind::Scoped(binding) => binding.prefix.specificity(),
            BindingKind::Fallback(_) => 0,
        }
    }

    /// Acquires the route selected by this REST credential binding and object.
    pub fn route(&self, object: &ObjectUri) -> Result<Rc<CatalogRoute>> {
        match &self.kind {
            BindingKind::Scoped(binding) => self.scoped_route(binding, object),
            BindingKind::Fallback(routes) => self.fallback_route(routes, object),
        }
    }

    fn scoped_route(
        &self,
        binding: &ScopedBinding,
        object: &ObjectUri,
    ) -> Result<Rc<CatalogRoute>> {
        match &binding.routes {
            ScopedRoutes::ResponseOnly(route) => {
                if let Some(route) = route.borrow().as_ref() {
                    return Ok(Rc::clone(route));
                }
                let materialized = self.materialize_route(
                    object,
                    binding.has_response_identity_source,
                    None,
                )?;
                *route.borrow_mut() = Some(Rc::clone(&materialized));
                Ok(materialized)
            }
            ScopedRoutes::LocalConfig(entries) => {
                let selected = self.select_storage_server(object)?;
                let storage_server_oid =
                    selected.as_ref().map(SelectedStorageProfile::server_oid);
                if let Some(route) = entries.borrow().get(&storage_server_oid) {
                    return Ok(Rc::clone(route));
                }
                let materialized = self.materialize_route(
                    object,
                    binding.has_response_identity_source,
                    selected.as_ref(),
                )?;
                entries
                    .borrow_mut()
                    .insert(storage_server_oid, Rc::clone(&materialized));
                Ok(materialized)
            }
        }
    }

    fn fallback_route(
        &self,
        routes: &RefCell<FallbackRoutes>,
        object: &ObjectUri,
    ) -> Result<Rc<CatalogRoute>> {
        let provider = ObjectProvider::from(object.scheme());
        let response = self.provider_config(None);
        let has_response_identity_source = response.has_response_identity(provider);
        let needs_local_config = !has_response_identity_source
            || !response.has_connection(provider, object.account());
        let selected = if needs_local_config {
            self.select_storage_server(object)?
        } else {
            None
        };
        let storage_server_oid =
            selected.as_ref().map(SelectedStorageProfile::server_oid);
        if let Some(route) = routes.borrow().get(storage_server_oid, object) {
            return Ok(Rc::clone(route));
        }
        let materialized = self.materialize_route(
            object,
            has_response_identity_source,
            selected.as_ref(),
        )?;
        routes.borrow_mut().insert(
            storage_server_oid,
            object,
            Rc::clone(&materialized),
        );
        Ok(materialized)
    }

    fn select_storage_server(
        &self,
        object: &ObjectUri,
    ) -> Result<Option<SelectedStorageProfile>> {
        self.context.profiles.select(object).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "failed to resolve PostgreSQL storage server",
            )
            .with_source(error)
        })
    }

    fn materialize_route(
        &self,
        object: &ObjectUri,
        has_response_identity_source: bool,
        selected: Option<&SelectedStorageProfile>,
    ) -> Result<Rc<CatalogRoute>> {
        let provider = ObjectProvider::from(object.scheme());
        let account = object.account();
        // Parse REST identities before reading the selected mapping: partial
        // identities cannot acquire fields from a different identity.
        let response_config = self
            .provider_config(None)
            .resolve(provider, account)?
            .into_owned();
        // A switch-only anonymous=false response is not a replacement identity.
        // Local credentials still require a valid server/mapping combination:
        // skip_signature=true with explicit mapping credentials is a conflict,
        // not a supported configuration that REST overrides are expected to repair.
        let local_config = selected
            .map(|selected| selected.config(!has_response_identity_source))
            .transpose()
            .map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "failed to load selected PostgreSQL storage server",
                )
                .with_source(error)
            })?;
        let config = match local_config.as_ref() {
            Some(local_config) => self
                .provider_config(Some(local_config))
                .resolve(provider, account)?
                .into_owned(),
            None => response_config,
        };
        let storage_server_oid = selected.map(SelectedStorageProfile::server_oid);
        Ok(Rc::new(CatalogRoute::new(
            self.prefix(),
            storage_server_oid,
            object,
            config,
            &self.context.catalog_identity,
            &self.context.endpoint,
            &self.context.staging_resolver,
        )?))
    }

    fn prefix(&self) -> Option<&ObjectUriPrefix> {
        match &self.kind {
            BindingKind::Scoped(binding) => Some(&binding.prefix),
            BindingKind::Fallback(_) => None,
        }
    }

    fn provider_config<'a>(
        &'a self,
        local_config: Option<&'a StoreConfig>,
    ) -> ProviderConfig<'a> {
        match &self.kind {
            BindingKind::Scoped(binding) => ProviderConfig::with_profile_overrides(
                &self.context.properties,
                local_config,
                &binding.overrides,
            ),
            BindingKind::Fallback(_) => {
                ProviderConfig::with_profile(&self.context.properties, local_config)
            }
        }
    }
}
