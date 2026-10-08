//! PostgreSQL foreign-server selection for object storage locations.

use std::ffi::CStr;

use pgrx::pg_sys;

use crate::catalog::{CatalogRelation, CatalogSnapshot};
use crate::storage::foreign::ForeignOptionView;

use super::{
    ObjectUri, StorageProfileError, StorageProvider, StorageScope,
    StorageServerOptions,
};

/// Catalog ownership constraints for storage-server discovery.
#[derive(Clone, Copy)]
pub struct StorageServerPolicy<'a> {
    owner_fdw_oid: pg_sys::Oid,
    server_type: Option<&'a CStr>,
}

impl<'a> StorageServerPolicy<'a> {
    pub const fn new(
        owner_fdw_oid: pg_sys::Oid,
        server_type: Option<&'a CStr>,
    ) -> Self {
        Self {
            owner_fdw_oid,
            server_type,
        }
    }
}

/// One server selected after wrapper, privilege, provider and scope
/// validation.
#[derive(Clone, Copy, Debug)]
pub struct ResolvedStorageServer<'a> {
    route: &'a StorageServerRoute,
}

impl ResolvedStorageServer<'_> {
    pub fn oid(self) -> pg_sys::Oid {
        self.route.server_oid
    }

    pub fn name(&self) -> &str {
        &self.route.server_name
    }
    pub fn validate_object(
        &self,
        object: &ObjectUri,
    ) -> Result<(), StorageProfileError> {
        self.route.validate_object(object)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StorageServerRoute {
    server_oid: pg_sys::Oid,
    server_name: Box<str>,
    provider: StorageProvider,
    scope_text: Option<Box<str>>,
    scope: Option<StorageScope>,
}

impl StorageServerRoute {
    fn contains(&self, object: &ObjectUri) -> bool {
        self.provider.matches_scheme(object.scheme())
            && self
                .scope
                .as_ref()
                .is_none_or(|scope| scope.contains(object))
    }

    fn validate_object(&self, object: &ObjectUri) -> Result<(), StorageProfileError> {
        if !self.provider.matches_scheme(object.scheme()) {
            return Err(StorageProfileError::ProviderMismatch {
                server: self.server_name.clone(),
                scheme: object.scheme().as_str(),
            });
        }
        if self
            .scope
            .as_ref()
            .is_some_and(|scope| !scope.contains(object))
        {
            return Err(StorageProfileError::ServerOutsideScope {
                server: self.server_name.clone(),
            });
        }
        Ok(())
    }

    fn specificity(&self) -> Option<usize> {
        self.scope.as_ref().map(StorageScope::specificity)
    }
}

/// Cold-path snapshot used by explicit and implicit storage-server resolution.
#[derive(Clone, Debug, Default)]
pub struct StorageServerCatalog {
    routes: Vec<StorageServerRoute>,
}

impl StorageServerCatalog {
    pub fn load(
        policy: StorageServerPolicy<'_>,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, StorageProfileError> {
        let relation = CatalogRelation::open(
            pg_sys::ForeignServerRelationId,
            pg_sys::AccessShareLock as _,
        )?;
        let mut scan = relation.begin_scan(
            pg_sys::InvalidOid,
            false,
            CatalogSnapshot::Default,
            [],
        )?;
        let mut routes = Vec::new();
        while let Some(tuple) = scan.get_next()? {
            // SAFETY: the tuple comes from pg_foreign_server and remains live
            // until the next scan call.
            let form = unsafe {
                &*(pg_sys::GETSTRUCT(tuple.as_raw())
                    as pg_sys::Form_pg_foreign_server)
            };
            if let Some(route) = Self::load_route(policy, effective_user, form.oid)? {
                routes.push(route);
            }
        }
        Self::from_routes(routes)
    }

    /// Loads only the named explicit server. Invalid unrelated servers cannot
    /// affect an operation whose storage binding is already explicit.
    pub fn load_explicit(
        policy: StorageServerPolicy<'_>,
        effective_user: pg_sys::Oid,
        server_name: &CStr,
    ) -> Result<Self, StorageProfileError> {
        let server_oid =
            unsafe { pg_sys::get_foreign_server_oid(server_name.as_ptr(), true) };
        if server_oid == pg_sys::InvalidOid {
            return Err(StorageProfileError::ServerNotFound {
                server: server_name.to_string_lossy().into_owned().into(),
            });
        }
        Self::load_explicit_oid(policy, effective_user, server_oid)
    }

    /// Loads only the explicit server OID recorded by a foreign object.
    pub fn load_explicit_oid(
        policy: StorageServerPolicy<'_>,
        effective_user: pg_sys::Oid,
        server_oid: pg_sys::Oid,
    ) -> Result<Self, StorageProfileError> {
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        let server_name = unsafe { CStr::from_ptr(server.servername) }
            .to_string_lossy()
            .into_owned()
            .into_boxed_str();
        if server.fdwid != policy.owner_fdw_oid
            || !Self::server_type_matches(server_oid, policy.server_type)
        {
            return Err(StorageProfileError::ServerPolicyMismatch {
                server: server_name,
            });
        }
        if !Self::has_usage(server_oid, effective_user) {
            return Err(StorageProfileError::ServerUsageDenied {
                server: server_name,
            });
        }
        let route = Self::parse_route(server_oid, server_name)?;
        Self::from_routes(vec![route])
    }

    fn from_routes(
        mut routes: Vec<StorageServerRoute>,
    ) -> Result<Self, StorageProfileError> {
        routes.sort_by(|left, right| {
            right
                .specificity()
                .cmp(&left.specificity())
                .then_with(|| left.server_name.cmp(&right.server_name))
        });
        Ok(Self { routes })
    }

    fn load_route(
        policy: StorageServerPolicy<'_>,
        effective_user: pg_sys::Oid,
        server_oid: pg_sys::Oid,
    ) -> Result<Option<StorageServerRoute>, StorageProfileError> {
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        if server.fdwid != policy.owner_fdw_oid
            || !Self::server_type_matches(server_oid, policy.server_type)
            || !Self::has_usage(server_oid, effective_user)
        {
            return Ok(None);
        }
        // Only explicitly scoped profiles participate in implicit routing.
        // Unscoped servers remain available through explicit selection.
        // SAFETY: GetForeignServer owns this option list for this catalog lookup.
        let options = unsafe { ForeignOptionView::from_raw(server.options) };
        if !options.iter().any(|option| option.name() == c"scope") {
            return Ok(None);
        }
        let server_name = unsafe { CStr::from_ptr(server.servername) }
            .to_string_lossy()
            .into_owned()
            .into_boxed_str();
        Self::parse_route(server_oid, server_name).map(Some)
    }

    fn parse_route(
        server_oid: pg_sys::Oid,
        server_name: Box<str>,
    ) -> Result<StorageServerRoute, StorageProfileError> {
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        let options = unsafe { ForeignOptionView::from_raw(server.options) };
        let parsed = StorageServerOptions::from_view(options)?;
        let provider = parsed.provider()?.ok_or_else(|| {
            StorageProfileError::invalid_option("provider", "is required")
        })?;
        let (scope_text, scope) = match parsed.scope() {
            Some(text) => (
                Some(Box::<str>::from(text)),
                Some(StorageScope::parse(text, provider, parsed.account())?),
            ),
            None => (None, None),
        };
        Ok(StorageServerRoute {
            server_oid,
            server_name,
            provider,
            scope_text,
            scope,
        })
    }

    pub fn resolve_named(
        &self,
        server_name: &str,
    ) -> Result<ResolvedStorageServer<'_>, StorageProfileError> {
        self.routes
            .iter()
            .find(|route| route.server_name.as_ref() == server_name)
            .map(|route| ResolvedStorageServer { route })
            .ok_or_else(|| StorageProfileError::UnavailableServer {
                server: server_name.into(),
            })
    }

    pub fn resolve_explicit(
        &self,
        server_name: &str,
        object: &ObjectUri,
    ) -> Result<ResolvedStorageServer<'_>, StorageProfileError> {
        let route = self
            .routes
            .iter()
            .find(|route| route.server_name.as_ref() == server_name)
            .ok_or_else(|| StorageProfileError::UnavailableServer {
                server: server_name.into(),
            })?;
        route.validate_object(object)?;
        Ok(ResolvedStorageServer { route })
    }

    pub fn resolve_explicit_oid(
        &self,
        server_oid: pg_sys::Oid,
        object: &ObjectUri,
    ) -> Result<ResolvedStorageServer<'_>, StorageProfileError> {
        let route = self
            .routes
            .iter()
            .find(|route| route.server_oid == server_oid)
            .ok_or_else(|| StorageProfileError::UnavailableServer {
                server: u32::from(server_oid).to_string().into(),
            })?;
        route.validate_object(object)?;
        Ok(ResolvedStorageServer { route })
    }

    pub fn resolve_implicit(
        &self,
        object: &ObjectUri,
    ) -> Result<ResolvedStorageServer<'_>, StorageProfileError> {
        self.resolve_implicit_optional(object)?.ok_or_else(|| {
            StorageProfileError::NoMatchingProfile {
                location: format!(
                    "{}://{}/{}",
                    object.scheme(),
                    object.bucket(),
                    object.key()
                )
                .into(),
            }
        })
    }

    pub(crate) fn resolve_implicit_optional(
        &self,
        object: &ObjectUri,
    ) -> Result<Option<ResolvedStorageServer<'_>>, StorageProfileError> {
        self.select(
            self.routes
                .iter()
                .filter(|route| route.scope.is_some() && route.contains(object)),
        )
    }

    fn select<'a>(
        &'a self,
        mut matches: impl Iterator<Item = &'a StorageServerRoute>,
    ) -> Result<Option<ResolvedStorageServer<'a>>, StorageProfileError> {
        let Some(selected) = matches.next() else {
            return Ok(None);
        };
        if let Some(other) = matches.next()
            && other.specificity() == selected.specificity()
        {
            return Err(StorageProfileError::AmbiguousScope {
                server: selected.server_name.clone(),
                other: other.server_name.clone(),
                scope: selected
                    .scope_text
                    .clone()
                    .expect("implicit candidates always have a scope"),
            });
        }
        Ok(Some(ResolvedStorageServer { route: selected }))
    }

    fn server_type_matches(server_oid: pg_sys::Oid, expected: Option<&CStr>) -> bool {
        let Some(expected) = expected else {
            return true;
        };
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        !server.servertype.is_null()
            && unsafe { CStr::from_ptr(server.servertype) == expected }
    }

    fn has_usage(server_oid: pg_sys::Oid, effective_user: pg_sys::Oid) -> bool {
        unsafe {
            pg_sys::object_aclcheck(
                pg_sys::ForeignServerRelationId,
                server_oid,
                effective_user,
                pg_sys::ACL_USAGE.into(),
            ) == pg_sys::AclResult::ACLCHECK_OK
        }
    }
}
