//! Cache state owned by a REST credential binding.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use lagodb_core::storage::profile::ObjectUri;
use pgrx::pg_sys;

use super::super::routes::{CatalogRoute, ObjectProvider};

pub(super) enum ScopedRoutes {
    /// The REST response completely defines the effective storage config, so
    /// every object accepted by this fixed namespace shares one route.
    ResponseOnly(RefCell<Option<Rc<CatalogRoute>>>),
    /// The object key still selects a scoped PostgreSQL storage server. `None`
    /// retains the valid branch where no local server matches. The fixed prefix
    /// already fixes provider, bucket, and URI account.
    LocalConfig(RefCell<HashMap<Option<pg_sys::Oid>, Rc<CatalogRoute>>>),
}

impl ScopedRoutes {
    pub(super) fn new(needs_local_config: bool) -> Self {
        if needs_local_config {
            Self::LocalConfig(RefCell::new(HashMap::new()))
        } else {
            Self::ResponseOnly(RefCell::new(None))
        }
    }
}

/// Borrow-friendly cache organization for the binding without a fixed
/// namespace. Owned bucket/account strings are created only when inserting a
/// newly materialized route.
#[derive(Default)]
pub(super) struct FallbackRoutes {
    // A selected server fixes provider and bucket, but an omitted Azure account
    // and an explicit account are distinct route-construction inputs.
    storage_servers: HashMap<pg_sys::Oid, AccountRoutes>,
    // Without a selected server, the REST response route is namespaced by every
    // object URI component that can affect the configured service.
    response_namespaces: HashMap<ObjectProvider, HashMap<Box<str>, AccountRoutes>>,
}

#[derive(Default)]
struct AccountRoutes {
    unspecified: Option<Rc<CatalogRoute>>,
    specified: HashMap<Box<str>, Rc<CatalogRoute>>,
}

impl AccountRoutes {
    fn get(&self, account: Option<&str>) -> Option<&Rc<CatalogRoute>> {
        match account {
            Some(account) => self.specified.get(account),
            None => self.unspecified.as_ref(),
        }
    }

    fn insert(&mut self, account: Option<&str>, route: Rc<CatalogRoute>) {
        match account {
            Some(account) => {
                self.specified.insert(account.into(), route);
            }
            None => self.unspecified = Some(route),
        }
    }
}

impl FallbackRoutes {
    pub(super) fn get(
        &self,
        storage_server_oid: Option<pg_sys::Oid>,
        object: &ObjectUri,
    ) -> Option<&Rc<CatalogRoute>> {
        match storage_server_oid {
            Some(storage_server_oid) => self
                .storage_servers
                .get(&storage_server_oid)?
                .get(object.account()),
            None => self
                .response_namespaces
                .get(&ObjectProvider::from(object.scheme()))?
                .get(object.bucket())?
                .get(object.account()),
        }
    }

    pub(super) fn insert(
        &mut self,
        storage_server_oid: Option<pg_sys::Oid>,
        object: &ObjectUri,
        route: Rc<CatalogRoute>,
    ) {
        match storage_server_oid {
            Some(storage_server_oid) => self
                .storage_servers
                .entry(storage_server_oid)
                .or_default()
                .insert(object.account(), route),
            None => self
                .response_namespaces
                .entry(ObjectProvider::from(object.scheme()))
                .or_default()
                .entry(object.bucket().into())
                .or_default()
                .insert(object.account(), route),
        }
    }
}
