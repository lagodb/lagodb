//! Deferred local storage configuration for REST object routes.

use std::cell::OnceCell;
use std::ffi::CStr;
use std::rc::Rc;

use lagodb_storage::StoreConfig;
use pgrx::pg_sys;

use crate::storage::foreign::{ForeignOptionView, ForeignUserMapping};

use super::mapping::MappingOptions;
use super::{
    ObjectUri, StorageProfileError, StorageServerCatalog, StorageServerOptions,
    StorageServerPolicy,
};

/// One operation's routing snapshot. Construction performs no catalog access;
/// the snapshot contains routing metadata, never initialized backends.
#[derive(Clone, Debug)]
pub struct StorageProfiles {
    owner_fdw_oid: pg_sys::Oid,
    server_type: &'static CStr,
    effective_user: pg_sys::Oid,
    catalog: Rc<OnceCell<StorageServerCatalog>>,
}

impl StorageProfiles {
    pub fn new(
        owner_fdw_oid: pg_sys::Oid,
        server_type: &'static CStr,
        effective_user: pg_sys::Oid,
    ) -> Self {
        Self {
            owner_fdw_oid,
            server_type,
            effective_user,
            catalog: Rc::new(OnceCell::new()),
        }
    }

    /// Selects from routing metadata without reading user mappings or
    /// initializing storage. No match leaves the default provider available.
    pub fn select(
        &self,
        object: &ObjectUri,
    ) -> Result<Option<SelectedStorageProfile>, StorageProfileError> {
        if self.catalog.get().is_none() {
            let catalog = StorageServerCatalog::load(
                StorageServerPolicy::new(self.owner_fdw_oid, Some(self.server_type)),
                self.effective_user,
            )?;
            self.catalog.set(catalog).expect(
                "storage routing snapshot is initialized on one backend thread",
            );
        }
        let catalog = self
            .catalog
            .get()
            .expect("storage routing snapshot initialized above");
        Ok(catalog.resolve_implicit_optional(object)?.map(|selected| {
            SelectedStorageProfile {
                server_oid: selected.oid(),
                effective_user: self.effective_user,
            }
        }))
    }
}

/// A selected route can load its public configuration independently of its
/// identity. When local credentials are requested, the server and mapping must
/// already form a valid local authentication configuration. Their DDL options
/// are validated separately; accepting both catalog objects does not guarantee
/// that their authentication settings can be combined. The caller validates
/// the effective backend configuration after REST values merge.
pub struct SelectedStorageProfile {
    server_oid: pg_sys::Oid,
    effective_user: pg_sys::Oid,
}

impl SelectedStorageProfile {
    pub fn server_oid(&self) -> pg_sys::Oid {
        self.server_oid
    }

    pub fn config(
        &self,
        include_credentials: bool,
    ) -> Result<StoreConfig, StorageProfileError> {
        // SAFETY: the route records a live foreign-server OID from this snapshot.
        let server = unsafe { &*pg_sys::GetForeignServer(self.server_oid) };
        let options = unsafe { ForeignOptionView::from_raw(server.options) };
        let server = StorageServerOptions::from_view(options)?;
        let mapping = if include_credentials {
            ForeignUserMapping::lookup(self.server_oid, self.effective_user)
        } else {
            None
        };
        let mapping = match &mapping {
            Some(mapping) => MappingOptions::from_view(mapping.options())?,
            None => MappingOptions::default(),
        };
        server.into_profile(mapping)
    }
}
