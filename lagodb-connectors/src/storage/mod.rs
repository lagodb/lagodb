//! Shared object-location and ForeignServer storage resolution.

mod config;
mod invalidation;
mod location;
mod object_input;
mod object_output;
mod upload;
mod uri;

pub(crate) use config::{ConnectorStoreConfig, validate_storage_options};
pub(crate) use location::ObjectLocationKind;
pub(crate) use object_input::{ObjectFiles, ObjectInput};
pub(crate) use object_output::{AllocatedObject, ObjectFileSuffix, ObjectOutput};
pub(crate) use upload::{StagedObjectUpload, StagedObjectWriter};
pub(crate) use uri::ObjectUri;

use std::ffi::CStr;

use pg_lakebase_core::storage::foreign::{
    ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use pg_lakebase_core::storage::profile::{StorageServerCatalog, StorageServerPolicy};
use pgrx::pg_sys;

use crate::error::ConnectorError;

pub(crate) struct ResolvedStorageLocation {
    server_oid: pg_sys::Oid,
    effective_user: pg_sys::Oid,
    object: ObjectUri,
}

impl ResolvedStorageLocation {
    pub(crate) fn resolve(
        object: ObjectUri,
        explicit_server: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        let effective_user = unsafe { pg_sys::GetUserId() };
        let server_oid = match explicit_server {
            Some(server) => {
                let server_name = std::ffi::CString::new(server).map_err(|_| {
                    ConnectorError::invalid_copy_option(
                        "server",
                        "must not contain a NUL byte",
                    )
                })?;
                let catalog = Self::explicit_server_catalog(
                    effective_user,
                    server_name.as_c_str(),
                )?;
                catalog.resolve_explicit(server, &object)?.oid()
            }
            None => Self::server_catalog(effective_user)?
                .resolve_implicit(&object)?
                .oid(),
        };
        Ok(Self {
            server_oid,
            effective_user,
            object,
        })
    }

    pub(crate) fn resolve_foreign_object(
        object: ObjectUri,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, ConnectorError> {
        let catalog = StorageServerCatalog::load_explicit_oid(
            Self::server_policy(),
            effective_user,
            server_oid,
        )?;
        let selected = catalog.resolve_explicit_oid(server_oid, &object)?;
        Ok(Self {
            server_oid: selected.oid(),
            effective_user,
            object,
        })
    }

    pub(crate) fn resolve_for_ddl(
        object: ObjectUri,
        server_name: &CStr,
    ) -> Result<Self, ConnectorError> {
        let effective_user = unsafe { pg_sys::GetUserId() };
        let catalog = Self::explicit_server_catalog(effective_user, server_name)?;
        let server_name = server_name.to_str().map_err(|_| {
            ConnectorError::invalid_option("server", "must be valid UTF-8")
        })?;
        let selected = catalog.resolve_explicit(server_name, &object)?;
        Ok(Self {
            server_oid: selected.oid(),
            effective_user,
            object,
        })
    }

    pub(crate) fn server_uses_lakebase(server_name: &CStr) -> bool {
        let server_oid =
            unsafe { pg_sys::get_foreign_server_oid(server_name.as_ptr(), true) };
        if server_oid == pg_sys::InvalidOid {
            return false;
        }
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        let lakebase_fdw = unsafe {
            pg_sys::get_foreign_data_wrapper_oid(c"lakebase_fdw".as_ptr(), true)
        };
        server.fdwid == lakebase_fdw
    }

    pub(crate) fn relation_uses_lakebase(relation_oid: pg_sys::Oid) -> bool {
        let table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        let server = unsafe { &*pg_sys::GetForeignServer(table.serverid) };
        let lakebase_fdw = unsafe {
            pg_sys::get_foreign_data_wrapper_oid(c"lakebase_fdw".as_ptr(), true)
        };
        server.fdwid == lakebase_fdw
    }

    fn server_catalog(
        effective_user: pg_sys::Oid,
    ) -> Result<StorageServerCatalog, ConnectorError> {
        StorageServerCatalog::load(Self::server_policy(), effective_user)
            .map_err(Into::into)
    }

    fn explicit_server_catalog(
        effective_user: pg_sys::Oid,
        server_name: &CStr,
    ) -> Result<StorageServerCatalog, ConnectorError> {
        StorageServerCatalog::load_explicit(
            Self::server_policy(),
            effective_user,
            server_name,
        )
        .map_err(Into::into)
    }

    fn server_policy() -> StorageServerPolicy<'static> {
        let lakebase_fdw = unsafe {
            pg_sys::get_foreign_data_wrapper_oid(c"lakebase_fdw".as_ptr(), true)
        };
        StorageServerPolicy::new(lakebase_fdw, None)
    }

    pub(crate) fn acquire_object_access(
        &self,
        manager: &StorageManager,
    ) -> Result<ObjectAccess, ConnectorError> {
        manager
            .acquire_object_access::<ConnectorStoreConfig>(
                self.server_oid,
                self.effective_user,
                self.object.bucket(),
                self.object.key(),
            )
            .map_err(ConnectorError::storage_acquire)
    }

    pub(crate) fn acquire_prefix_access(
        &self,
        manager: &StorageManager,
        prefix: &str,
    ) -> Result<ObjectPrefixAccess, ConnectorError> {
        manager
            .acquire_prefix_access::<ConnectorStoreConfig>(
                self.server_oid,
                self.effective_user,
                self.object.bucket(),
                prefix,
            )
            .map_err(ConnectorError::storage_acquire)
    }

    pub(crate) fn object_key(&self) -> &str {
        self.object.key()
    }

    pub(crate) fn normalized_prefix(&self) -> String {
        let key = self.object.key();
        if key.ends_with('/') {
            key.to_owned()
        } else {
            format!("{key}/")
        }
    }

    pub(crate) fn acquire_object_access_from_pg_gucs(
        &self,
    ) -> Result<ObjectAccess, ConnectorError> {
        let manager = StorageManager::from_pg_gucs()?;
        self.acquire_object_access(&manager)
    }
}
