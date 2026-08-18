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
pub(crate) use uri::{ObjectScheme, ObjectUri, StorageScope};

use std::ffi::{CStr, CString};

use pg_lakebase_core::storage::foreign::{
    ForeignOptionView, ObjectAccess, ObjectPrefixAccess, StorageManager,
};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::gucs::DefaultServerConfig;

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
        let server_name = match explicit_server {
            Some(server) => CString::new(server.as_bytes()).map_err(|_| {
                ConnectorError::invalid_copy_option(
                    "server",
                    "must not contain a NUL byte",
                )
            })?,
            None => {
                let config = match object.scheme() {
                    ObjectScheme::S3 => DefaultServerConfig::s3(),
                    ObjectScheme::Gcs => DefaultServerConfig::gcs(),
                    ObjectScheme::Azure => DefaultServerConfig::azure(),
                };
                config.server_name().ok_or_else(|| {
                    ConnectorError::default_server_not_configured(
                        object.scheme().as_str(),
                        config.guc_name(),
                    )
                })?
            }
        };
        let server_oid =
            unsafe { pg_sys::get_foreign_server_oid(server_name.as_ptr(), true) };
        if server_oid == pg_sys::InvalidOid {
            return Err(ConnectorError::server_not_found(
                &server_name.to_string_lossy(),
            ));
        }

        Self::resolve_on_server(object, server_oid, unsafe { pg_sys::GetUserId() })
    }

    pub(crate) fn resolve_foreign_object(
        object: ObjectUri,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, ConnectorError> {
        Self::resolve_on_server(object, server_oid, effective_user)
    }

    pub(crate) fn resolve_for_ddl(
        object: ObjectUri,
        server_name: &CStr,
    ) -> Result<Self, ConnectorError> {
        let server_oid =
            unsafe { pg_sys::get_foreign_server_oid(server_name.as_ptr(), true) };
        if server_oid == pg_sys::InvalidOid {
            return Err(ConnectorError::server_not_found(
                &server_name.to_string_lossy(),
            ));
        }
        Self::resolve_on_server(object, server_oid, unsafe { pg_sys::GetUserId() })
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

    fn resolve_on_server(
        object: ObjectUri,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, ConnectorError> {
        // PostgreSQL returns a live catalog object for a valid server OID.
        // The resolver never fabricates a foreign relation to reach this
        // catalog boundary.
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        let server_name =
            unsafe { CStr::from_ptr(server.servername) }.to_string_lossy();
        let lakebase_fdw = unsafe {
            pg_sys::get_foreign_data_wrapper_oid(c"lakebase_fdw".as_ptr(), true)
        };
        if server.fdwid != lakebase_fdw {
            return Err(ConnectorError::server_wrong_fdw(&server_name));
        }

        let usage = unsafe {
            pg_sys::object_aclcheck(
                pg_sys::ForeignServerRelationId,
                server_oid,
                effective_user,
                pg_sys::ACL_USAGE.into(),
            )
        };
        if usage != pg_sys::AclResult::ACLCHECK_OK {
            return Err(ConnectorError::server_usage_denied(&server_name));
        }

        // SAFETY: `GetForeignServer` returned the live catalog object and its
        // options list remains valid for this cold-path resolution call.
        let server_options = unsafe { ForeignOptionView::from_raw(server.options) };
        let parsed = config::ServerOptions::from_view(server_options)?;
        let provider = parsed.provider()?.ok_or_else(|| {
            ConnectorError::invalid_option("provider", "is required")
        })?;
        if !provider.matches_scheme(object.scheme()) {
            return Err(ConnectorError::provider_mismatch(
                &server_name,
                object.scheme().as_str(),
            ));
        }
        if let Some(scope) = parsed.scope() {
            let scope = StorageScope::parse(scope, provider)?;
            if !scope.contains(&object) {
                return Err(ConnectorError::scope_denied(&server_name));
            }
        }

        Ok(Self {
            server_oid,
            effective_user,
            object,
        })
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
