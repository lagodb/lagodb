//! PostgreSQL authorization and catalog binding for connector entry points.
//!
//! COPY preparation owns server-file privileges. FDW path changes own read
//! authorization; table/server privileges then govern scans. Inserts require
//! local write authorization for the executor's effective user.

use std::ffi::{CStr, CString};

use lagodb_core::diag::PgReportError;
use lagodb_core::storage::profile::{
    StorageProfileConfig, StorageServerCatalog, StorageServerPolicy,
};
use pgrx::{PgSqlErrorCode, pg_sys};

use crate::CONNECTOR_FDW_NAME;
use crate::error::ConnectorError;
use crate::storage::{ResolvedStorageLocation, StoragePath};

pub(crate) struct ConnectorAccess;

impl ConnectorAccess {
    pub(crate) fn validate_options(
        options: &[Option<String>],
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), ConnectorError> {
        // An optionless server is the connector's local-file namespace.
        if catalog == Some(pg_sys::ForeignServerRelationId)
            && options.iter().flatten().next().is_none()
        {
            return Ok(());
        }
        StorageProfileConfig::validate_options(options, catalog).map_err(Into::into)
    }

    pub(crate) fn resolve(
        object: StoragePath,
        explicit_server: Option<&str>,
    ) -> Result<ResolvedStorageLocation, ConnectorError> {
        let effective_user = unsafe { pg_sys::GetUserId() };
        let StoragePath::Object(remote) = &object else {
            if explicit_server.is_some() {
                return Err(ConnectorError::invalid_copy_option(
                    "server",
                    "is only valid for object-storage COPY",
                ));
            }
            return Ok(ResolvedStorageLocation::new(
                object,
                pg_sys::InvalidOid,
                effective_user,
            ));
        };
        let server_oid = match explicit_server {
            Some(server) => {
                let server_name = CString::new(server).map_err(|_| {
                    ConnectorError::invalid_copy_option(
                        "server",
                        "must not contain a NUL byte",
                    )
                })?;
                let catalog = Self::explicit_server_catalog(
                    effective_user,
                    server_name.as_c_str(),
                )?;
                catalog.resolve_explicit(server, remote)?.oid()
            }
            None => Self::server_catalog(effective_user)?
                .resolve_implicit(remote)?
                .oid(),
        };
        Ok(ResolvedStorageLocation::new(
            object,
            server_oid,
            effective_user,
        ))
    }

    pub(crate) fn resolve_foreign_object(
        object: StoragePath,
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<ResolvedStorageLocation, ConnectorError> {
        let StoragePath::Object(remote) = &object else {
            Self::check_local_server(server_oid, effective_user)?;
            return Ok(ResolvedStorageLocation::new(
                object,
                server_oid,
                effective_user,
            ));
        };
        let catalog = StorageServerCatalog::load_explicit_oid(
            Self::server_policy(),
            effective_user,
            server_oid,
        )?;
        let selected = catalog.resolve_explicit_oid(server_oid, remote)?;
        Ok(ResolvedStorageLocation::new(
            object,
            selected.oid(),
            effective_user,
        ))
    }

    pub(crate) fn resolve_for_ddl(
        object: StoragePath,
        server_name: &CStr,
    ) -> Result<ResolvedStorageLocation, ConnectorError> {
        let effective_user = unsafe { pg_sys::GetUserId() };
        let StoragePath::Object(remote) = &object else {
            LocalFilePolicy::require_read(effective_user)?;
            let server_oid = unsafe {
                pg_sys::get_foreign_server_oid(server_name.as_ptr(), false)
            };
            Self::check_local_server(server_oid, effective_user)?;
            return Ok(ResolvedStorageLocation::new(
                object,
                server_oid,
                effective_user,
            ));
        };
        let catalog = Self::explicit_server_catalog(effective_user, server_name)?;
        let server_name = server_name.to_str().map_err(|_| {
            ConnectorError::invalid_option("server", "must be valid UTF-8")
        })?;
        let selected = catalog.resolve_explicit(server_name, remote)?;
        Ok(ResolvedStorageLocation::new(
            object,
            selected.oid(),
            effective_user,
        ))
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
        let provider_oid = unsafe {
            pg_sys::get_foreign_data_wrapper_oid(CONNECTOR_FDW_NAME.as_ptr(), true)
        };
        StorageServerPolicy::new(provider_oid, None)
    }

    fn check_local_server(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<(), ConnectorError> {
        // Local files need server USAGE, but no object-store profile or user mapping.
        // SAFETY: the caller resolved a live foreign-server OID on the backend.
        let allowed = unsafe {
            pg_sys::object_aclcheck(
                pg_sys::ForeignServerRelationId,
                server_oid,
                effective_user,
                pg_sys::ACL_USAGE.into(),
            ) == pg_sys::AclResult::ACLCHECK_OK
        };
        if allowed {
            Ok(())
        } else {
            Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
                "permission denied for local-file foreign server",
            )
            .into())
        }
    }
}

pub(crate) struct LocalFilePolicy;

impl LocalFilePolicy {
    pub(crate) fn require_read(user: pg_sys::Oid) -> Result<(), ConnectorError> {
        Self::require_role(
            user,
            pg_sys::ROLE_PG_READ_SERVER_FILES.into(),
            "pg_read_server_files",
        )
    }

    pub(crate) fn require_write(user: pg_sys::Oid) -> Result<(), ConnectorError> {
        Self::require_role(
            user,
            pg_sys::ROLE_PG_WRITE_SERVER_FILES.into(),
            "pg_write_server_files",
        )
    }

    fn require_role(
        user: pg_sys::Oid,
        role: pg_sys::Oid,
        name: &str,
    ) -> Result<(), ConnectorError> {
        // SAFETY: called on the backend thread with catalog role OIDs.
        if unsafe { pg_sys::has_privs_of_role(user, role) } {
            Ok(())
        } else {
            Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
                format!("local file access requires privileges of the {name} role"),
            )
            .into())
        }
    }
}
