use std::collections::HashMap;
use std::ffi::CStr;
use std::fmt;

use iceberg_lite::catalog::rest::RestCatalog;
use lagodb_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;

use super::super::catalog::rest::PgRestCatalogBuilder;
use super::super::error::IcebergFdwError;
use super::schema::{ENABLE_VENDED_CREDENTIALS, OptionLayer, ParsedOptions};

const VENDED_CREDENTIALS_HEADER: &str = "header.x-iceberg-access-delegation";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatalogType {
    Rest,
}

impl CatalogType {
    fn from_server(server: &pg_sys::ForeignServer) -> Result<Self, IcebergFdwError> {
        if server.servertype.is_null() {
            return Err(IcebergFdwError::InvalidCatalogType {
                actual: "<unspecified>".to_owned(),
            });
        }
        let server_type = unsafe { CStr::from_ptr(server.servertype) }
            .to_str()
            .map_err(|_| IcebergFdwError::InvalidUtf8 {
                subject: "foreign server type",
            })?;
        match server_type {
            "rest" => Ok(Self::Rest),
            actual => Err(IcebergFdwError::InvalidCatalogType {
                actual: actual.to_owned(),
            }),
        }
    }
}

/// Non-secret connection configuration read only from the foreign server.
struct PublicCatalogConfig {
    catalog_type: CatalogType,
    owner_fdw_oid: pg_sys::Oid,
    properties: HashMap<String, String>,
}

impl PublicCatalogConfig {
    fn resolve(server_oid: pg_sys::Oid) -> Result<Self, IcebergFdwError> {
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        let catalog_type = CatalogType::from_server(server)?;
        let options = unsafe { ForeignOptionView::from_raw(server.options) };
        let options = ParsedOptions::from_view(OptionLayer::Server, options, true)?;
        let mut properties = options.values;
        let enable_vended_credentials = properties
            .remove(ENABLE_VENDED_CREDENTIALS)
            .is_none_or(|value| value.eq_ignore_ascii_case("true"));
        if enable_vended_credentials {
            properties.insert(
                VENDED_CREDENTIALS_HEADER.to_owned(),
                "vended-credentials".to_owned(),
            );
        }
        Ok(Self {
            catalog_type,
            owner_fdw_oid: server.fdwid,
            properties,
        })
    }
}

/// Per-role credentials read only from the selected user mapping.
struct UserCredentials {
    mapping_oid: pg_sys::Oid,
    properties: HashMap<String, String>,
}

impl UserCredentials {
    fn resolve(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        let mapping = unsafe { &*pg_sys::GetUserMapping(effective_user, server_oid) };
        let options = unsafe { ForeignOptionView::from_raw(mapping.options) };
        let options = ParsedOptions::from_view(OptionLayer::Mapping, options, true)?;
        Ok(Self {
            mapping_oid: mapping.umid,
            properties: options.values,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ServerBindingKey {
    pub(crate) server_oid: pg_sys::Oid,
    pub(crate) effective_user: pg_sys::Oid,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CatalogBindingKey {
    pub(crate) server: ServerBindingKey,
    pub(crate) catalog_name: String,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CatalogRuntimeConfig {
    owner_fdw_oid: pg_sys::Oid,
    mapping_oid: pg_sys::Oid,
    properties: HashMap<String, String>,
}

impl fmt::Debug for CatalogRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogRuntimeConfig")
            .field("owner_fdw_oid", &self.owner_fdw_oid)
            .field("mapping_oid", &self.mapping_oid)
            .field("property_count", &self.properties.len())
            .finish()
    }
}

pub(crate) struct RestCatalogConnection {
    catalog_name: String,
    properties: HashMap<String, String>,
    server_oid: pg_sys::Oid,
    owner_fdw_oid: pg_sys::Oid,
    effective_user: pg_sys::Oid,
    mapping_oid: pg_sys::Oid,
}

impl RestCatalogConnection {
    pub(crate) fn validate_server(
        server_oid: pg_sys::Oid,
    ) -> Result<(), IcebergFdwError> {
        match PublicCatalogConfig::resolve(server_oid)?.catalog_type {
            CatalogType::Rest => Ok(()),
        }
    }

    pub(crate) fn resolve(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        catalog_name: impl Into<String>,
    ) -> Result<Self, IcebergFdwError> {
        let PublicCatalogConfig {
            catalog_type,
            owner_fdw_oid,
            mut properties,
        } = PublicCatalogConfig::resolve(server_oid)?;
        let UserCredentials {
            mapping_oid,
            properties: credential_properties,
        } = UserCredentials::resolve(server_oid, effective_user)?;
        properties.extend(credential_properties);
        match catalog_type {
            CatalogType::Rest => Ok(Self {
                catalog_name: catalog_name.into(),
                properties,
                server_oid,
                owner_fdw_oid,
                effective_user,
                mapping_oid,
            }),
        }
    }

    pub(crate) fn server_binding_key(&self) -> ServerBindingKey {
        ServerBindingKey {
            server_oid: self.server_oid,
            effective_user: self.effective_user,
        }
    }

    pub(crate) fn catalog_binding_key(&self) -> CatalogBindingKey {
        CatalogBindingKey {
            server: self.server_binding_key(),
            catalog_name: self.catalog_name.clone(),
        }
    }

    pub(crate) fn runtime_config(&self) -> CatalogRuntimeConfig {
        CatalogRuntimeConfig {
            owner_fdw_oid: self.owner_fdw_oid,
            mapping_oid: self.mapping_oid,
            properties: self.properties.clone(),
        }
    }

    pub(crate) fn connect(self) -> Result<RestCatalog, IcebergFdwError> {
        PgRestCatalogBuilder::default()
            .load(
                self.catalog_name,
                self.properties,
                self.server_oid,
                self.owner_fdw_oid,
                self.effective_user,
            )
            .map_err(IcebergFdwError::from)
    }
}
