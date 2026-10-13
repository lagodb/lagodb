use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use lagodb_core::storage::foreign::{ForeignOptionView, ForeignUserMapping};
use pgrx::pg_sys;

use super::super::catalog::rest::PgRestCatalogBuilder;
use super::super::catalog::{ForeignCatalog, PgSqliteCatalogBuilder};
use super::super::error::IcebergFdwError;
use super::catalog_type::{CatalogServerOptions, CatalogType};
use super::schema::{OptionLayer, ParsedOptions};

/// Per-role credentials read only from the selected user mapping.
struct UserCredentials {
    mapping_oid: Option<pg_sys::Oid>,
    properties: HashMap<String, String>,
}

impl UserCredentials {
    fn resolve(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        let Some(mapping) = ForeignUserMapping::lookup(server_oid, effective_user)
        else {
            return Ok(Self {
                mapping_oid: None,
                properties: HashMap::new(),
            });
        };
        let options = mapping.options();
        let options = ParsedOptions::from_view(OptionLayer::Mapping, options, true)?;
        Ok(Self {
            mapping_oid: Some(mapping.oid()),
            properties: options.values,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ServerBindingKey {
    pub(crate) server_oid: pg_sys::Oid,
    pub(crate) effective_user: pg_sys::Oid,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CatalogRuntimeConfig {
    catalog_type: CatalogType,
    owner_fdw_oid: pg_sys::Oid,
    mapping_oid: Option<pg_sys::Oid>,
    properties: HashMap<String, String>,
}

impl fmt::Debug for CatalogRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogRuntimeConfig")
            .field("catalog_type", &self.catalog_type)
            .field("owner_fdw_oid", &self.owner_fdw_oid)
            .field("mapping_oid", &self.mapping_oid)
            .field("property_count", &self.properties.len())
            .finish()
    }
}

pub(crate) struct CatalogConnection {
    catalog_type: CatalogType,
    properties: HashMap<String, String>,
    server_oid: pg_sys::Oid,
    owner_fdw_oid: pg_sys::Oid,
    effective_user: pg_sys::Oid,
    mapping_oid: Option<pg_sys::Oid>,
}

impl CatalogConnection {
    pub(crate) fn resolve(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
    ) -> Result<Self, IcebergFdwError> {
        // SAFETY: GetForeignServer returns the live server definition for this OID.
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        let catalog_type = CatalogType::from_server(server)?;
        let owner_fdw_oid = server.fdwid;
        let options = unsafe { ForeignOptionView::from_raw(server.options) };
        let options = ParsedOptions::from_view(OptionLayer::Server, options, true)?;
        let options = CatalogServerOptions::from_options(options)?;
        options.validate_type(catalog_type)?;
        let mut properties = options.into_properties();
        let UserCredentials {
            mapping_oid,
            properties: credential_properties,
        } = UserCredentials::resolve(server_oid, effective_user)?;
        match catalog_type {
            CatalogType::Rest => {
                properties.extend(credential_properties);
                let oauth2 = properties.get("rest.auth.type").map_or_else(
                    || properties.contains_key("oauth2-server-uri"),
                    |auth_type| auth_type.eq_ignore_ascii_case("oauth2"),
                );
                if oauth2
                    && !properties.contains_key("token")
                    && !properties.contains_key("credential")
                {
                    return Err(IcebergFdwError::invalid_option(
                        "rest.auth.type",
                        "oauth2 requires a token or credential in a user mapping",
                    ));
                }
            }
            CatalogType::Sqlite => {
                if !credential_properties.is_empty() {
                    return Err(IcebergFdwError::invalid_option(
                        "user mapping",
                        "SQLite catalogs do not accept authentication options",
                    ));
                }
            }
        }
        Ok(Self {
            catalog_type,
            properties,
            server_oid,
            owner_fdw_oid,
            effective_user,
            mapping_oid,
        })
    }

    pub(crate) fn server_binding_key(&self) -> ServerBindingKey {
        ServerBindingKey {
            server_oid: self.server_oid,
            effective_user: self.effective_user,
        }
    }

    pub(crate) fn runtime_config(&self) -> CatalogRuntimeConfig {
        CatalogRuntimeConfig {
            catalog_type: self.catalog_type,
            owner_fdw_oid: self.owner_fdw_oid,
            mapping_oid: self.mapping_oid,
            properties: self.properties.clone(),
        }
    }

    pub(crate) fn connect(mut self) -> Result<ForeignCatalog, IcebergFdwError> {
        match self.catalog_type {
            CatalogType::Rest => Ok(ForeignCatalog::Rest(Arc::new(
                PgRestCatalogBuilder::default().load(
                    format!("server:{}", u32::from(self.server_oid)),
                    self.properties,
                    self.server_oid,
                    self.owner_fdw_oid,
                    self.effective_user,
                )?,
            ))),
            CatalogType::Sqlite => {
                let name = self
                    .properties
                    .remove("catalog_name")
                    .expect("SQLite configuration requires catalog_name");
                Ok(ForeignCatalog::Sqlite(PgSqliteCatalogBuilder::load(
                    name,
                    self.properties,
                )?))
            }
        }
    }
}
