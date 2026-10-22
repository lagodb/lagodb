//! Server catalog types and configuration shared by DDL and runtime binding.

use std::collections::HashMap;
use std::ffi::CStr;

use iceberg_lite::io::Storage;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::schema::ParsedOptions;
use crate::storage::LocalStorage;

pub(super) const REQUEST_VENDED_CREDENTIALS: &str = "request_vended_credentials";
const VENDED_CREDENTIALS_HEADER: &str = "header.x-iceberg-access-delegation";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::foreign_table) enum CatalogType {
    Rest,
    Sqlite,
}

impl CatalogType {
    fn server_type(
        server: &pg_sys::ForeignServer,
    ) -> Result<Option<&str>, IcebergFdwError> {
        if server.servertype.is_null() {
            return Ok(None);
        }
        // SAFETY: GetForeignServer supplies a live, terminated type string.
        unsafe { CStr::from_ptr(server.servertype) }
            .to_str()
            .map(Some)
            .map_err(|_| IcebergFdwError::InvalidUtf8 {
                subject: "foreign server type",
            })
    }

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "rest" => Some(Self::Rest),
            "sqlite" => Some(Self::Sqlite),
            _ => None,
        }
    }

    pub(in crate::foreign_table) fn from_server(
        server: &pg_sys::ForeignServer,
    ) -> Result<Self, IcebergFdwError> {
        let server_type = Self::server_type(server)?;
        server_type.and_then(Self::from_name).ok_or_else(|| {
            IcebergFdwError::InvalidCatalogType {
                actual: server_type.unwrap_or("<unspecified>").to_owned(),
            }
        })
    }

    pub(super) fn catalog_from_server(
        server: &pg_sys::ForeignServer,
    ) -> Result<Option<Self>, IcebergFdwError> {
        Ok(Self::server_type(server)?.and_then(Self::from_name))
    }

    pub(super) fn is_identity_option(self, name: &str) -> bool {
        match self {
            Self::Rest => {
                matches!(name, "uri" | "warehouse" | "prefix" | "oauth2-server-uri")
            }
            Self::Sqlite => matches!(name, "uri" | "warehouse" | "catalog_name"),
        }
    }

    /// Server option ownership shared by DDL and catalog configuration parsing.
    pub(super) fn accepts_server_option(self, name: &str) -> bool {
        if matches!(name, "uri" | "warehouse") {
            return true;
        }
        match self {
            Self::Rest => matches!(
                name,
                "prefix"
                    | "rest.auth.type"
                    | "oauth2-server-uri"
                    | "scope"
                    | "audience"
                    | "resource"
                    | REQUEST_VENDED_CREDENTIALS
            ),
            Self::Sqlite => name == "catalog_name",
        }
    }
}

/// Non-secret server options shared by DDL validation and runtime binding.
/// PostgreSQL identity and user-mapping credentials belong to CatalogConnection.
pub(in crate::foreign_table) struct CatalogServerOptions {
    catalog_type: CatalogType,
    properties: HashMap<String, String>,
}

impl CatalogServerOptions {
    /// ParsedOptions has already checked individual values and the required URI.
    pub(in crate::foreign_table) fn from_options(
        options: ParsedOptions,
    ) -> Result<Self, IcebergFdwError> {
        let properties = options.values;
        // ParsedOptions already accepted either a sqlite: URI or an HTTP(S) URL.
        // Classify it without reparsing or changing the original string.
        let uri = &properties["uri"];
        let catalog_type = if uri.starts_with("sqlite:") {
            CatalogType::Sqlite
        } else {
            CatalogType::Rest
        };
        for name in properties.keys() {
            if !catalog_type.accepts_server_option(name) {
                return Err(IcebergFdwError::unsupported_option(name.as_str()));
            }
        }
        if catalog_type == CatalogType::Sqlite {
            for name in ["catalog_name", "warehouse"] {
                if !properties.contains_key(name) {
                    return Err(IcebergFdwError::MissingOption { name });
                }
            }
            if properties["catalog_name"].trim().is_empty() {
                return Err(IcebergFdwError::invalid_option(
                    "catalog_name",
                    "must not be blank",
                ));
            }
            // Use the same path rules as the catalog's local storage adapter.
            // Resolving a URI performs no filesystem I/O.
            LocalStorage::with_wal(false)
                .with_absolute_paths()
                .resolve_uri(&properties["warehouse"])?;
        }
        Ok(Self {
            catalog_type,
            properties,
        })
    }

    pub(super) fn catalog_type(&self) -> CatalogType {
        self.catalog_type
    }

    /// The FDW validator receives options and the PG catalog OID, not server TYPE.
    /// Runtime binding checks TYPE against the same parsed configuration.
    pub(in crate::foreign_table) fn validate_type(
        &self,
        expected: CatalogType,
    ) -> Result<(), IcebergFdwError> {
        if self.catalog_type != expected {
            let reason = match expected {
                CatalogType::Rest => "REST server requires an HTTP or HTTPS URI",
                CatalogType::Sqlite => "SQLite server requires a sqlite: URI",
            };
            return Err(IcebergFdwError::invalid_option("uri", reason));
        }
        Ok(())
    }

    pub(in crate::foreign_table) fn into_properties(
        mut self,
    ) -> HashMap<String, String> {
        if self.catalog_type == CatalogType::Rest {
            let request_vended_credentials = self
                .properties
                .remove(REQUEST_VENDED_CREDENTIALS)
                .is_none_or(|value| value.eq_ignore_ascii_case("true"));
            if request_vended_credentials {
                self.properties.insert(
                    VENDED_CREDENTIALS_HEADER.to_owned(),
                    "vended-credentials".to_owned(),
                );
            }
        }
        self.properties
    }
}
