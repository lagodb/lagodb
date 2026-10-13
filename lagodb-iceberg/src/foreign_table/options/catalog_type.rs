//! Server catalog types and configuration shared by DDL and runtime binding.

use std::collections::HashMap;
use std::ffi::CStr;

use iceberg_lite::io::Storage;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::schema::ParsedOptions;
use crate::storage::LocalStorage;

pub(super) const ENABLE_VENDED_CREDENTIALS: &str = "enable_vended_credentials";
const VENDED_CREDENTIALS_HEADER: &str = "header.x-iceberg-access-delegation";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CatalogType {
    Rest,
    Sqlite,
}

impl CatalogType {
    pub(super) fn from_server(
        server: &pg_sys::ForeignServer,
    ) -> Result<Self, IcebergFdwError> {
        if server.servertype.is_null() {
            return Err(IcebergFdwError::InvalidCatalogType {
                actual: "<unspecified>".to_owned(),
            });
        }
        // SAFETY: GetForeignServer supplies a live, terminated type string.
        let server_type = unsafe { CStr::from_ptr(server.servertype) }
            .to_str()
            .map_err(|_| IcebergFdwError::InvalidUtf8 {
                subject: "foreign server type",
            })?;
        match server_type {
            "rest" => Ok(Self::Rest),
            "sqlite" => Ok(Self::Sqlite),
            actual => Err(IcebergFdwError::InvalidCatalogType {
                actual: actual.to_owned(),
            }),
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
                    | ENABLE_VENDED_CREDENTIALS
            ),
            Self::Sqlite => name == "catalog_name",
        }
    }
}

/// Non-secret server options shared by DDL validation and runtime binding.
/// PostgreSQL identity and user-mapping credentials belong to CatalogConnection.
pub(super) struct CatalogServerOptions {
    catalog_type: CatalogType,
    properties: HashMap<String, String>,
}

impl CatalogServerOptions {
    /// ParsedOptions has already checked individual values and the required URI.
    pub(super) fn from_options(
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
    pub(super) fn validate_type(
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

    pub(super) fn into_properties(mut self) -> HashMap<String, String> {
        if self.catalog_type == CatalogType::Rest {
            let enable_vended_credentials = self
                .properties
                .remove(ENABLE_VENDED_CREDENTIALS)
                .is_none_or(|value| value.eq_ignore_ascii_case("true"));
            if enable_vended_credentials {
                self.properties.insert(
                    VENDED_CREDENTIALS_HEADER.to_owned(),
                    "vended-credentials".to_owned(),
                );
            }
        }
        self.properties
    }
}
