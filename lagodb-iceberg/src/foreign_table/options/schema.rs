use std::collections::HashMap;

use http::header::{HeaderName, HeaderValue};
use iceberg_lite::catalog::sqlite::SqliteCatalogBuilder;
use lagodb_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;
use url::Url;

use super::super::error::IcebergFdwError;
use super::catalog_type::{CatalogType, ENABLE_VENDED_CREDENTIALS};
use super::table::ForeignTableIdentity;

pub(super) const TABLE: &str = "table";
pub(super) const SCHEMA_ID: &str = "schema_id";

#[derive(Clone, Copy)]
pub(super) enum OptionLayer {
    Wrapper,
    Server,
    Mapping,
    Table,
    Column,
    Import,
}

impl OptionLayer {
    pub(super) fn from_catalog(catalog: Option<pg_sys::Oid>) -> Self {
        match catalog {
            Some(oid) if oid == pg_sys::ForeignDataWrapperRelationId => Self::Wrapper,
            Some(oid) if oid == pg_sys::ForeignServerRelationId => Self::Server,
            Some(oid) if oid == pg_sys::UserMappingRelationId => Self::Mapping,
            Some(oid) if oid == pg_sys::ForeignTableRelationId => Self::Table,
            Some(oid) if oid == pg_sys::AttributeRelationId => Self::Column,
            None => Self::Import,
            Some(_) => Self::Wrapper,
        }
    }

    pub(super) fn accepts(self, name: &str) -> bool {
        match self {
            Self::Wrapper | Self::Column => false,
            Self::Import => name == "strict",
            Self::Server => {
                CatalogType::Rest.accepts_server_option(name)
                    || CatalogType::Sqlite.accepts_server_option(name)
            }
            Self::Mapping => {
                matches!(
                    name,
                    "token"
                        | "credential"
                        | "oauth2-server-uri"
                        | "scope"
                        | "audience"
                        | "resource"
                ) || name.starts_with("header.")
            }
            Self::Table => matches!(name, "table" | "schema_id"),
        }
    }

    pub(super) fn validate(
        self,
        options: &[Option<String>],
    ) -> Result<(), IcebergFdwError> {
        let parsed = ParsedOptions::from_array(self, options)?;
        if matches!(self, Self::Table) {
            ForeignTableIdentity::from_options(parsed)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct ParsedOptions {
    pub(super) values: HashMap<String, String>,
}

impl ParsedOptions {
    /// Parse the complete options array supplied by the PostgreSQL validator.
    pub(super) fn from_array(
        layer: OptionLayer,
        options: &[Option<String>],
    ) -> Result<Self, IcebergFdwError> {
        let mut parsed = Self::default();
        for option in options.iter().flatten() {
            let (name, value) = option.split_once('=').ok_or_else(|| {
                IcebergFdwError::invalid_option(
                    "foreign option",
                    "expected name=value",
                )
            })?;
            parsed.set(layer, name, value)?;
        }
        parsed.validate_required(layer)?;
        Ok(parsed)
    }

    fn validate_value(name: &str, value: &str) -> Result<(), IcebergFdwError> {
        match name {
            "uri" if value.starts_with("sqlite:") => {
                SqliteCatalogBuilder::database_path(value)?;
            }
            "uri" | "oauth2-server-uri" => {
                let url = Url::parse(value).map_err(|_| {
                    IcebergFdwError::invalid_option(name, "must be an absolute URL")
                })?;
                if !matches!(url.scheme(), "http" | "https") {
                    return Err(IcebergFdwError::invalid_option(
                        name,
                        "must use the http or https scheme",
                    ));
                }
                if !url.username().is_empty() || url.password().is_some() {
                    return Err(IcebergFdwError::invalid_option(
                        name,
                        "must not contain URI userinfo credentials",
                    ));
                }
            }
            "rest.auth.type"
                if !value.eq_ignore_ascii_case("none")
                    && !value.eq_ignore_ascii_case("oauth2") =>
            {
                return Err(IcebergFdwError::invalid_option(
                    name,
                    "must be none or oauth2",
                ));
            }
            ENABLE_VENDED_CREDENTIALS | "strict"
                if !value.eq_ignore_ascii_case("true")
                    && !value.eq_ignore_ascii_case("false") =>
            {
                return Err(IcebergFdwError::invalid_option(
                    name,
                    "must be true or false",
                ));
            }
            SCHEMA_ID if !value.parse::<i32>().is_ok_and(|id| id >= 0) => {
                return Err(IcebergFdwError::invalid_option(
                    name,
                    "must be a non-negative 32-bit integer",
                ));
            }
            _ if name.starts_with("header.") => {
                HeaderName::from_bytes(&name.as_bytes()["header.".len()..]).map_err(
                    |_| {
                        IcebergFdwError::invalid_option(
                            name,
                            "has an invalid HTTP header name",
                        )
                    },
                )?;
                HeaderValue::from_str(value).map_err(|_| {
                    IcebergFdwError::invalid_option(
                        name,
                        "has an invalid HTTP header value",
                    )
                })?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn from_view(
        layer: OptionLayer,
        options: ForeignOptionView<'_>,
        require_complete: bool,
    ) -> Result<Self, IcebergFdwError> {
        let mut parsed = Self::default();
        for option in options.iter() {
            let name =
                option
                    .name()
                    .to_str()
                    .map_err(|_| IcebergFdwError::InvalidUtf8 {
                        subject: "foreign option name",
                    })?;
            let value =
                option
                    .value_str()
                    .map_err(|_| IcebergFdwError::InvalidUtf8 {
                        subject: "foreign option value",
                    })?;
            parsed.set(layer, name, value)?;
        }
        if require_complete {
            parsed.validate_required(layer)?;
        }
        Ok(parsed)
    }

    fn set(
        &mut self,
        layer: OptionLayer,
        name: &str,
        value: &str,
    ) -> Result<(), IcebergFdwError> {
        if !layer.accepts(name) {
            return Err(IcebergFdwError::unsupported_option(name));
        }
        if value.is_empty() {
            return Err(IcebergFdwError::invalid_option(name, "must not be empty"));
        }
        if name == "header." {
            return Err(IcebergFdwError::invalid_option(
                name,
                "must include a header name after header.",
            ));
        }
        Self::validate_value(name, value)?;
        let stored_name = if name.starts_with("header.") {
            name.to_ascii_lowercase()
        } else {
            name.to_owned()
        };
        if self.values.insert(stored_name, value.to_owned()).is_some() {
            return Err(IcebergFdwError::invalid_option(
                name,
                "must not be specified more than once",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_required(
        &self,
        layer: OptionLayer,
    ) -> Result<(), IcebergFdwError> {
        let required: &[&str] = match layer {
            OptionLayer::Server => &["uri"],
            OptionLayer::Table => &[TABLE],
            OptionLayer::Wrapper
            | OptionLayer::Mapping
            | OptionLayer::Column
            | OptionLayer::Import => &[],
        };
        for &name in required {
            if !self.values.contains_key(name) {
                return Err(IcebergFdwError::MissingOption { name });
            }
        }
        Ok(())
    }

    pub(super) fn take_required(
        &mut self,
        name: &'static str,
    ) -> Result<String, IcebergFdwError> {
        self.values
            .remove(name)
            .ok_or(IcebergFdwError::MissingOption { name })
    }
}
