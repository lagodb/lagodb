use std::collections::HashMap;

use http::header::{HeaderName, HeaderValue};
use pg_lakebase_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;
use url::Url;

use super::super::error::IcebergFdwError;

pub(super) const CATALOG_NAME: &str = "catalog_name";
pub(super) const CATALOG_NAMESPACE: &str = "catalog_namespace";
pub(super) const CATALOG_TABLE_NAME: &str = "catalog_table_name";
pub(super) const MODE: &str = "mode";
pub(super) const READ_ONLY: &str = "read_only";
pub(super) const READ_WRITE: &str = "read_write";
pub(super) const ENABLE_VENDED_CREDENTIALS: &str = "enable_vended_credentials";

#[derive(Clone, Copy)]
pub(super) enum OptionLayer {
    Wrapper,
    Server,
    Mapping,
    Table,
    Import,
}

impl OptionLayer {
    pub(super) fn from_catalog(catalog: Option<pg_sys::Oid>) -> Self {
        match catalog {
            Some(oid) if oid == pg_sys::ForeignDataWrapperRelationId => Self::Wrapper,
            Some(oid) if oid == pg_sys::ForeignServerRelationId => Self::Server,
            Some(oid) if oid == pg_sys::UserMappingRelationId => Self::Mapping,
            Some(oid) if oid == pg_sys::ForeignTableRelationId => Self::Table,
            _ => Self::Import,
        }
    }

    pub(super) fn accepts(self, name: &str) -> bool {
        match self {
            Self::Wrapper | Self::Import => false,
            Self::Server => matches!(
                name,
                "uri"
                    | "warehouse"
                    | "prefix"
                    | "rest.auth.type"
                    | "oauth2-server-uri"
                    | "scope"
                    | "audience"
                    | "resource"
                    | ENABLE_VENDED_CREDENTIALS
            ),
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
            Self::Table => matches!(
                name,
                "catalog_name" | "catalog_namespace" | "catalog_table_name" | "mode"
            ),
        }
    }

    pub(super) fn validate(
        self,
        options: &[Option<String>],
    ) -> Result<(), IcebergFdwError> {
        let mut parsed = ParsedOptions::default();
        for option in options.iter().flatten() {
            let (name, value) = option.split_once('=').ok_or_else(|| {
                IcebergFdwError::invalid_option(
                    "foreign option",
                    "expected name=value",
                )
            })?;
            parsed.set(self, name, value)?;
        }
        parsed.validate_required(self)
    }
}

#[derive(Default)]
pub(super) struct ParsedOptions {
    pub(super) values: HashMap<String, String>,
}

impl ParsedOptions {
    fn validate_value(name: &str, value: &str) -> Result<(), IcebergFdwError> {
        match name {
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
            ENABLE_VENDED_CREDENTIALS
                if !value.eq_ignore_ascii_case("true")
                    && !value.eq_ignore_ascii_case("false") =>
            {
                return Err(IcebergFdwError::invalid_option(
                    name,
                    "must be true or false",
                ));
            }
            MODE if !matches!(value, READ_ONLY | READ_WRITE) => {
                return Err(IcebergFdwError::invalid_option(
                    name,
                    "must be read_only or read_write",
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
            OptionLayer::Table => {
                &[CATALOG_NAME, CATALOG_NAMESPACE, CATALOG_TABLE_NAME, MODE]
            }
            OptionLayer::Wrapper | OptionLayer::Mapping | OptionLayer::Import => &[],
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
