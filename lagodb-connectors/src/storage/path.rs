//! Connector locations keep local paths separate from object-store URIs.

use crate::error::ConnectorError;

use super::ObjectUri;

pub(crate) enum StoragePath {
    Object(ObjectUri),
    Local(Box<str>),
}

impl StoragePath {
    pub(crate) fn parse(value: &str) -> Result<Self, ConnectorError> {
        if ObjectUri::has_uri_scheme(value.as_bytes()) {
            return Ok(Self::Object(ObjectUri::parse(value)?));
        }
        if value.is_empty() || value.contains('\0') {
            return Err(ConnectorError::invalid_option(
                "path",
                "local paths must be nonempty and contain no NUL byte",
            ));
        }
        Ok(Self::Local(value.into()))
    }

    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Object(object) => object.key(),
            Self::Local(path) => path,
        }
    }
}
