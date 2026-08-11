//! Strict object-location URI and server-scope parsing.

use std::fmt::{self, Display, Formatter};

use url::Url;

use crate::error::ConnectorError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageProvider {
    S3,
    S3Compatible,
    Gcs,
    Azure,
}

impl StorageProvider {
    pub(crate) fn parse(value: &str) -> Result<Self, ConnectorError> {
        match value {
            "s3" => Ok(Self::S3),
            "s3_compatible" => Ok(Self::S3Compatible),
            "gcs" => Ok(Self::Gcs),
            "azure" => Ok(Self::Azure),
            _ => Err(ConnectorError::invalid_option(
                "provider",
                "must be one of s3, s3_compatible, gcs, or azure",
            )),
        }
    }

    pub(crate) fn matches_scheme(self, scheme: ObjectScheme) -> bool {
        match self {
            Self::S3 | Self::S3Compatible => scheme == ObjectScheme::S3,
            Self::Gcs => scheme == ObjectScheme::Gcs,
            Self::Azure => scheme == ObjectScheme::Azure,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectScheme {
    S3,
    Gcs,
    Azure,
}

impl ObjectScheme {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "s3" => Some(Self::S3),
            "gs" => Some(Self::Gcs),
            "az" => Some(Self::Azure),
            _ => None,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gs",
            Self::Azure => "az",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ObjectUri {
    scheme: ObjectScheme,
    bucket: Box<str>,
    key: Box<str>,
}

impl ObjectUri {
    pub(crate) fn is_supported_prefix(value: &[u8]) -> bool {
        value
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"s3://"))
            || value
                .get(..5)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"gs://"))
            || value
                .get(..5)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"az://"))
    }

    pub(crate) fn parse(value: &str) -> Result<Self, ConnectorError> {
        let parsed = Url::parse(value)
            .map_err(|_| ConnectorError::invalid_object_uri("is not a valid URI"))?;
        let scheme = ObjectScheme::parse(parsed.scheme()).ok_or_else(|| {
            ConnectorError::invalid_object_uri(
                "uses an unsupported object URI scheme",
            )
        })?;
        if has_userinfo(value)
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.port().is_some()
        {
            return Err(ConnectorError::invalid_object_uri(
                "must not contain userinfo, port, query, or fragment",
            ));
        }
        if contains_encoded_separator(value) {
            return Err(ConnectorError::invalid_object_uri(
                "must not contain an encoded path separator",
            ));
        }
        let bucket = parsed
            .host_str()
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| {
                ConnectorError::invalid_object_uri("bucket/container is required")
            })?;
        // `Url::path()` is a normalized URL path. Object keys are opaque: a
        // literal `.` or `..` segment is part of the key and must not be
        // collapsed before scope checking or backend access.
        let key = decode_path(raw_path(value))?;
        let key = key
            .strip_prefix('/')
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                ConnectorError::invalid_object_uri("object key is required")
            })?;
        reject_glob_characters(bucket)?;
        reject_glob_characters(key)?;
        Ok(Self {
            scheme,
            bucket: bucket.into(),
            key: key.into(),
        })
    }

    pub(crate) fn scheme(&self) -> ObjectScheme {
        self.scheme
    }

    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn default_server_name(&self) -> &'static str {
        match self.scheme {
            ObjectScheme::S3 => "pg_lakebase_s3",
            ObjectScheme::Gcs => "pg_lakebase_gcs",
            ObjectScheme::Azure => "pg_lakebase_azure",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageScope {
    scheme: ObjectScheme,
    bucket: Box<str>,
    prefix: Option<Box<str>>,
}

impl StorageScope {
    pub(crate) fn parse(
        value: &str,
        provider: StorageProvider,
    ) -> Result<Self, ConnectorError> {
        let parsed = Url::parse(value).map_err(|_| {
            ConnectorError::invalid_option("scope", "is not a valid URI")
        })?;
        let scheme = ObjectScheme::parse(parsed.scheme()).ok_or_else(|| {
            ConnectorError::invalid_option("scope", "uses an unsupported URI scheme")
        })?;
        if !provider.matches_scheme(scheme)
            || has_userinfo(value)
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.port().is_some()
            || contains_encoded_separator(value)
        {
            return Err(ConnectorError::invalid_option(
                "scope",
                "has an invalid provider, credential, port, query, fragment, or encoded separator",
            ));
        }
        let bucket = parsed
            .host_str()
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| {
                ConnectorError::invalid_option(
                    "scope",
                    "bucket/container is required",
                )
            })?;
        // Keep the raw path for the same reason as ObjectUri::parse: scope
        // prefixes are opaque object-key prefixes, not filesystem paths.
        let path = decode_scope_path(raw_path(value))?;
        let path = path.strip_prefix('/').unwrap_or(path.as_str());
        reject_scope_glob_characters(bucket)?;
        reject_scope_glob_characters(path)?;
        let prefix = if path.is_empty() {
            None
        } else {
            if !path.ends_with('/') {
                return Err(ConnectorError::invalid_option(
                    "scope",
                    "a key prefix must end with '/'",
                ));
            }
            Some(path.into())
        };
        Ok(Self {
            scheme,
            bucket: bucket.into(),
            prefix,
        })
    }

    pub(crate) fn contains(&self, object: &ObjectUri) -> bool {
        self.scheme == object.scheme
            && self.bucket == object.bucket
            && self
                .prefix
                .as_deref()
                .is_none_or(|prefix| object.key.starts_with(prefix))
    }
}

impl Display for ObjectScheme {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn contains_encoded_separator(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.windows(3).any(|window| {
        window[0] == b'%'
            && ((window[1] == b'2' && matches!(window[2], b'F' | b'f'))
                || (window[1] == b'5' && matches!(window[2], b'C' | b'c')))
    })
}

fn has_userinfo(value: &str) -> bool {
    let Some(scheme_end) = value.find("://") else {
        return false;
    };
    let authority = &value[(scheme_end + 3)..];
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    authority[..authority_end].contains('@')
}

fn raw_path(value: &str) -> &str {
    let authority_start = value.find("://").map_or(0, |scheme_end| scheme_end + 3);
    let remainder = &value[authority_start..];
    let path_start = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    &remainder[path_start..]
}

fn decode_path(path: &str) -> Result<String, ConnectorError> {
    percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| ConnectorError::invalid_object_uri("path is not valid UTF-8"))
}

fn decode_scope_path(path: &str) -> Result<String, ConnectorError> {
    percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| {
            ConnectorError::invalid_option("scope", "path must be valid UTF-8")
        })
}

fn reject_scope_glob_characters(value: &str) -> Result<(), ConnectorError> {
    if value
        .chars()
        .any(|character| matches!(character, '*' | '?' | '[' | ']' | '{' | '}'))
    {
        return Err(ConnectorError::invalid_option(
            "scope",
            "wildcards are not valid object-scope characters",
        ));
    }
    Ok(())
}

fn reject_glob_characters(value: &str) -> Result<(), ConnectorError> {
    if value
        .chars()
        .any(|character| matches!(character, '*' | '?' | '[' | ']' | '{' | '}'))
    {
        return Err(ConnectorError::invalid_object_uri(
            "wildcards are not valid object-location URI characters",
        ));
    }
    Ok(())
}
