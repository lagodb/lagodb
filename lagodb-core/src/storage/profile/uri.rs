//! Strict object-location URI and server-scope parsing.

use std::fmt::{self, Display, Formatter};
use std::str;

use url::Url;

use super::error::StorageProfileError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageProvider {
    S3,
    S3Compatible,
    Gcs,
    Azure,
}

impl StorageProvider {
    pub fn parse(value: &str) -> Result<Self, StorageProfileError> {
        match value {
            "s3" => Ok(Self::S3),
            "s3_compatible" => Ok(Self::S3Compatible),
            "gcs" => Ok(Self::Gcs),
            "azure" => Ok(Self::Azure),
            _ => Err(StorageProfileError::invalid_option(
                "provider",
                "must be one of s3, s3_compatible, gcs, or azure",
            )),
        }
    }

    pub fn matches_scheme(self, scheme: ObjectScheme) -> bool {
        match self {
            Self::S3 | Self::S3Compatible => scheme == ObjectScheme::S3,
            Self::Gcs => scheme == ObjectScheme::Gcs,
            Self::Azure => scheme == ObjectScheme::Azure,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectScheme {
    S3,
    Gcs,
    Azure,
}

impl ObjectScheme {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "s3" | "s3a" | "s3n" => Some(Self::S3),
            "gs" | "gcs" => Some(Self::Gcs),
            "abfs" | "abfss" | "wasb" | "wasbs" | "az" | "azure" => Some(Self::Azure),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gs",
            Self::Azure => "az",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectUri {
    scheme: ObjectScheme,
    bucket: Box<str>,
    account: Option<Box<str>>,
    key: Box<str>,
}

impl ObjectUri {
    pub fn is_supported_prefix(value: &[u8]) -> bool {
        let Some(scheme_end) = value.windows(3).position(|part| part == b"://")
        else {
            return false;
        };
        str::from_utf8(&value[..scheme_end])
            .ok()
            .and_then(|scheme| ObjectScheme::parse(&scheme.to_ascii_lowercase()))
            .is_some()
    }

    pub fn parse(value: &str) -> Result<Self, StorageProfileError> {
        let parsed = Url::parse(value).map_err(|_| {
            StorageProfileError::invalid_object_uri("is not a valid URI")
        })?;
        let scheme = ObjectScheme::parse(parsed.scheme()).ok_or_else(|| {
            StorageProfileError::invalid_object_uri(
                "uses an unsupported object URI scheme",
            )
        })?;
        let azure_userinfo = scheme == ObjectScheme::Azure
            && !parsed.username().is_empty()
            && parsed.password().is_none();
        if (has_userinfo(value) && !azure_userinfo)
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.port().is_some()
        {
            return Err(StorageProfileError::invalid_object_uri(
                "must not contain userinfo, port, query, or fragment",
            ));
        }
        if contains_encoded_separator(value) {
            return Err(StorageProfileError::invalid_object_uri(
                "must not contain an encoded path separator",
            ));
        }
        let host = parsed
            .host_str()
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| {
                StorageProfileError::invalid_object_uri(
                    "bucket/container is required",
                )
            })?;
        let (bucket, account) = azure_namespace(&parsed, scheme, host)?;
        // `Url::path()` is a normalized URL path. Object keys are opaque: a
        // literal `.` or `..` segment is part of the key and must not be
        // collapsed before scope checking or backend access.
        let key = decode_path(raw_path(value))?;
        let key = key
            .strip_prefix('/')
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                StorageProfileError::invalid_object_uri("object key is required")
            })?;
        reject_glob_characters(bucket)?;
        reject_glob_characters(key)?;
        Ok(Self {
            scheme,
            bucket: bucket.into(),
            account: account.map(Into::into),
            key: key.into(),
        })
    }

    pub fn scheme(&self) -> ObjectScheme {
        self.scheme
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn account(&self) -> Option<&str> {
        self.account.as_deref()
    }

    pub fn key(&self) -> &str {
        &self.key
    }
}

/// A normalized credential or routing prefix for object locations.
///
/// Unlike [`StorageScope`], an Iceberg REST credential prefix is not required
/// to end at a path-segment boundary. Containment therefore follows the REST
/// contract's key-prefix semantics after URI normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectUriPrefix {
    scheme: ObjectScheme,
    bucket: Box<str>,
    account: Option<Box<str>>,
    key_prefix: Option<Box<str>>,
}

impl ObjectUriPrefix {
    pub fn parse(value: &str) -> Result<Self, StorageProfileError> {
        let parsed = Url::parse(value).map_err(|_| {
            StorageProfileError::invalid_object_uri("is not a valid URI")
        })?;
        let scheme = ObjectScheme::parse(parsed.scheme()).ok_or_else(|| {
            StorageProfileError::invalid_object_uri(
                "uses an unsupported object URI scheme",
            )
        })?;
        let azure_userinfo = scheme == ObjectScheme::Azure
            && !parsed.username().is_empty()
            && parsed.password().is_none();
        if (has_userinfo(value) && !azure_userinfo)
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.port().is_some()
            || contains_encoded_separator(value)
        {
            return Err(StorageProfileError::invalid_object_uri(
                "must not contain unsupported userinfo, port, query, fragment, or encoded separator",
            ));
        }
        let host = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| {
                StorageProfileError::invalid_object_uri(
                    "bucket/container is required",
                )
            })?;
        let (bucket, account) = azure_namespace(&parsed, scheme, host)?;
        let path = decode_path(raw_path(value))?;
        let path = path.strip_prefix('/').unwrap_or(path.as_str());
        reject_glob_characters(bucket)?;
        reject_glob_characters(path)?;
        Ok(Self {
            scheme,
            bucket: bucket.into(),
            account: account.map(Into::into),
            key_prefix: (!path.is_empty()).then(|| path.into()),
        })
    }

    pub fn scheme(&self) -> ObjectScheme {
        self.scheme
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn account(&self) -> Option<&str> {
        self.account.as_deref()
    }

    pub fn key_prefix(&self) -> Option<&str> {
        self.key_prefix.as_deref()
    }

    pub fn contains(&self, object: &ObjectUri) -> bool {
        self.scheme == object.scheme
            && self.bucket == object.bucket
            && self.account == object.account
            && self
                .key_prefix
                .as_deref()
                .is_none_or(|prefix| object.key.starts_with(prefix))
    }

    pub fn specificity(&self) -> usize {
        self.key_prefix.as_deref().map_or(0, str::len)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageScope {
    scheme: ObjectScheme,
    bucket: Box<str>,
    account: Option<Box<str>>,
    prefix: Option<Box<str>>,
}

impl StorageScope {
    pub fn parse(
        value: &str,
        provider: StorageProvider,
        configured_account: Option<&str>,
    ) -> Result<Self, StorageProfileError> {
        let mut prefix = ObjectUriPrefix::parse(value).map_err(|_| {
            StorageProfileError::invalid_option(
                "scope",
                "has an invalid provider, credential, port, query, fragment, or encoded separator",
            )
        })?;
        if !provider.matches_scheme(prefix.scheme) {
            return Err(StorageProfileError::invalid_option(
                "scope",
                "does not match the configured provider",
            ));
        }
        if provider == StorageProvider::Azure {
            match (prefix.account.as_deref(), configured_account) {
                (Some(uri_account), Some(account)) if uri_account != account => {
                    return Err(StorageProfileError::invalid_option(
                        "scope",
                        "Azure authority account conflicts with the configured account",
                    ));
                }
                (None, Some(account)) => prefix.account = Some(account.into()),
                _ => {}
            }
        }
        if prefix
            .key_prefix
            .as_deref()
            .is_some_and(|path| !path.ends_with('/'))
        {
            return Err(StorageProfileError::invalid_option(
                "scope",
                "a key prefix must end with '/'",
            ));
        }
        Ok(Self {
            scheme: prefix.scheme,
            bucket: prefix.bucket,
            account: prefix.account,
            prefix: prefix.key_prefix,
        })
    }

    pub fn contains(&self, object: &ObjectUri) -> bool {
        self.scheme == object.scheme
            && self.bucket == object.bucket
            && object
                .account
                .as_deref()
                .is_none_or(|account| self.account.as_deref() == Some(account))
            && self
                .prefix
                .as_deref()
                .is_none_or(|prefix| object.key.starts_with(prefix))
    }

    pub fn specificity(&self) -> usize {
        self.prefix.as_deref().map_or(0, str::len)
    }
}

fn azure_namespace<'a>(
    parsed: &'a Url,
    scheme: ObjectScheme,
    host: &'a str,
) -> Result<(&'a str, Option<&'a str>), StorageProfileError> {
    if scheme != ObjectScheme::Azure || parsed.username().is_empty() {
        return Ok((host, None));
    }
    let account = host.split('.').next().filter(|value| !value.is_empty());
    let account = account.ok_or_else(|| {
        StorageProfileError::invalid_object_uri("Azure account is required")
    })?;
    Ok((parsed.username(), Some(account)))
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

fn decode_path(path: &str) -> Result<String, StorageProfileError> {
    percent_encoding::percent_decode_str(path)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| {
            StorageProfileError::invalid_object_uri("path is not valid UTF-8")
        })
}

fn reject_glob_characters(value: &str) -> Result<(), StorageProfileError> {
    if value
        .chars()
        .any(|character| matches!(character, '*' | '?' | '[' | ']' | '{' | '}'))
    {
        return Err(StorageProfileError::invalid_object_uri(
            "wildcards are not valid object-location URI characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_share_canonical_provider_and_decoded_key() {
        let s3 = ObjectUri::parse("s3a://bucket/a%20b.parquet").unwrap();
        let gcs = ObjectUri::parse("gcs://bucket/a%25b.parquet").unwrap();

        assert_eq!(s3.scheme(), ObjectScheme::S3);
        assert_eq!(s3.bucket(), "bucket");
        assert_eq!(s3.key(), "a b.parquet");
        assert_eq!(gcs.scheme(), ObjectScheme::Gcs);
        assert_eq!(gcs.key(), "a%b.parquet");
    }

    #[test]
    fn azure_authority_separates_container_and_account() {
        let object = ObjectUri::parse(
            "abfss://container@account.dfs.core.windows.net/path/file.parquet",
        )
        .unwrap();

        assert_eq!(object.scheme(), ObjectScheme::Azure);
        assert_eq!(object.bucket(), "container");
        assert_eq!(object.account(), Some("account"));
        assert_eq!(object.key(), "path/file.parquet");
    }

    #[test]
    fn credential_prefix_uses_normalized_containment() {
        let prefix = ObjectUriPrefix::parse("s3://bucket/a%20b/").unwrap();
        let matching = ObjectUri::parse("s3n://bucket/a%20b/file.parquet").unwrap();
        let encoded_literal =
            ObjectUri::parse("s3://bucket/a%2520b/file.parquet").unwrap();

        assert!(prefix.contains(&matching));
        assert!(!prefix.contains(&encoded_literal));
    }

    #[test]
    fn azure_prefix_requires_same_account() {
        let prefix = ObjectUriPrefix::parse(
            "abfs://container@account-a.dfs.core.windows.net/path/",
        )
        .unwrap();
        let other = ObjectUri::parse(
            "abfs://container@account-b.dfs.core.windows.net/path/file.parquet",
        )
        .unwrap();

        assert!(!prefix.contains(&other));
    }

    #[test]
    fn azure_scope_binds_unqualified_uri_to_configured_account() {
        let scope = StorageScope::parse(
            "az://container/path/",
            StorageProvider::Azure,
            Some("account-a"),
        )
        .unwrap();
        let qualified = ObjectUri::parse(
            "abfss://container@account-a.dfs.core.windows.net/path/file.parquet",
        )
        .unwrap();
        let unqualified =
            ObjectUri::parse("azure://container/path/file.parquet").unwrap();
        let other = ObjectUri::parse(
            "abfs://container@account-b.dfs.core.windows.net/path/file.parquet",
        )
        .unwrap();

        assert!(scope.contains(&qualified));
        assert!(scope.contains(&unqualified));
        assert!(!scope.contains(&other));
    }

    #[test]
    fn azure_scope_rejects_conflicting_authority_and_config() {
        assert!(
            StorageScope::parse(
                "abfs://container@account-a.dfs.core.windows.net/path/",
                StorageProvider::Azure,
                Some("account-b"),
            )
            .is_err()
        );
    }

    #[test]
    fn encoded_separator_is_rejected_before_decoding() {
        assert!(ObjectUri::parse("s3://bucket/a%2Fb.parquet").is_err());
        assert!(ObjectUriPrefix::parse("s3://bucket/a%5Cb/").is_err());
    }
}
