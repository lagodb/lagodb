use std::borrow::Cow;
use std::collections::HashMap;

use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_storage::{SecretString, StoreConfig};

use super::routes::ObjectProvider;

mod azure;
mod fallback;
mod gcs;
mod s3;

const S3_CREDENTIAL_EXPIRY: &str = "s3.session-token-expires-at-ms";

const S3_PROPERTIES: &[&str] = &[
    "s3.endpoint",
    "s3.access-key-id",
    "s3.secret-access-key",
    "s3.session-token",
    // Accepted catalog metadata. REST-vended credentials remain static in
    // this adapter; accepting an expiry does not enable automatic renewal.
    S3_CREDENTIAL_EXPIRY,
    "s3.region",
    "s3.path-style-access",
    "s3.allow-anonymous",
    "s3.sse.type",
    "s3.sse.key",
    "s3.sse.md5",
];

const ASSUME_ROLE_PROPERTIES: &[&str] = &[
    "client.assume-role.arn",
    "client.assume-role.external-id",
    "client.assume-role.session-name",
];

const GCS_PROPERTIES: &[&str] = &[
    "gcs.service.host",
    "gcs.credentials-json",
    "gcs.oauth2.token",
    "gcs.no-auth",
    "gcs.allow-anonymous",
];

const AZURE_PROPERTIES: &[&str] = &[
    "adls.account-name",
    "adls.account-key",
    "adls.sas-token",
    "adls.tenant-id",
    "adls.client-id",
    "adls.client-secret",
    "adls.authority-host",
    "adls.endpoint",
    "adls.bearer-token",
];

/// Consumes Iceberg property names into the storage service's provider model.
pub(super) struct ProviderConfig<'a> {
    properties: &'a HashMap<String, String>,
    overrides: Option<&'a HashMap<String, String>>,
    profile: Option<&'a StoreConfig>,
}

impl<'a> ProviderConfig<'a> {
    pub(super) fn with_profile(
        properties: &'a HashMap<String, String>,
        profile: Option<&'a StoreConfig>,
    ) -> Self {
        Self {
            properties,
            overrides: None,
            profile,
        }
    }

    pub(super) fn with_profile_overrides(
        properties: &'a HashMap<String, String>,
        profile: Option<&'a StoreConfig>,
        overrides: &'a HashMap<String, String>,
    ) -> Self {
        Self {
            properties,
            overrides: Some(overrides),
            profile,
        }
    }

    /// Resolves the effective configuration while preserving whether it is the
    /// selected PostgreSQL profile itself or response-owned configuration.
    pub(super) fn resolve(
        &self,
        provider: ObjectProvider,
        account_from_uri: Option<&str>,
    ) -> Result<Cow<'a, StoreConfig>> {
        match provider {
            ObjectProvider::S3 => self.s3(),
            ObjectProvider::Gcs => self.gcs(),
            ObjectProvider::Azure => self.azure(account_from_uri),
        }
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.overrides
            .and_then(|overrides| overrides.get(key))
            .or_else(|| self.properties.get(key))
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }

    fn value(&self, key: &str) -> Option<String> {
        self.get(key).map(str::to_owned)
    }

    fn has(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    fn has_any(&self, keys: &[&str]) -> bool {
        keys.iter().any(|key| self.has(key))
    }

    fn profile_provider_mismatch(&self, provider: &str) -> Error {
        Error::new(
            ErrorKind::DataInvalid,
            format!("selected storage profile is not configured for {provider}"),
        )
    }

    fn secret(&self, key: &str) -> Option<SecretString> {
        self.get(key).map(SecretString::new)
    }

    fn ensure_supported(&self, prefix: &str, supported: &[&str]) -> Result<()> {
        let unsupported = self
            .overrides
            .into_iter()
            .flat_map(HashMap::keys)
            .chain(self.properties.keys())
            .filter(|key| key.starts_with(prefix))
            .filter(|key| self.get(key).is_some())
            .filter(|key| !supported.contains(&key.as_str()))
            .min();
        match unsupported {
            Some(key) => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "Iceberg storage property {key} is not supported by the PostgreSQL storage adapter"
                ),
            )),
            None => Ok(()),
        }
    }

    fn truthy(&self, key: &str) -> bool {
        self.get(key).is_some_and(|value| {
            value.eq_ignore_ascii_case("true")
                || value.eq_ignore_ascii_case("t")
                || value == "1"
                || value.eq_ignore_ascii_case("on")
        })
    }
}
