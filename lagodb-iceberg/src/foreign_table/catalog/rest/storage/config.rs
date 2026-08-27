use std::borrow::Cow;
use std::collections::HashMap;

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_storage::{
    AzureStoreConfig, GcsStoreConfig, S3Encryption, S3StoreConfig, SecretString,
    StoreConfig,
};
use md5::{Digest as _, Md5};

use super::routes::ObjectProvider;

const S3_PROPERTIES: &[&str] = &[
    "s3.endpoint",
    "s3.access-key-id",
    "s3.secret-access-key",
    "s3.session-token",
    "s3.region",
    "s3.path-style-access",
    "s3.allow-anonymous",
    "s3.sse.type",
    "s3.sse.key",
    "s3.sse.md5",
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
    #[cfg(test)]
    pub(super) fn new(properties: &'a HashMap<String, String>) -> Self {
        Self {
            properties,
            overrides: None,
            profile: None,
        }
    }

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

    #[cfg(test)]
    pub(super) fn build(
        self,
        provider: ObjectProvider,
        account_from_uri: Option<&str>,
    ) -> Result<StoreConfig> {
        self.resolve(provider, account_from_uri)
            .map(Cow::into_owned)
    }

    /// Resolves the effective configuration while preserving whether it is the
    /// selected PostgreSQL profile itself or response-owned configuration.
    pub(super) fn resolve(
        self,
        provider: ObjectProvider,
        account_from_uri: Option<&str>,
    ) -> Result<Cow<'a, StoreConfig>> {
        match provider {
            ObjectProvider::S3 => self.s3(),
            ObjectProvider::Gcs => self.gcs(),
            ObjectProvider::Azure => self.azure(account_from_uri),
        }
    }

    fn s3(&self) -> Result<Cow<'a, StoreConfig>> {
        self.ensure_supported("s3.", S3_PROPERTIES)?;
        self.ensure_supported("client.assume-role.", &[])?;
        if self.overrides.is_none()
            && !self.has_any(S3_PROPERTIES)
            && !self.has("client.region")
            && let Some(profile @ (StoreConfig::S3(_) | StoreConfig::S3Compatible(_))) =
                self.profile
        {
            return Ok(Cow::Borrowed(profile));
        }
        let mut config = match self.profile {
            Some(StoreConfig::S3(config)) => config.clone(),
            Some(StoreConfig::S3Compatible(config)) => S3StoreConfig {
                region: config.region.clone(),
                endpoint: Some(config.endpoint.clone()),
                access_key_id: config.access_key_id.clone(),
                secret_access_key: config.secret_access_key.clone(),
                token: config.token.clone(),
                allow_http: config.allow_http,
                virtual_hosted_style_request: config.virtual_hosted_style_request,
                skip_signature: config.skip_signature,
                encryption: config.encryption.clone(),
            },
            Some(_) => return Err(self.profile_provider_mismatch("S3")),
            None => S3StoreConfig::default(),
        };
        if self.has("s3.endpoint") {
            config.endpoint = self.value("s3.endpoint");
            config.allow_http = config
                .endpoint
                .as_deref()
                .is_some_and(|endpoint| endpoint.starts_with("http://"));
        }
        if self.has("s3.path-style-access") {
            config.virtual_hosted_style_request =
                !self.truthy("s3.path-style-access");
        }
        if self.has("s3.allow-anonymous") {
            config.skip_signature = self.truthy("s3.allow-anonymous");
        }
        if [
            "s3.access-key-id",
            "s3.secret-access-key",
            "s3.session-token",
        ]
        .into_iter()
        .any(|key| self.has(key))
        {
            config.access_key_id = self.secret("s3.access-key-id");
            config.secret_access_key = self.secret("s3.secret-access-key");
            config.token = self.secret("s3.session-token");
        }
        if self.has("s3.sse.type") {
            config.encryption = self.s3_encryption()?;
        }
        // Iceberg's generic client region takes precedence over the S3-specific value.
        if self.has("client.region") || self.has("s3.region") {
            config.region = self
                .value("client.region")
                .or_else(|| self.value("s3.region"));
        }

        Ok(Cow::Owned(config.into_canonical()))
    }

    fn gcs(&self) -> Result<Cow<'a, StoreConfig>> {
        self.ensure_supported("gcs.", GCS_PROPERTIES)?;
        if self.overrides.is_none()
            && !self.has_any(GCS_PROPERTIES)
            && let Some(profile @ StoreConfig::Gcs(_)) = self.profile
        {
            return Ok(Cow::Borrowed(profile));
        }
        let mut config = match self.profile {
            Some(StoreConfig::Gcs(config)) => config.clone(),
            Some(_) => return Err(self.profile_provider_mismatch("GCS")),
            None => GcsStoreConfig::default(),
        };
        if self.has("gcs.service.host") {
            config.base_url = self.value("gcs.service.host");
        }
        if self.has("gcs.no-auth") || self.has("gcs.allow-anonymous") {
            config.skip_signature =
                self.truthy("gcs.no-auth") || self.truthy("gcs.allow-anonymous");
        }
        if self.has("gcs.credentials-json") || self.has("gcs.oauth2.token") {
            config.service_account_path = None;
            config.application_credentials_path = None;
            config.service_account_key = self.gcs_credentials_json()?;
            config.bearer_token = self.secret("gcs.oauth2.token");
        }
        Ok(Cow::Owned(StoreConfig::Gcs(config)))
    }

    fn azure(&self, account_from_uri: Option<&str>) -> Result<Cow<'a, StoreConfig>> {
        self.ensure_supported("adls.", AZURE_PROPERTIES)?;
        if self.overrides.is_none()
            && !self.has_any(AZURE_PROPERTIES)
            && let Some(profile @ StoreConfig::Azure(config)) = self.profile
            && (config.account.is_some() || account_from_uri.is_none())
        {
            return Ok(Cow::Borrowed(profile));
        }
        let mut config = match self.profile {
            Some(StoreConfig::Azure(config)) => config.clone(),
            Some(_) => return Err(self.profile_provider_mismatch("Azure")),
            None => AzureStoreConfig::default(),
        };
        if self.has("adls.account-name") {
            config.account = self.value("adls.account-name");
        } else if config.account.is_none() {
            config.account = account_from_uri.map(str::to_owned);
        }
        if self.has("adls.endpoint") {
            config.endpoint = self.value("adls.endpoint");
            config.allow_http = config
                .endpoint
                .as_deref()
                .is_some_and(|endpoint| endpoint.starts_with("http://"));
        }
        if [
            "adls.account-key",
            "adls.bearer-token",
            "adls.sas-token",
            "adls.client-id",
            "adls.client-secret",
            "adls.tenant-id",
        ]
        .into_iter()
        .any(|key| self.has(key))
        {
            config.access_key = self.secret("adls.account-key");
            config.bearer_token = self.secret("adls.bearer-token");
            config.sas_token = self.secret("adls.sas-token");
            config.client_id = self.value("adls.client-id");
            config.client_secret = self.secret("adls.client-secret");
            config.tenant_id = self.value("adls.tenant-id");
        }
        if self.has("adls.authority-host") {
            config.authority_host = self.value("adls.authority-host");
        }
        Ok(Cow::Owned(StoreConfig::Azure(config)))
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

    fn gcs_credentials_json(&self) -> Result<Option<SecretString>> {
        let Some(encoded) = self.get("gcs.credentials-json") else {
            return Ok(None);
        };
        let json = BASE64_STANDARD.decode(encoded).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "Iceberg GCS credentials JSON is not valid Base64",
            )
            .with_source(error)
        })?;
        let json = String::from_utf8(json).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "Iceberg GCS credentials JSON is not UTF-8",
            )
            .with_source(error)
        })?;
        Ok(Some(SecretString::new(json)))
    }

    fn s3_encryption(&self) -> Result<Option<S3Encryption>> {
        let Some(encryption_type) = self.get("s3.sse.type") else {
            return Ok(None);
        };
        match encryption_type.to_ascii_lowercase().as_str() {
            "none" => Ok(None),
            "s3" => Ok(Some(S3Encryption::S3)),
            "kms" => Ok(Some(S3Encryption::Kms {
                key_id: self.value("s3.sse.key"),
            })),
            "custom" => {
                let encoded_key = self.get("s3.sse.key").ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "s3.sse.key is required when s3.sse.type is custom",
                    )
                })?;
                let decoded_key =
                    BASE64_STANDARD.decode(encoded_key).map_err(|error| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "s3.sse.key must be a Base64-encoded customer key",
                        )
                        .with_source(error)
                    })?;
                if let Some(expected_md5) = self.get("s3.sse.md5") {
                    let actual_md5 = BASE64_STANDARD.encode(Md5::digest(decoded_key));
                    if actual_md5 != expected_md5 {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "s3.sse.md5 does not match s3.sse.key",
                        ));
                    }
                }
                Ok(Some(S3Encryption::Custom {
                    key: SecretString::new(encoded_key),
                }))
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "s3.sse.type must be one of none, s3, kms or custom",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lagodb_storage::S3CompatibleStoreConfig;

    #[test]
    fn client_region_overrides_s3_region() {
        let properties = HashMap::from([
            ("s3.region".to_owned(), "catalog-region".to_owned()),
            ("client.region".to_owned(), "client-region".to_owned()),
        ]);
        let config = ProviderConfig::new(&properties)
            .build(ObjectProvider::S3, None)
            .unwrap();
        let StoreConfig::S3(config) = config else {
            panic!("an S3 URI without a custom endpoint must build S3 config");
        };
        assert_eq!(config.region.as_deref(), Some("client-region"));
    }

    #[test]
    fn gcs_credentials_json_is_decoded_at_the_adapter_boundary() {
        let encoded =
            BASE64_STANDARD.encode(r#"{"client_email":"test@example.com"}"#);
        let properties =
            HashMap::from([("gcs.credentials-json".to_owned(), encoded)]);
        let config = ProviderConfig::new(&properties)
            .build(ObjectProvider::Gcs, None)
            .unwrap();
        let StoreConfig::Gcs(config) = config else {
            panic!("GCS properties must build GCS config");
        };
        assert_eq!(
            config
                .service_account_key
                .as_ref()
                .map(SecretString::expose_secret),
            Some(r#"{"client_email":"test@example.com"}"#)
        );
    }

    #[test]
    fn s3_kms_encryption_is_mapped_to_storage_config() {
        let properties = HashMap::from([
            ("s3.sse.type".to_owned(), "kms".to_owned()),
            ("s3.sse.key".to_owned(), "kms-key".to_owned()),
        ]);
        let config = ProviderConfig::new(&properties)
            .build(ObjectProvider::S3, None)
            .unwrap();
        let StoreConfig::S3(config) = config else {
            panic!("an S3 URI without a custom endpoint must build S3 config");
        };
        assert_eq!(
            config.encryption,
            Some(S3Encryption::Kms {
                key_id: Some("kms-key".to_owned()),
            })
        );
    }

    #[test]
    fn s3_customer_encryption_rejects_a_mismatched_md5() {
        let properties = HashMap::from([
            ("s3.sse.type".to_owned(), "custom".to_owned()),
            (
                "s3.sse.key".to_owned(),
                BASE64_STANDARD.encode(b"01234567890123456789012345678901"),
            ),
            ("s3.sse.md5".to_owned(), "incorrect-md5".to_owned()),
        ]);

        let error = ProviderConfig::new(&properties)
            .build(ObjectProvider::S3, None)
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.to_string().contains("s3.sse.md5"));
    }

    #[test]
    fn azure_authority_host_is_mapped_to_storage_config() {
        let properties = HashMap::from([(
            "adls.authority-host".to_owned(),
            "https://login.microsoftonline.com".to_owned(),
        )]);
        let config = ProviderConfig::new(&properties)
            .build(ObjectProvider::Azure, Some("account"))
            .unwrap();
        let StoreConfig::Azure(config) = config else {
            panic!("Azure properties must build Azure config");
        };
        assert_eq!(
            config.authority_host.as_deref(),
            Some("https://login.microsoftonline.com")
        );
    }

    #[test]
    fn unsupported_assume_role_is_not_silently_ignored() {
        let properties = HashMap::from([(
            "client.assume-role.arn".to_owned(),
            "arn:aws:iam::123456789012:role/catalog".to_owned(),
        )]);
        let error = ProviderConfig::new(&properties)
            .build(ObjectProvider::S3, None)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn catalog_properties_override_the_selected_profile() {
        let profile = StoreConfig::S3Compatible(S3CompatibleStoreConfig {
            endpoint: "http://profile.example".to_owned(),
            region: Some("profile-region".to_owned()),
            access_key_id: Some(SecretString::new("profile-key")),
            secret_access_key: Some(SecretString::new("profile-secret")),
            token: None,
            allow_http: true,
            virtual_hosted_style_request: false,
            skip_signature: false,
            encryption: None,
        });
        let properties = HashMap::from([
            (
                "s3.endpoint".to_owned(),
                "https://catalog.example".to_owned(),
            ),
            ("s3.region".to_owned(), "catalog-region".to_owned()),
            ("s3.access-key-id".to_owned(), "catalog-key".to_owned()),
            (
                "s3.secret-access-key".to_owned(),
                "catalog-secret".to_owned(),
            ),
        ]);

        let StoreConfig::S3Compatible(config) =
            ProviderConfig::with_profile(&properties, Some(&profile))
                .build(ObjectProvider::S3, None)
                .unwrap()
        else {
            panic!("catalog endpoint must retain the S3-compatible variant");
        };
        assert_eq!(config.endpoint, "https://catalog.example");
        assert_eq!(config.region.as_deref(), Some("catalog-region"));
        assert_eq!(
            config
                .access_key_id
                .as_ref()
                .map(SecretString::expose_secret),
            Some("catalog-key")
        );
    }

    #[test]
    fn scoped_credentials_override_catalog_credentials_only() {
        let profile = StoreConfig::S3Compatible(S3CompatibleStoreConfig {
            endpoint: "http://profile.example".to_owned(),
            region: Some("profile-region".to_owned()),
            access_key_id: None,
            secret_access_key: None,
            token: None,
            allow_http: true,
            virtual_hosted_style_request: false,
            skip_signature: false,
            encryption: None,
        });
        let properties = HashMap::from([
            ("s3.access-key-id".to_owned(), "catalog-key".to_owned()),
            (
                "s3.secret-access-key".to_owned(),
                "catalog-secret".to_owned(),
            ),
        ]);
        let credential = HashMap::from([
            ("s3.access-key-id".to_owned(), "vended-key".to_owned()),
            (
                "s3.secret-access-key".to_owned(),
                "vended-secret".to_owned(),
            ),
        ]);

        let StoreConfig::S3Compatible(config) =
            ProviderConfig::with_profile_overrides(
                &properties,
                Some(&profile),
                &credential,
            )
            .build(ObjectProvider::S3, None)
            .unwrap()
        else {
            panic!("profile endpoint must retain the S3-compatible variant");
        };
        assert_eq!(config.endpoint, "http://profile.example");
        assert_eq!(
            config
                .access_key_id
                .as_ref()
                .map(SecretString::expose_secret),
            Some("vended-key")
        );
        assert_eq!(
            config
                .secret_access_key
                .as_ref()
                .map(SecretString::expose_secret),
            Some("vended-secret")
        );
    }

    #[test]
    fn gcs_profile_keeps_connector_only_credential_sources() {
        let profile = StoreConfig::Gcs(GcsStoreConfig {
            service_account_path: Some(
                "/credentials/service-account.json".to_owned(),
            ),
            ..GcsStoreConfig::default()
        });
        let properties = HashMap::new();

        let StoreConfig::Gcs(config) =
            ProviderConfig::with_profile(&properties, Some(&profile))
                .build(ObjectProvider::Gcs, None)
                .unwrap()
        else {
            panic!("GCS profile must retain its provider");
        };
        assert_eq!(
            config.service_account_path.as_deref(),
            Some("/credentials/service-account.json")
        );
    }
}
