//! Provider-specific [`object_store`] configuration and validation facade.
//!
//! [`StoreConfig`] is the user-facing enum that wraps one of the provider configs. It owns both
//! the validation rules exercised when attaching a context and the actual builder invocations that
//! hand back an [`ObjectStore`] client for a given bucket.
//!
use std::sync::Arc;

use object_store::{ObjectStore, RetryConfig};

use super::secret::SecretString;
use crate::error::{StorageError, StorageResult};

mod azure;
mod gcs;
mod s3;

/// Credentials and transport tweaks for native AWS S3.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct S3StoreConfig {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<SecretString>,
    pub secret_access_key: Option<SecretString>,
    pub token: Option<SecretString>,
    pub allow_http: bool,
    pub virtual_hosted_style_request: bool,
    pub skip_signature: bool,
}

/// Same surface as [`S3StoreConfig`] but with a mandatory custom endpoint (MinIO, Ceph, R2…).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3CompatibleStoreConfig {
    pub endpoint: String,
    pub region: Option<String>,
    pub access_key_id: Option<SecretString>,
    pub secret_access_key: Option<SecretString>,
    pub token: Option<SecretString>,
    pub allow_http: bool,
    pub virtual_hosted_style_request: bool,
    pub skip_signature: bool,
}

impl S3StoreConfig {
    /// Converts the S3 configuration into the canonical backend variant.
    ///
    /// An endpoint selects the custom-endpoint builder regardless of which
    /// catalog-facing adapter supplied the configuration. Keeping that rule
    /// here makes a storage volume and a ForeignServer address the same
    /// physical service with the same [`StoreConfig`] variant and backend
    /// identity.
    pub fn into_canonical(self) -> StoreConfig {
        let Self {
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            token,
            allow_http,
            virtual_hosted_style_request,
            skip_signature,
        } = self;
        match endpoint {
            Some(endpoint) => StoreConfig::S3Compatible(S3CompatibleStoreConfig {
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                token,
                allow_http,
                virtual_hosted_style_request,
                skip_signature,
            }),
            None => StoreConfig::S3(S3StoreConfig {
                region,
                endpoint: None,
                access_key_id,
                secret_access_key,
                token,
                allow_http,
                virtual_hosted_style_request,
                skip_signature,
            }),
        }
    }
}

/// Google Cloud Storage credentials. At most one credential source may be set.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcsStoreConfig {
    pub base_url: Option<String>,
    pub service_account_path: Option<String>,
    pub service_account_key: Option<SecretString>,
    pub application_credentials_path: Option<String>,
    pub skip_signature: bool,
}

/// Azure Blob Storage credentials. Client-secret auth requires all three of
/// `client_id`/`client_secret`/`tenant_id` together.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AzureStoreConfig {
    pub account: Option<String>,
    pub endpoint: Option<String>,
    pub access_key: Option<SecretString>,
    pub bearer_token: Option<SecretString>,
    pub client_id: Option<String>,
    pub client_secret: Option<SecretString>,
    pub tenant_id: Option<String>,
    pub allow_http: bool,
    pub use_emulator: bool,
}

/// Top-level config enum describing which provider to instantiate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreConfig {
    S3(S3StoreConfig),
    S3Compatible(S3CompatibleStoreConfig),
    Gcs(GcsStoreConfig),
    Azure(AzureStoreConfig),
}

impl StoreConfig {
    /// Validate config invariants (non-empty endpoints, mutually exclusive credential sources…).
    pub fn validate(&self) -> StorageResult<()> {
        match self {
            Self::S3(config) => config.validate(),
            Self::S3Compatible(config) => config.validate(),
            Self::Gcs(config) => config.validate(),
            Self::Azure(config) => config.validate(),
        }
    }

    /// Validate that this config can construct a local client for `bucket`.
    ///
    /// This performs builder parsing and local credential decoding only. It
    /// does not contact the object-store service or validate remote access.
    pub fn validate_for_bucket(&self, bucket: &str) -> StorageResult<()> {
        self.validate()?;
        let _ = self.build_store(bucket)?;
        Ok(())
    }

    /// Instantiate an [`ObjectStore`] client for `bucket` using this provider config.
    pub(super) fn build_store(
        &self,
        bucket: &str,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        self.build_store_with_retry(bucket, RetryConfig::default())
    }

    /// Builds the client used by caller-controlled staging uploads.
    ///
    /// The database chooses whether to retry a failed command. Disable hidden
    /// SDK retries so one protocol Upload request performs one upload attempt
    /// and returns its ordinary success or failure result to that caller.
    pub(super) fn build_upload_store(
        &self,
        bucket: &str,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        self.build_store_with_retry(bucket, retry)
    }

    fn build_store_with_retry(
        &self,
        bucket: &str,
        retry_config: RetryConfig,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        match self {
            Self::S3(config) => config.build_store(bucket, retry_config),
            Self::S3Compatible(config) => config.build_store(bucket, retry_config),
            Self::Gcs(config) => config.build_store(bucket, retry_config),
            Self::Azure(config) => config.build_store(bucket, retry_config),
        }
    }
}

pub(super) fn validate_non_empty(name: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        return Err(StorageError::configuration(format!(
            "{name} must not be empty"
        )));
    }
    Ok(())
}

pub(super) fn validate_optional_non_empty(
    name: &str,
    value: Option<&str>,
) -> StorageResult<()> {
    if let Some(value) = value {
        validate_non_empty(name, value)?;
    }
    Ok(())
}

pub(super) fn validate_endpoint(
    name: &str,
    value: &str,
    allow_http: bool,
) -> StorageResult<()> {
    validate_non_empty(name, value)?;
    let parsed = url::Url::parse(value).map_err(|error| {
        StorageError::configuration(format!("{name} is not a valid URL: {error}"))
    })?;
    match parsed.scheme() {
        "https" => {}
        "http" if allow_http => {}
        "http" => {
            return Err(StorageError::configuration(format!(
                "{name} uses HTTP but allow_http is false"
            )));
        }
        scheme => {
            return Err(StorageError::configuration(format!(
                "{name} uses unsupported URL scheme {scheme}"
            )));
        }
    }
    if parsed.host_str().is_none() {
        return Err(StorageError::configuration(format!(
            "{name} must include a host"
        )));
    }
    if endpoint_has_userinfo(value) || parsed.password().is_some() {
        return Err(StorageError::configuration(format!(
            "{name} must not contain URL user credentials"
        )));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(StorageError::configuration(format!(
            "{name} must not contain a query or fragment"
        )));
    }
    Ok(())
}

fn endpoint_has_userinfo(value: &str) -> bool {
    let Some(scheme_end) = value.find("://") else {
        return false;
    };
    let authority = &value[(scheme_end + 3)..];
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    authority[..authority_end].contains('@')
}

pub(super) fn validate_optional_secret(
    name: &str,
    value: Option<&SecretString>,
) -> StorageResult<()> {
    if value
        .map(SecretString::expose_secret)
        .is_some_and(|secret| secret.trim().is_empty())
    {
        return Err(StorageError::configuration(format!(
            "{name} must not be empty"
        )));
    }
    Ok(())
}

pub(super) fn finish_store_build<O: ObjectStore>(
    result: object_store::Result<O>,
    provider: &str,
    resource_label: &str,
    resource_name: &str,
) -> StorageResult<Arc<dyn ObjectStore>> {
    result
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(|error| StorageError::configuration(format!("failed to build {provider} store for {resource_label} {resource_name}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ManagedStoreRegistry;

    #[test]
    fn secret_string_debug_is_redacted() {
        let config = StoreConfig::S3(S3StoreConfig {
            access_key_id: Some(SecretString::new("AKIA_TEST_VALUE")),
            secret_access_key: Some(SecretString::new("SECRET_TEST_VALUE")),
            ..S3StoreConfig::default()
        });

        let debug = format!("{config:?}");

        assert!(!debug.contains("AKIA_TEST_VALUE"));
        assert!(!debug.contains("SECRET_TEST_VALUE"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn replace_managed_config_rejects_invalid_provider_config() {
        let registry = ManagedStoreRegistry::new();

        let error = registry
            .replace_config(
                1,
                StoreConfig::S3Compatible(S3CompatibleStoreConfig {
                    endpoint: " ".to_string(),
                    region: None,
                    access_key_id: None,
                    secret_access_key: None,
                    token: None,
                    allow_http: false,
                    virtual_hosted_style_request: false,
                    skip_signature: false,
                }),
            )
            .unwrap_err();

        assert!(matches!(&error, StorageError::Configuration { .. }));
        assert!(error.wire_message().contains("endpoint"));
        assert!(registry.resolve(1).is_err());
    }

    #[test]
    fn store_config_rejects_ambiguous_or_partial_credentials() {
        let gcs = StoreConfig::Gcs(GcsStoreConfig {
            service_account_path: Some("/tmp/service-account.json".to_string()),
            service_account_key: Some(SecretString::new("{}")),
            ..GcsStoreConfig::default()
        });
        let azure = StoreConfig::Azure(AzureStoreConfig {
            client_id: Some("client-id".to_string()),
            client_secret: Some(SecretString::new("secret")),
            tenant_id: None,
            ..AzureStoreConfig::default()
        });

        assert!(
            gcs.validate()
                .unwrap_err()
                .wire_message()
                .contains("at most one credential source")
        );
        assert!(
            azure
                .validate()
                .unwrap_err()
                .wire_message()
                .contains("requires client_id, client_secret and tenant_id")
        );
    }

    #[test]
    fn bucket_validation_rejects_credentials_the_static_checks_cannot_parse() {
        let gcs = StoreConfig::Gcs(GcsStoreConfig {
            service_account_key: Some(SecretString::new(
                r#"{"type":"service_account"}"#,
            )),
            ..GcsStoreConfig::default()
        });
        let azure = StoreConfig::Azure(AzureStoreConfig {
            account: Some("account".to_owned()),
            access_key: Some(SecretString::new("not-base64")),
            ..AzureStoreConfig::default()
        });

        assert!(gcs.validate().is_ok());
        assert!(gcs.validate_for_bucket("bucket").is_err());
        assert!(azure.validate().is_ok());
        assert!(azure.validate_for_bucket("container").is_err());
    }

    #[test]
    fn endpoint_rejects_embedded_credentials_and_query_tokens() {
        for endpoint in [
            "https://user:password@example.test",
            "https://example.test?token=secret",
            "https://example.test/#secret",
        ] {
            let error =
                validate_endpoint("test endpoint", endpoint, false).unwrap_err();
            assert!(matches!(error, StorageError::Configuration { .. }));
        }
    }
}
