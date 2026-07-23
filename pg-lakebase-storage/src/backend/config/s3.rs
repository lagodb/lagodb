use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};

use super::{
    S3CompatibleStoreConfig, S3StoreConfig, finish_store_build, validate_endpoint,
    validate_optional_non_empty, validate_optional_secret,
};
use crate::error::{StorageError, StorageResult};

// `from_env` also imports namespace, transport, encryption and request behavior. Keep default
// credential discovery opt-in so the Volume descriptor remains authoritative for those fields.
const ENVIRONMENT_CREDENTIAL_KEYS: &[AmazonS3ConfigKey] = &[
    AmazonS3ConfigKey::AccessKeyId,
    AmazonS3ConfigKey::SecretAccessKey,
    AmazonS3ConfigKey::Region,
    AmazonS3ConfigKey::Token,
    AmazonS3ConfigKey::ImdsV1Fallback,
    AmazonS3ConfigKey::MetadataEndpoint,
    AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
    AmazonS3ConfigKey::ContainerCredentialsFullUri,
    AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
    AmazonS3ConfigKey::WebIdentityTokenFile,
    AmazonS3ConfigKey::RoleArn,
    AmazonS3ConfigKey::RoleSessionName,
    AmazonS3ConfigKey::StsEndpoint,
];

trait AmazonS3BuilderDefaultChain {
    fn with_environment_credentials(self) -> Self;
    fn with_environment_credentials_from(self, environment: &Self) -> Self;
}

impl AmazonS3BuilderDefaultChain for AmazonS3Builder {
    fn with_environment_credentials(self) -> Self {
        self.with_environment_credentials_from(&Self::from_env())
    }

    fn with_environment_credentials_from(mut self, environment: &Self) -> Self {
        for key in ENVIRONMENT_CREDENTIAL_KEYS {
            if let Some(value) = environment.get_config_value(key) {
                self = self.with_config(*key, value);
            }
        }
        self
    }
}

impl S3StoreConfig {
    fn uses_default_chain(&self) -> bool {
        self.access_key_id.is_none()
            && self.secret_access_key.is_none()
            && self.token.is_none()
            && !self.skip_signature
    }

    pub(super) fn validate(&self) -> StorageResult<()> {
        validate_optional_non_empty("S3 region", self.region.as_deref())?;
        if let Some(endpoint) = &self.endpoint {
            validate_endpoint("S3 endpoint", endpoint, self.allow_http)?;
        }
        validate_optional_secret("S3 access_key_id", self.access_key_id.as_ref())?;
        validate_optional_secret(
            "S3 secret_access_key",
            self.secret_access_key.as_ref(),
        )?;
        validate_optional_secret("S3 token", self.token.as_ref())?;
        validate_credential_fields(
            self.access_key_id.is_some(),
            self.secret_access_key.is_some(),
            self.token.is_some(),
        )
    }

    pub(super) fn build_store(
        &self,
        bucket: &str,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        self.validate()?;
        let mut builder = AmazonS3Builder::new();
        if self.uses_default_chain() {
            builder = builder.with_environment_credentials();
        }
        builder = builder.with_bucket_name(bucket);
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(access_key_id) = &self.access_key_id {
            builder = builder.with_access_key_id(access_key_id.expose_secret());
        }
        if let Some(secret_access_key) = &self.secret_access_key {
            builder =
                builder.with_secret_access_key(secret_access_key.expose_secret());
        }
        if let Some(token) = &self.token {
            builder = builder.with_token(token.expose_secret());
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }
        if self.virtual_hosted_style_request {
            builder = builder.with_virtual_hosted_style_request(true);
        }
        if self.skip_signature {
            builder = builder.with_skip_signature(true);
        }
        finish_store_build(builder.build(), "S3", "bucket", bucket)
    }
}

impl S3CompatibleStoreConfig {
    fn uses_default_chain(&self) -> bool {
        self.access_key_id.is_none()
            && self.secret_access_key.is_none()
            && self.token.is_none()
            && !self.skip_signature
    }

    pub(super) fn validate(&self) -> StorageResult<()> {
        validate_endpoint("S3-compatible endpoint", &self.endpoint, self.allow_http)?;
        validate_optional_non_empty("S3-compatible region", self.region.as_deref())?;
        validate_optional_secret(
            "S3-compatible access_key_id",
            self.access_key_id.as_ref(),
        )?;
        validate_optional_secret(
            "S3-compatible secret_access_key",
            self.secret_access_key.as_ref(),
        )?;
        validate_optional_secret("S3-compatible token", self.token.as_ref())?;
        validate_credential_fields(
            self.access_key_id.is_some(),
            self.secret_access_key.is_some(),
            self.token.is_some(),
        )
    }

    pub(super) fn build_store(
        &self,
        bucket: &str,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        self.validate()?;
        let mut builder = AmazonS3Builder::new();
        if self.uses_default_chain() {
            builder = builder.with_environment_credentials();
        }
        builder = builder
            .with_bucket_name(bucket)
            .with_endpoint(&self.endpoint);
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if let Some(access_key_id) = &self.access_key_id {
            builder = builder.with_access_key_id(access_key_id.expose_secret());
        }
        if let Some(secret_access_key) = &self.secret_access_key {
            builder =
                builder.with_secret_access_key(secret_access_key.expose_secret());
        }
        if let Some(token) = &self.token {
            builder = builder.with_token(token.expose_secret());
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }
        if self.virtual_hosted_style_request {
            builder = builder.with_virtual_hosted_style_request(true);
        }
        if self.skip_signature {
            builder = builder.with_skip_signature(true);
        }
        finish_store_build(builder.build(), "S3-compatible", "bucket", bucket)
    }
}

fn validate_credential_fields(
    has_access_key_id: bool,
    has_secret_access_key: bool,
    has_token: bool,
) -> StorageResult<()> {
    if has_access_key_id != has_secret_access_key {
        return Err(StorageError::configuration(
            "S3 credential requires access_key_id and secret_access_key together",
        ));
    }
    if has_token && !has_access_key_id {
        return Err(StorageError::configuration(
            "S3 token requires access_key_id and secret_access_key",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SecretString;

    #[test]
    fn default_chain_imports_only_s3_credential_discovery_config() {
        let environment = AmazonS3Builder::new()
            .with_config(AmazonS3ConfigKey::AccessKeyId, "environment-access-key")
            .with_config(AmazonS3ConfigKey::SecretAccessKey, "environment-secret-key")
            .with_config(AmazonS3ConfigKey::Region, "environment-region")
            .with_config(AmazonS3ConfigKey::WebIdentityTokenFile, "/token")
            .with_config(AmazonS3ConfigKey::RoleArn, "environment-role")
            .with_config(AmazonS3ConfigKey::Bucket, "environment-bucket")
            .with_config(AmazonS3ConfigKey::Endpoint, "https://environment.example")
            .with_config(
                AmazonS3ConfigKey::S3Endpoint,
                "https://s3-environment.example",
            )
            .with_config(AmazonS3ConfigKey::VirtualHostedStyleRequest, "true")
            .with_config(AmazonS3ConfigKey::SkipSignature, "true")
            .with_config(AmazonS3ConfigKey::RequestPayer, "true");

        let filtered =
            AmazonS3Builder::new().with_environment_credentials_from(&environment);

        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::AccessKeyId),
            Some("environment-access-key".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::SecretAccessKey),
            Some("environment-secret-key".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::Region),
            Some("environment-region".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::WebIdentityTokenFile),
            Some("/token".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::RoleArn),
            Some("environment-role".to_owned())
        );
        assert_eq!(filtered.get_config_value(&AmazonS3ConfigKey::Bucket), None);
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::Endpoint),
            None
        );
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::S3Endpoint),
            None
        );
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::VirtualHostedStyleRequest),
            Some("false".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::SkipSignature),
            Some("false".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AmazonS3ConfigKey::RequestPayer),
            Some("false".to_owned())
        );
    }

    #[test]
    fn s3_configs_distinguish_default_explicit_and_anonymous_credentials() {
        assert!(S3StoreConfig::default().uses_default_chain());
        assert!(
            S3CompatibleStoreConfig {
                endpoint: "https://storage.example".to_owned(),
                region: None,
                access_key_id: None,
                secret_access_key: None,
                token: None,
                allow_http: false,
                virtual_hosted_style_request: false,
                skip_signature: false,
            }
            .uses_default_chain()
        );
        assert!(
            !S3StoreConfig {
                access_key_id: Some(SecretString::new("access-key")),
                secret_access_key: Some(SecretString::new("secret-key")),
                ..S3StoreConfig::default()
            }
            .uses_default_chain()
        );
        assert!(
            !S3StoreConfig {
                skip_signature: true,
                ..S3StoreConfig::default()
            }
            .uses_default_chain()
        );
    }
}
