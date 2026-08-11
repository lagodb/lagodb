use std::sync::Arc;

use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::{ObjectStore, RetryConfig};

use super::{
    GcsStoreConfig, finish_store_build, validate_endpoint, validate_optional_secret,
};
use crate::error::{StorageError, StorageResult};

// Bucket, base URL, anonymous access and HTTP behavior belong to the Volume descriptor, not the
// worker environment.
const ENVIRONMENT_CREDENTIAL_KEYS: &[GoogleConfigKey] = &[
    GoogleConfigKey::ServiceAccount,
    GoogleConfigKey::ServiceAccountKey,
    GoogleConfigKey::ApplicationCredentials,
];

trait GoogleCloudStorageBuilderDefaultChain {
    fn with_environment_credentials(self) -> Self;
    fn with_environment_credentials_from(self, environment: &Self) -> Self;
}

impl GoogleCloudStorageBuilderDefaultChain for GoogleCloudStorageBuilder {
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

impl GcsStoreConfig {
    fn uses_default_chain(&self) -> bool {
        self.service_account_path.is_none()
            && self.service_account_key.is_none()
            && self.application_credentials_path.is_none()
            && !self.skip_signature
    }

    pub(super) fn validate(&self) -> StorageResult<()> {
        if let Some(base_url) = &self.base_url {
            validate_endpoint("GCS base_url", base_url, true)?;
        }
        validate_optional_secret(
            "GCS service_account_key",
            self.service_account_key.as_ref(),
        )?;
        let credential_sources = usize::from(self.service_account_path.is_some())
            + usize::from(self.service_account_key.is_some())
            + usize::from(self.application_credentials_path.is_some());
        if credential_sources > 1 {
            return Err(StorageError::configuration(
                "GCS config must use at most one credential source: service_account_path, service_account_key or application_credentials_path",
            ));
        }
        if self.skip_signature && credential_sources != 0 {
            return Err(StorageError::configuration(
                "GCS skip_signature cannot be combined with explicit credentials",
            ));
        }
        Ok(())
    }

    pub(super) fn build_store(
        &self,
        bucket: &str,
        retry_config: RetryConfig,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        self.validate()?;
        let mut builder = GoogleCloudStorageBuilder::new();
        if self.uses_default_chain() {
            builder = builder.with_environment_credentials();
        }
        builder = builder.with_bucket_name(bucket);
        if let Some(base_url) = &self.base_url {
            builder = builder.with_base_url(base_url);
        }
        if let Some(service_account_path) = &self.service_account_path {
            builder = builder.with_service_account_path(service_account_path);
        }
        if let Some(service_account_key) = &self.service_account_key {
            builder =
                builder.with_service_account_key(service_account_key.expose_secret());
        }
        if let Some(application_credentials_path) = &self.application_credentials_path
        {
            builder =
                builder.with_application_credentials(application_credentials_path);
        }
        if self.skip_signature {
            builder = builder.with_skip_signature(true);
        }
        builder = builder.with_retry(retry_config);
        finish_store_build(builder.build(), "GCS", "bucket", bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SecretString;

    #[test]
    fn default_chain_imports_only_gcs_credential_discovery_config() {
        let environment = GoogleCloudStorageBuilder::new()
            .with_config(GoogleConfigKey::ServiceAccount, "/service-account.json")
            .with_config(GoogleConfigKey::ServiceAccountKey, "service-account-json")
            .with_config(
                GoogleConfigKey::ApplicationCredentials,
                "/application-credentials.json",
            )
            .with_config(GoogleConfigKey::Bucket, "environment-bucket")
            .with_config(GoogleConfigKey::BaseUrl, "https://environment.example")
            .with_config(GoogleConfigKey::SkipSignature, "true");

        let filtered = GoogleCloudStorageBuilder::new()
            .with_environment_credentials_from(&environment);

        assert_eq!(
            filtered.get_config_value(&GoogleConfigKey::ServiceAccount),
            Some("/service-account.json".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&GoogleConfigKey::ServiceAccountKey),
            Some("service-account-json".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&GoogleConfigKey::ApplicationCredentials),
            Some("/application-credentials.json".to_owned())
        );
        assert_eq!(filtered.get_config_value(&GoogleConfigKey::Bucket), None);
        assert_eq!(filtered.get_config_value(&GoogleConfigKey::BaseUrl), None);
        assert_eq!(
            filtered.get_config_value(&GoogleConfigKey::SkipSignature),
            Some("false".to_owned())
        );
    }

    #[test]
    fn gcs_config_distinguishes_default_explicit_and_anonymous_credentials() {
        assert!(GcsStoreConfig::default().uses_default_chain());
        assert!(
            !GcsStoreConfig {
                service_account_key: Some(SecretString::new("{}")),
                ..GcsStoreConfig::default()
            }
            .uses_default_chain()
        );
        assert!(
            !GcsStoreConfig {
                skip_signature: true,
                ..GcsStoreConfig::default()
            }
            .uses_default_chain()
        );
    }
}
