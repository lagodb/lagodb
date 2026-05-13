//! Provider-specific [`object_store`] configuration, validation, and lazy client construction.
//!
//! [`StoreConfig`] is the user-facing enum that wraps one of the provider configs. It owns both
//! the validation rules exercised at registration time and the actual builder invocations that
//! hand back an [`ObjectStore`] client for a given bucket.
//!
//! [`ConfiguredObjectBackend`] turns a [`StoreConfig`] into an [`ObjectBackend`] by caching a
//! client per bucket on first access.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::ObjectStore;

use super::object_store::ObjectStoreBackend;
use super::secret::SecretString;
use super::ObjectBackend;
use crate::error::{StorageError, StorageResult};
use crate::object::{ListEntry, ObjectInfo, ObjectLocation};

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
            Self::S3(config) => validate_s3(config),
            Self::S3Compatible(config) => validate_s3_compatible(config),
            Self::Gcs(config) => validate_gcs(config),
            Self::Azure(config) => validate_azure(config),
        }
    }

    /// Instantiate an [`ObjectStore`] client for `bucket` using this provider config.
    pub(super) fn build_store(&self, bucket: &str) -> StorageResult<Arc<dyn ObjectStore>> {
        match self {
            Self::S3(config) => build_s3_store(config, bucket),
            Self::S3Compatible(config) => build_s3_compatible_store(config, bucket),
            Self::Gcs(config) => build_gcs_store(config, bucket),
            Self::Azure(config) => build_azure_store(config, bucket),
        }
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn validate_s3(config: &S3StoreConfig) -> StorageResult<()> {
    validate_optional_secret("S3 access_key_id", config.access_key_id.as_ref())?;
    validate_optional_secret("S3 secret_access_key", config.secret_access_key.as_ref())?;
    validate_optional_secret("S3 token", config.token.as_ref())?;
    Ok(())
}

fn validate_s3_compatible(config: &S3CompatibleStoreConfig) -> StorageResult<()> {
    validate_non_empty("S3-compatible endpoint", &config.endpoint)?;
    validate_optional_secret("S3-compatible access_key_id", config.access_key_id.as_ref())?;
    validate_optional_secret("S3-compatible secret_access_key", config.secret_access_key.as_ref())?;
    validate_optional_secret("S3-compatible token", config.token.as_ref())?;
    Ok(())
}

fn validate_gcs(config: &GcsStoreConfig) -> StorageResult<()> {
    validate_optional_secret("GCS service_account_key", config.service_account_key.as_ref())?;
    let credential_sources = usize::from(config.service_account_path.is_some())
        + usize::from(config.service_account_key.is_some())
        + usize::from(config.application_credentials_path.is_some());
    if credential_sources > 1 {
        return Err(StorageError::configuration(
            "GCS config must use at most one credential source: service_account_path, service_account_key or application_credentials_path",
        ));
    }
    Ok(())
}

fn validate_azure(config: &AzureStoreConfig) -> StorageResult<()> {
    validate_optional_secret("Azure access_key", config.access_key.as_ref())?;
    validate_optional_secret("Azure bearer_token", config.bearer_token.as_ref())?;
    validate_optional_secret("Azure client_secret", config.client_secret.as_ref())?;
    let client_secret_fields = usize::from(config.client_id.is_some())
        + usize::from(config.client_secret.is_some())
        + usize::from(config.tenant_id.is_some());
    if client_secret_fields != 0 && client_secret_fields != 3 {
        return Err(StorageError::configuration(
            "Azure client secret auth requires client_id, client_secret and tenant_id",
        ));
    }
    Ok(())
}

fn validate_non_empty(name: &str, value: &str) -> StorageResult<()> {
    if value.trim().is_empty() {
        return Err(StorageError::configuration(format!("{name} must not be empty")));
    }
    Ok(())
}

fn validate_optional_secret(name: &str, value: Option<&SecretString>) -> StorageResult<()> {
    if matches!(value.map(SecretString::expose_secret), Some("")) {
        return Err(StorageError::configuration(format!("{name} must not be empty")));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn finish_store_build<O: ObjectStore>(
    result: object_store::Result<O>,
    provider: &str,
    resource_label: &str,
    resource_name: &str,
) -> StorageResult<Arc<dyn ObjectStore>> {
    result
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(|error| StorageError::configuration(format!("failed to build {provider} store for {resource_label} {resource_name}: {error}")))
}

fn build_s3_store(config: &S3StoreConfig, bucket: &str) -> StorageResult<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket.to_string());
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(access_key_id) = &config.access_key_id {
        builder = builder.with_access_key_id(access_key_id.expose_secret());
    }
    if let Some(secret_access_key) = &config.secret_access_key {
        builder = builder.with_secret_access_key(secret_access_key.expose_secret());
    }
    if let Some(token) = &config.token {
        builder = builder.with_token(token.expose_secret());
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }
    if config.virtual_hosted_style_request {
        builder = builder.with_virtual_hosted_style_request(true);
    }
    if config.skip_signature {
        builder = builder.with_skip_signature(true);
    }
    finish_store_build(builder.build(), "S3", "bucket", bucket)
}

fn build_s3_compatible_store(config: &S3CompatibleStoreConfig, bucket: &str) -> StorageResult<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket.to_string())
        .with_endpoint(&config.endpoint);
    if let Some(region) = &config.region {
        builder = builder.with_region(region);
    }
    if let Some(access_key_id) = &config.access_key_id {
        builder = builder.with_access_key_id(access_key_id.expose_secret());
    }
    if let Some(secret_access_key) = &config.secret_access_key {
        builder = builder.with_secret_access_key(secret_access_key.expose_secret());
    }
    if let Some(token) = &config.token {
        builder = builder.with_token(token.expose_secret());
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }
    if config.virtual_hosted_style_request {
        builder = builder.with_virtual_hosted_style_request(true);
    }
    if config.skip_signature {
        builder = builder.with_skip_signature(true);
    }
    finish_store_build(builder.build(), "S3-compatible", "bucket", bucket)
}

fn build_gcs_store(config: &GcsStoreConfig, bucket: &str) -> StorageResult<Arc<dyn ObjectStore>> {
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket.to_string());
    if let Some(base_url) = &config.base_url {
        builder = builder.with_base_url(base_url);
    }
    if let Some(service_account_path) = &config.service_account_path {
        builder = builder.with_service_account_path(service_account_path);
    }
    if let Some(service_account_key) = &config.service_account_key {
        builder = builder.with_service_account_key(service_account_key.expose_secret());
    }
    if let Some(application_credentials_path) = &config.application_credentials_path {
        builder = builder.with_application_credentials(application_credentials_path);
    }
    if config.skip_signature {
        builder = builder.with_skip_signature(true);
    }
    finish_store_build(builder.build(), "GCS", "bucket", bucket)
}

fn build_azure_store(config: &AzureStoreConfig, bucket: &str) -> StorageResult<Arc<dyn ObjectStore>> {
    let mut builder = MicrosoftAzureBuilder::new().with_container_name(bucket.to_string());
    if let Some(account) = &config.account {
        builder = builder.with_account(account);
    }
    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }
    if let Some(access_key) = &config.access_key {
        builder = builder.with_access_key(access_key.expose_secret());
    }
    if let Some(bearer_token) = &config.bearer_token {
        builder = builder.with_bearer_token_authorization(bearer_token.expose_secret());
    }
    if let (Some(client_id), Some(client_secret), Some(tenant_id)) =
        (&config.client_id, &config.client_secret, &config.tenant_id)
    {
        builder = builder.with_client_secret_authorization(client_id, client_secret.expose_secret(), tenant_id);
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }
    if config.use_emulator {
        builder = builder.with_use_emulator(true);
    }
    finish_store_build(builder.build(), "Azure", "container", bucket)
}

// ---------------------------------------------------------------------------
// Lazy per-bucket backend
// ---------------------------------------------------------------------------

/// Backend that lazily materialises a per-bucket [`ObjectStore`] client from a [`StoreConfig`].
///
/// Clients are cached by bucket name so repeated reads against the same bucket share connection
/// state and credential loading.
pub struct ConfiguredObjectBackend {
    config: StoreConfig,
    stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl ConfiguredObjectBackend {
    pub fn new(config: StoreConfig) -> Self {
        Self {
            config,
            stores: RwLock::new(HashMap::new()),
        }
    }

    fn store_for_bucket(&self, bucket: &str) -> StorageResult<Arc<dyn ObjectStore>> {
        if let Some(store) = self
            .stores
            .read()
            .expect("configured object backend rwlock poisoned; bucket clients are no longer trustworthy")
            .get(bucket)
            .cloned()
        {
            return Ok(store);
        }

        let store = self.config.build_store(bucket)?;
        let mut stores = self
            .stores
            .write()
            .expect("configured object backend rwlock poisoned; bucket clients are no longer trustworthy");
        if let Some(existing) = stores.get(bucket).cloned() {
            return Ok(existing);
        }
        stores.insert(bucket.to_string(), store.clone());
        Ok(store)
    }
}

#[async_trait]
impl ObjectBackend for ConfiguredObjectBackend {
    async fn head(&self, key: &ObjectLocation) -> StorageResult<ObjectInfo> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket()).head(key).await
    }

    async fn get_range(&self, key: &ObjectLocation, range: Range<u64>) -> StorageResult<bytes::Bytes> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket()).get_range(key, range).await
    }

    async fn put_from_file(
        &self,
        key: &ObjectLocation,
        path: &std::path::Path,
        len: u64,
    ) -> StorageResult<ObjectInfo> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket())
            .put_from_file(key, path, len)
            .await
    }

    fn list(
        &self,
        store_id: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> BoxStream<'static, StorageResult<ListEntry>> {
        match self.store_for_bucket(bucket) {
            Ok(store) => ObjectStoreBackend::for_bucket(store, bucket).list(store_id, bucket, prefix),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn delete(&self, key: &ObjectLocation) -> StorageResult<()> {
        let store = self.store_for_bucket(key.bucket())?;
        ObjectStoreBackend::for_bucket(store, key.bucket()).delete(key).await
    }

    fn delete_stream(
        &self,
        store_id: &str,
        bucket: &str,
        keys: BoxStream<'static, StorageResult<String>>,
    ) -> BoxStream<'static, StorageResult<String>> {
        match self.store_for_bucket(bucket) {
            Ok(store) => ObjectStoreBackend::for_bucket(store, bucket).delete_stream(store_id, bucket, keys),
            Err(error) => {
                // The bucket cannot be reached at all (build error). Drain the input stream as
                // failures so callers see a deterministic error per attempted key.
                let template = error.to_string();
                keys.map(move |item| {
                    item.and_then(|_| Err(StorageError::configuration(template.clone())))
                })
                .boxed()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::registry::StoreRegistry;
    use crate::object::StoreId;

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
    fn register_config_rejects_invalid_provider_config() {
        let registry = StoreRegistry::new();

        let error = registry
            .register_config(
                "bad-s3-compatible",
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
        assert!(!registry.contains(&StoreId::new("bad-s3-compatible").unwrap()));
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

        assert!(gcs
            .validate()
            .unwrap_err()
            .wire_message()
            .contains("at most one credential source"));
        assert!(azure
            .validate()
            .unwrap_err()
            .wire_message()
            .contains("requires client_id, client_secret and tenant_id"));
    }
}
