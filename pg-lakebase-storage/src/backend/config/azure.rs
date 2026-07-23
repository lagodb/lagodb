use std::sync::Arc;

use object_store::ObjectStore;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};

use super::{
    AzureStoreConfig, finish_store_build, validate_endpoint,
    validate_optional_non_empty, validate_optional_secret,
};
use crate::error::{StorageError, StorageResult};

// Account, container, endpoint, emulator and data-plane behavior must not escape the Volume
// descriptor through Azure's broad `from_env` import.
const ENVIRONMENT_CREDENTIAL_KEYS: &[AzureConfigKey] = &[
    AzureConfigKey::AccessKey,
    AzureConfigKey::ClientId,
    AzureConfigKey::ClientSecret,
    AzureConfigKey::AuthorityId,
    AzureConfigKey::AuthorityHost,
    AzureConfigKey::SasKey,
    AzureConfigKey::Token,
    AzureConfigKey::MsiEndpoint,
    AzureConfigKey::ObjectId,
    AzureConfigKey::MsiResourceId,
    AzureConfigKey::FederatedTokenFile,
    AzureConfigKey::UseAzureCli,
    AzureConfigKey::FabricTokenServiceUrl,
    AzureConfigKey::FabricWorkloadHost,
    AzureConfigKey::FabricSessionToken,
    AzureConfigKey::FabricClusterIdentifier,
];

trait MicrosoftAzureBuilderDefaultChain {
    fn with_environment_credentials(self) -> Self;
    fn with_environment_credentials_from(self, environment: &Self) -> Self;
}

impl MicrosoftAzureBuilderDefaultChain for MicrosoftAzureBuilder {
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

impl AzureStoreConfig {
    fn uses_default_chain(&self) -> bool {
        self.access_key.is_none()
            && self.bearer_token.is_none()
            && self.client_id.is_none()
            && self.client_secret.is_none()
            && self.tenant_id.is_none()
            && !self.use_emulator
    }

    pub(super) fn validate(&self) -> StorageResult<()> {
        validate_optional_non_empty("Azure account", self.account.as_deref())?;
        if let Some(endpoint) = &self.endpoint {
            validate_endpoint(
                "Azure endpoint",
                endpoint,
                self.allow_http || self.use_emulator,
            )?;
        }
        validate_optional_non_empty("Azure client_id", self.client_id.as_deref())?;
        validate_optional_non_empty("Azure tenant_id", self.tenant_id.as_deref())?;
        validate_optional_secret("Azure access_key", self.access_key.as_ref())?;
        validate_optional_secret("Azure bearer_token", self.bearer_token.as_ref())?;
        validate_optional_secret("Azure client_secret", self.client_secret.as_ref())?;
        let client_secret_fields = usize::from(self.client_id.is_some())
            + usize::from(self.client_secret.is_some())
            + usize::from(self.tenant_id.is_some());
        if client_secret_fields != 0 && client_secret_fields != 3 {
            return Err(StorageError::configuration(
                "Azure client secret auth requires client_id, client_secret and tenant_id",
            ));
        }
        let credential_sources = usize::from(self.access_key.is_some())
            + usize::from(self.bearer_token.is_some())
            + usize::from(client_secret_fields == 3);
        if credential_sources > 1 {
            return Err(StorageError::configuration(
                "Azure config must use at most one credential source",
            ));
        }
        if !self.use_emulator && self.account.is_none() {
            return Err(StorageError::configuration(
                "Azure account is required unless use_emulator is true",
            ));
        }
        Ok(())
    }

    pub(super) fn build_store(
        &self,
        container: &str,
    ) -> StorageResult<Arc<dyn ObjectStore>> {
        self.validate()?;
        let mut builder = MicrosoftAzureBuilder::new();
        if self.uses_default_chain() {
            builder = builder.with_environment_credentials();
        }
        builder = builder.with_container_name(container);
        if let Some(account) = &self.account {
            builder = builder.with_account(account);
        }
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint.clone());
        }
        if let Some(access_key) = &self.access_key {
            builder = builder.with_access_key(access_key.expose_secret());
        }
        if let Some(bearer_token) = &self.bearer_token {
            builder =
                builder.with_bearer_token_authorization(bearer_token.expose_secret());
        }
        if let (Some(client_id), Some(client_secret), Some(tenant_id)) =
            (&self.client_id, &self.client_secret, &self.tenant_id)
        {
            builder = builder.with_client_secret_authorization(
                client_id,
                client_secret.expose_secret(),
                tenant_id,
            );
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }
        if self.use_emulator {
            builder = builder.with_use_emulator(true);
        }
        finish_store_build(builder.build(), "Azure", "container", container)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::SecretString;

    #[test]
    fn default_chain_imports_only_azure_credential_discovery_config() {
        let environment = MicrosoftAzureBuilder::new()
            .with_config(AzureConfigKey::AccessKey, "environment-access-key")
            .with_config(AzureConfigKey::ClientId, "environment-client-id")
            .with_config(AzureConfigKey::ClientSecret, "environment-client-secret")
            .with_config(AzureConfigKey::AuthorityId, "environment-tenant")
            .with_config(AzureConfigKey::FederatedTokenFile, "/federated-token")
            .with_config(AzureConfigKey::UseAzureCli, "true")
            .with_config(AzureConfigKey::AccountName, "environment-account")
            .with_config(AzureConfigKey::ContainerName, "environment-container")
            .with_config(AzureConfigKey::Endpoint, "https://environment.example")
            .with_config(AzureConfigKey::UseEmulator, "true")
            .with_config(AzureConfigKey::UseFabricEndpoint, "true")
            .with_config(AzureConfigKey::SkipSignature, "true")
            .with_config(AzureConfigKey::DisableTagging, "true");

        let filtered = MicrosoftAzureBuilder::new()
            .with_environment_credentials_from(&environment);

        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::AccessKey),
            Some("environment-access-key".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::ClientId),
            Some("environment-client-id".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::FederatedTokenFile),
            Some("/federated-token".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::UseAzureCli),
            Some("true".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::AccountName),
            None
        );
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::ContainerName),
            None
        );
        assert_eq!(filtered.get_config_value(&AzureConfigKey::Endpoint), None);
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::UseEmulator),
            Some("false".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::UseFabricEndpoint),
            Some("false".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::SkipSignature),
            Some("false".to_owned())
        );
        assert_eq!(
            filtered.get_config_value(&AzureConfigKey::DisableTagging),
            Some("false".to_owned())
        );
    }

    #[test]
    fn azure_config_distinguishes_default_explicit_and_emulator_credentials() {
        assert!(
            AzureStoreConfig {
                account: Some("account".to_owned()),
                ..AzureStoreConfig::default()
            }
            .uses_default_chain()
        );
        assert!(
            !AzureStoreConfig {
                account: Some("account".to_owned()),
                access_key: Some(SecretString::new("access-key")),
                ..AzureStoreConfig::default()
            }
            .uses_default_chain()
        );
        assert!(
            !AzureStoreConfig {
                use_emulator: true,
                ..AzureStoreConfig::default()
            }
            .uses_default_chain()
        );
    }
}
