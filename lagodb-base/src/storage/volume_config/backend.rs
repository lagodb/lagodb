//! Conversion from validated volume domain objects to storage backends.

use lagodb_storage::{
    AwsCredentialConfig, AwsCredentialSource, AzureStoreConfig, GcsStoreConfig,
    S3Authentication, S3StoreConfig, SecretString, StoreConfig,
};

use super::credential::CredentialConfig;
use super::domain::{StorageLocation, StorageVolumeConfig, StorageVolumeError};

impl StorageVolumeConfig {
    pub(crate) fn compact_id(&self) -> String {
        self.id.to_compact_string()
    }
}

impl StorageLocation {
    pub(crate) fn store_config(
        &self,
        credential: &CredentialConfig,
    ) -> Result<StoreConfig, StorageVolumeError> {
        credential.validate_for(self)?;
        let config = match (self, credential) {
            (
                StorageLocation::S3 {
                    region,
                    endpoint,
                    allow_http,
                    virtual_hosted_style_request,
                    ..
                },
                credential,
            ) => S3StoreConfig {
                region: region.clone(),
                endpoint: endpoint.clone(),
                authentication: credential.s3_authentication(),
                allow_http: *allow_http,
                virtual_hosted_style_request: *virtual_hosted_style_request,
                encryption: None,
            }
            .into_canonical(),
            (StorageLocation::Gcs { base_url, .. }, credential) => {
                let (service_account_key, skip_signature) = match credential {
                    CredentialConfig::GcsServiceAccount { service_account } => (
                        Some(SecretString::new(service_account.as_json().to_owned())),
                        false,
                    ),
                    CredentialConfig::Anonymous => (None, true),
                    CredentialConfig::DefaultChain => (None, false),
                    _ => unreachable!("validated GCS credential"),
                };
                StoreConfig::Gcs(GcsStoreConfig {
                    base_url: base_url.clone(),
                    service_account_path: None,
                    service_account_key,
                    application_credentials_path: None,
                    bearer_token: None,
                    skip_signature,
                })
            }
            (
                StorageLocation::Azure {
                    account,
                    endpoint,
                    allow_http,
                    use_emulator,
                    ..
                },
                credential,
            ) => {
                let mut config = AzureStoreConfig {
                    account: account.clone(),
                    endpoint: endpoint.clone(),
                    allow_http: *allow_http,
                    use_emulator: *use_emulator,
                    ..AzureStoreConfig::default()
                };
                match credential {
                    CredentialConfig::DefaultChain => {}
                    CredentialConfig::AzureAccessKey { access_key } => {
                        config.access_key =
                            Some(SecretString::new(access_key.clone()));
                    }
                    CredentialConfig::AzureBearerToken { bearer_token } => {
                        config.bearer_token =
                            Some(SecretString::new(bearer_token.clone()));
                    }
                    CredentialConfig::AzureClientSecret {
                        client_id,
                        client_secret,
                        tenant_id,
                    } => {
                        config.client_id = Some(client_id.clone());
                        config.client_secret =
                            Some(SecretString::new(client_secret.clone()));
                        config.tenant_id = Some(tenant_id.clone());
                    }
                    _ => unreachable!("validated Azure credential"),
                }
                StoreConfig::Azure(config)
            }
        };
        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate_for_persistence(
        &self,
        credential: &CredentialConfig,
    ) -> Result<(), StorageVolumeError> {
        let config = self.store_config(credential)?;
        if !credential.reloads_provider_on_force() {
            config.validate_for_bucket(self.namespace())?;
        }
        Ok(())
    }
}

impl CredentialConfig {
    fn s3_authentication(&self) -> S3Authentication {
        match self {
            Self::DefaultChain => S3Authentication::default(),
            Self::Anonymous => S3Authentication::Anonymous,
            Self::Aws(config) => S3Authentication::Aws(config.clone()),
            Self::S3AccessKey {
                access_key_id,
                secret_access_key,
                token,
            } => S3Authentication::Aws(AwsCredentialConfig {
                source: AwsCredentialSource::AccessKey {
                    access_key_id: SecretString::new(access_key_id.clone()),
                    secret_access_key: SecretString::new(secret_access_key.clone()),
                    token: token.clone().map(SecretString::new),
                },
                assume_role: None,
            }),
            _ => unreachable!("validated S3 credential"),
        }
    }
}
