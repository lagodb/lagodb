//! Provider-aware credential domain values.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::domain::{StorageLocation, StorageVolumeError};
use super::error::CredentialValidationError;
use super::service_account::ServiceAccountJson;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CredentialConfig {
    DefaultChain,
    Anonymous,
    S3AccessKey {
        access_key_id: String,
        secret_access_key: String,
        #[serde(default)]
        session_token: Option<String>,
    },
    GcsServiceAccount {
        service_account: ServiceAccountJson,
    },
    AzureAccessKey {
        access_key: String,
    },
    AzureBearerToken {
        bearer_token: String,
    },
    AzureClientSecret {
        client_id: String,
        client_secret: String,
        tenant_id: String,
    },
}

impl CredentialConfig {
    pub(crate) fn credential_type(&self) -> &'static str {
        match self {
            Self::DefaultChain => "default_chain",
            Self::Anonymous => "anonymous",
            Self::S3AccessKey { .. } => "s3_access_key",
            Self::GcsServiceAccount { .. } => "gcs_service_account",
            Self::AzureAccessKey { .. } => "azure_access_key",
            Self::AzureBearerToken { .. } => "azure_bearer_token",
            Self::AzureClientSecret { .. } => "azure_client_secret",
        }
    }

    pub(crate) fn uses_default_chain(&self) -> bool {
        matches!(self, Self::DefaultChain)
    }

    pub(crate) fn parse(
        value: Value,
        location: &StorageLocation,
    ) -> Result<Self, StorageVolumeError> {
        let credential: Self = serde_json::from_value(value)
            .map_err(CredentialValidationError::InvalidShape)?;
        credential.validate_for(location)?;
        Ok(credential)
    }

    pub(crate) fn validate_for(
        &self,
        location: &StorageLocation,
    ) -> Result<(), CredentialValidationError> {
        let valid_provider = matches!(
            (location, self),
            (_, Self::DefaultChain)
                | (StorageLocation::S3 { .. }, Self::Anonymous)
                | (StorageLocation::Gcs { .. }, Self::Anonymous)
                | (StorageLocation::S3 { .. }, Self::S3AccessKey { .. })
                | (StorageLocation::Gcs { .. }, Self::GcsServiceAccount { .. })
                | (StorageLocation::Azure { .. }, Self::AzureAccessKey { .. })
                | (StorageLocation::Azure { .. }, Self::AzureBearerToken { .. })
                | (
                    StorageLocation::Azure { .. },
                    Self::AzureClientSecret { .. }
                )
        );
        if !valid_provider {
            return Err(CredentialValidationError::ProviderMismatch);
        }
        let non_empty = match self {
            Self::S3AccessKey {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                !access_key_id.is_empty()
                    && !secret_access_key.is_empty()
                    && session_token.as_ref().is_none_or(|value| !value.is_empty())
            }
            Self::GcsServiceAccount { service_account } => {
                !service_account.is_empty()
            }
            Self::AzureAccessKey { access_key } => !access_key.is_empty(),
            Self::AzureBearerToken { bearer_token } => !bearer_token.is_empty(),
            Self::AzureClientSecret {
                client_id,
                client_secret,
                tenant_id,
            } => {
                !client_id.is_empty()
                    && !client_secret.is_empty()
                    && !tenant_id.is_empty()
            }
            Self::DefaultChain | Self::Anonymous => true,
        };
        if !non_empty {
            return Err(CredentialValidationError::EmptyFields);
        }
        Ok(())
    }
}

impl fmt::Debug for CredentialConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialConfig")
            .field("type", &self.credential_type())
            .finish_non_exhaustive()
    }
}
