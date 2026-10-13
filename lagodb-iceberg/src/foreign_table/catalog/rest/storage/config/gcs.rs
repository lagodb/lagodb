//! GCS connection resolution and complete response identity replacement.

use std::borrow::Cow;

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_storage::{GcsStoreConfig, SecretString, StoreConfig};

use super::super::routes::ObjectProvider;
use super::{GCS_PROPERTIES, ProviderConfig};

impl<'a> ProviderConfig<'a> {
    pub(super) fn gcs(&self) -> Result<Cow<'a, StoreConfig>> {
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
        let (identity, anonymous) = self.identity(ObjectProvider::Gcs);
        if let Some(identity) = identity {
            config.service_account_path = None;
            config.application_credentials_path = None;
            config.service_account_key = identity.gcs_credentials_json()?;
            config.bearer_token = identity.secret("gcs.oauth2.token");
            config.skip_signature = anonymous.unwrap_or(false);
        } else if let Some(anonymous) = anonymous {
            config.skip_signature = anonymous;
        }
        Ok(Cow::Owned(StoreConfig::Gcs(config)))
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
}
