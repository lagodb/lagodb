//! Azure connection resolution; endpoint and authentication have separate precedence.

use std::borrow::Cow;

use iceberg_lite::Result;
use lagodb_storage::{AzureStoreConfig, StoreConfig};

use super::super::routes::ObjectProvider;
use super::{AZURE_PROPERTIES, ProviderConfig};

impl<'a> ProviderConfig<'a> {
    pub(super) fn azure(
        &self,
        account_from_uri: Option<&str>,
    ) -> Result<Cow<'a, StoreConfig>> {
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
            config.use_emulator = false;
            config.allow_http = config
                .endpoint
                .as_deref()
                .is_some_and(|endpoint| endpoint.starts_with("http://"));
        }
        if let (Some(identity), _) = self.identity(ObjectProvider::Azure) {
            config.access_key = identity.secret("adls.account-key");
            config.bearer_token = identity.secret("adls.bearer-token");
            config.sas_token = identity.secret("adls.sas-token");
            config.client_id = identity.value("adls.client-id");
            config.client_secret = identity.secret("adls.client-secret");
            config.tenant_id = identity.value("adls.tenant-id");
        }
        // Authority selects the authentication environment independently of identity.
        if let Some(authority_host) = self.value("adls.authority-host") {
            config.authority_host = Some(authority_host);
        }
        Ok(Cow::Owned(StoreConfig::Azure(config)))
    }
}
