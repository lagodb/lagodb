//! ForeignServer/UserMapping option ownership and storage backend mapping.

use crate::storage::foreign::{
    ForeignOptionView, StorageConfigProvider, StorageOptions,
};
use lagodb_storage::{
    AzureStoreConfig, GcsStoreConfig, S3CompatibleStoreConfig, S3StoreConfig,
    SecretString, StoreConfig,
};
use url::Url;

use super::mapping::MappingOptions;
use super::{
    ObjectScheme, ObjectUriPrefix, StorageProfileError, StorageProvider, StorageScope,
};

pub struct StorageProfileConfig;

impl StorageConfigProvider for StorageProfileConfig {
    type Error = StorageProfileError;

    fn build_store_config(
        options: StorageOptions<'_>,
    ) -> Result<StoreConfig, Self::Error> {
        let server = StorageServerOptions::from_view(options.server())?;
        let mapping = MappingOptions::from_view(options.mapping())?;
        let config = server.into_profile(mapping)?;
        config.validate()?;
        Ok(config)
    }
}

#[derive(Default)]
pub struct StorageServerOptions<'a> {
    pub(super) provider: Option<&'a str>,
    pub(super) scope: Option<&'a str>,
    pub(super) region: Option<&'a str>,
    pub(super) endpoint: Option<&'a str>,
    pub(super) allow_http: Option<&'a str>,
    pub(super) virtual_hosted_style_request: Option<&'a str>,
    pub(super) skip_signature: Option<&'a str>,
    pub(super) base_url: Option<&'a str>,
    pub(super) account: Option<&'a str>,
    pub(super) use_emulator: Option<&'a str>,
}

impl<'a> StorageServerOptions<'a> {
    pub fn from_view(
        options: ForeignOptionView<'a>,
    ) -> Result<Self, StorageProfileError> {
        let mut parsed = Self::default();
        for option in options.iter() {
            let name = option.name().to_str().map_err(|_| {
                StorageProfileError::invalid_option(
                    "server option",
                    "must be valid UTF-8",
                )
            })?;
            let value = option.value_str().map_err(|_| {
                StorageProfileError::invalid_option(name, "must be valid UTF-8")
            })?;
            parsed.set_option(name, value)?;
        }
        Ok(parsed)
    }

    pub(super) fn set_option(
        &mut self,
        name: &str,
        value: &'a str,
    ) -> Result<(), StorageProfileError> {
        match name {
            "provider" => Self::set(&mut self.provider, name, value)?,
            "scope" => Self::set(&mut self.scope, name, value)?,
            "region" => Self::set(&mut self.region, name, value)?,
            "endpoint" => Self::set(&mut self.endpoint, name, value)?,
            "allow_http" => Self::set(&mut self.allow_http, name, value)?,
            "virtual_hosted_style_request" => {
                Self::set(&mut self.virtual_hosted_style_request, name, value)?
            }
            "skip_signature" => Self::set(&mut self.skip_signature, name, value)?,
            "base_url" => Self::set(&mut self.base_url, name, value)?,
            "account" => Self::set(&mut self.account, name, value)?,
            "use_emulator" => Self::set(&mut self.use_emulator, name, value)?,
            _ => {
                return Err(StorageProfileError::invalid_option(
                    name,
                    "is not a supported foreign server option",
                ));
            }
        }
        Ok(())
    }

    pub fn provider(&self) -> Result<Option<StorageProvider>, StorageProfileError> {
        if let Some(provider) = self.provider {
            return StorageProvider::parse(provider).map(Some);
        }
        self.scope
            .map(|scope| {
                let scheme = ObjectUriPrefix::parse(scope)?.scheme();
                Ok(match scheme {
                    ObjectScheme::S3 => StorageProvider::S3,
                    ObjectScheme::Gcs => StorageProvider::Gcs,
                    ObjectScheme::Azure => StorageProvider::Azure,
                })
            })
            .transpose()
    }

    pub fn scope(&self) -> Option<&str> {
        self.scope
    }

    pub fn account(&self) -> Option<&str> {
        self.account
    }

    pub(super) fn set(
        slot: &mut Option<&'a str>,
        name: &str,
        value: &'a str,
    ) -> Result<(), StorageProfileError> {
        if slot.replace(value).is_some() {
            return Err(StorageProfileError::invalid_option(
                name,
                "must not be specified more than once",
            ));
        }
        Ok(())
    }

    pub(super) fn into_profile(
        self,
        mapping: MappingOptions<'a>,
    ) -> Result<StoreConfig, StorageProfileError> {
        let provider = self.provider()?.ok_or_else(|| {
            StorageProfileError::invalid_option(
                "provider",
                "is required when scope is absent",
            )
        })?;
        mapping.validate_provider_independent()?;
        if let Some(scope) = self.scope {
            StorageScope::parse(scope, provider, self.account)?;
        }
        let allow_http = parse_bool(self.allow_http, "allow_http")?;
        let virtual_hosted = parse_bool(
            self.virtual_hosted_style_request,
            "virtual_hosted_style_request",
        )?;
        let skip_signature = parse_bool(self.skip_signature, "skip_signature")?;
        let use_emulator = parse_bool(self.use_emulator, "use_emulator")?;
        self.validate_endpoint_transport(allow_http || use_emulator)?;

        let config = match provider {
            StorageProvider::S3 => {
                reject_server_option(self.base_url, "base_url")?;
                reject_server_option(self.account, "account")?;
                reject_server_option(self.use_emulator, "use_emulator")?;
                reject_mapping_options(&mapping, provider)?;
                S3StoreConfig {
                    region: self.region.map(str::to_owned),
                    endpoint: self.endpoint.map(str::to_owned),
                    authentication: mapping
                        .aws_options()
                        .into_authentication(skip_signature)?,
                    allow_http,
                    virtual_hosted_style_request: virtual_hosted,
                    encryption: None,
                }
                .into_canonical()
            }
            StorageProvider::S3Compatible => {
                reject_server_option(self.base_url, "base_url")?;
                reject_server_option(self.account, "account")?;
                reject_server_option(self.use_emulator, "use_emulator")?;
                reject_mapping_options(&mapping, provider)?;
                let endpoint = self.endpoint.ok_or_else(|| {
                    StorageProfileError::invalid_option(
                        "endpoint",
                        "is required for s3_compatible",
                    )
                })?;
                StoreConfig::S3Compatible(S3CompatibleStoreConfig {
                    endpoint: endpoint.to_owned(),
                    region: self.region.map(str::to_owned),
                    authentication: mapping
                        .aws_options()
                        .into_authentication(skip_signature)?,
                    allow_http,
                    virtual_hosted_style_request: virtual_hosted,
                    encryption: None,
                })
            }
            StorageProvider::Gcs => {
                mapping.reject_aws_options()?;
                reject_server_option(self.region, "region")?;
                reject_server_option(self.endpoint, "endpoint")?;
                reject_server_option(self.account, "account")?;
                reject_server_option(self.allow_http, "allow_http")?;
                reject_server_option(
                    self.virtual_hosted_style_request,
                    "virtual_hosted_style_request",
                )?;
                reject_server_option(self.use_emulator, "use_emulator")?;
                reject_mapping_options(&mapping, provider)?;
                reject_skip_signature_credentials(skip_signature, &mapping)?;
                StoreConfig::Gcs(GcsStoreConfig {
                    base_url: self.base_url.map(str::to_owned),
                    service_account_path: mapping
                        .service_account_path
                        .map(str::to_owned),
                    service_account_key: mapping
                        .service_account_key
                        .map(SecretString::new),
                    application_credentials_path: mapping
                        .application_credentials_path
                        .map(str::to_owned),
                    bearer_token: None,
                    skip_signature,
                })
            }
            StorageProvider::Azure => {
                mapping.reject_aws_options()?;
                reject_server_option(self.region, "region")?;
                reject_server_option(
                    self.virtual_hosted_style_request,
                    "virtual_hosted_style_request",
                )?;
                reject_server_option(self.skip_signature, "skip_signature")?;
                reject_mapping_options(&mapping, provider)?;
                StoreConfig::Azure(AzureStoreConfig {
                    account: self.account.map(str::to_owned),
                    endpoint: self.endpoint.map(str::to_owned),
                    access_key: mapping.access_key.map(SecretString::new),
                    bearer_token: mapping.bearer_token.map(SecretString::new),
                    sas_token: None,
                    client_id: mapping.client_id.map(str::to_owned),
                    client_secret: mapping.client_secret.map(SecretString::new),
                    tenant_id: mapping.tenant_id.map(str::to_owned),
                    authority_host: None,
                    allow_http,
                    use_emulator,
                })
            }
        };
        Ok(config)
    }

    fn validate_endpoint_transport(
        &self,
        allow_http: bool,
    ) -> Result<(), StorageProfileError> {
        let Some(endpoint) = self.endpoint else {
            return Ok(());
        };
        let parsed = Url::parse(endpoint).map_err(|_| {
            StorageProfileError::invalid_option(
                "endpoint",
                "must be a valid HTTPS URL",
            )
        })?;
        let valid_scheme =
            parsed.scheme() == "https" || (allow_http && parsed.scheme() == "http");
        if !valid_scheme {
            return Err(StorageProfileError::invalid_option(
                "endpoint",
                "must use HTTPS unless allow_http is true",
            ));
        }
        if parsed.host_str().is_none() {
            return Err(StorageProfileError::invalid_option(
                "endpoint",
                "must include a host",
            ));
        }
        if endpoint_has_userinfo(endpoint)
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(StorageProfileError::invalid_option(
                "endpoint",
                "must not contain userinfo, query, or fragment",
            ));
        }
        Ok(())
    }
}

fn endpoint_has_userinfo(endpoint: &str) -> bool {
    let Some(scheme_end) = endpoint.find("://") else {
        return false;
    };
    let authority = &endpoint[(scheme_end + 3)..];
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    authority[..authority_end].contains('@')
}

fn parse_bool(value: Option<&str>, name: &str) -> Result<bool, StorageProfileError> {
    match value {
        None => Ok(false),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(StorageProfileError::invalid_option(
            name,
            "must be true or false",
        )),
    }
}

fn reject_server_option(
    value: Option<&str>,
    name: &'static str,
) -> Result<(), StorageProfileError> {
    if value.is_some() {
        return Err(StorageProfileError::invalid_option(
            name,
            "is not valid for the selected provider",
        ));
    }
    Ok(())
}

fn reject_mapping_options(
    mapping: &MappingOptions<'_>,
    provider: StorageProvider,
) -> Result<(), StorageProfileError> {
    let invalid = match provider {
        StorageProvider::S3 | StorageProvider::S3Compatible => [
            (mapping.service_account_path, "service_account_path"),
            (mapping.service_account_key, "service_account_key"),
            (
                mapping.application_credentials_path,
                "application_credentials_path",
            ),
            (mapping.access_key, "access_key"),
            (mapping.bearer_token, "bearer_token"),
            (mapping.client_id, "client_id"),
            (mapping.client_secret, "client_secret"),
            (mapping.tenant_id, "tenant_id"),
        ]
        .into_iter()
        .find_map(|(value, name)| value.map(|_| name)),
        StorageProvider::Gcs => [
            (mapping.access_key_id, "access_key_id"),
            (mapping.secret_access_key, "secret_access_key"),
            (mapping.token, "token"),
            (mapping.access_key, "access_key"),
            (mapping.bearer_token, "bearer_token"),
            (mapping.client_id, "client_id"),
            (mapping.client_secret, "client_secret"),
            (mapping.tenant_id, "tenant_id"),
        ]
        .into_iter()
        .find_map(|(value, name)| value.map(|_| name)),
        StorageProvider::Azure => [
            (mapping.access_key_id, "access_key_id"),
            (mapping.secret_access_key, "secret_access_key"),
            (mapping.token, "token"),
            (mapping.service_account_path, "service_account_path"),
            (mapping.service_account_key, "service_account_key"),
            (
                mapping.application_credentials_path,
                "application_credentials_path",
            ),
        ]
        .into_iter()
        .find_map(|(value, name)| value.map(|_| name)),
    };
    invalid.map_or(Ok(()), |name| {
        Err(StorageProfileError::invalid_option(
            name,
            "is not valid for the selected provider",
        ))
    })
}

fn reject_skip_signature_credentials(
    skip_signature: bool,
    mapping: &MappingOptions<'_>,
) -> Result<(), StorageProfileError> {
    if skip_signature
        && (mapping.aws_options().is_configured()
            || mapping.service_account_path.is_some()
            || mapping.service_account_key.is_some()
            || mapping.application_credentials_path.is_some())
    {
        return Err(StorageProfileError::invalid_option(
            "skip_signature",
            "cannot be combined with an explicit credential",
        ));
    }
    Ok(())
}
