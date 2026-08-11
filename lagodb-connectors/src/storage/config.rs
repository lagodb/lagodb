//! ForeignServer/UserMapping option ownership and storage backend mapping.

use pg_lakebase_core::storage::foreign::{
    ForeignOptionView, StorageConfigProvider, StorageOptions,
};
use pg_lakebase_storage::{
    AzureStoreConfig, GcsStoreConfig, S3CompatibleStoreConfig, S3StoreConfig,
    SecretString, StoreConfig,
};
use pgrx::pg_sys;
use url::Url;

use super::uri::{StorageProvider, StorageScope};
use crate::error::ConnectorError;

pub(crate) struct ConnectorStoreConfig;

impl StorageConfigProvider for ConnectorStoreConfig {
    type Error = ConnectorError;

    fn build_store_config(
        options: StorageOptions<'_>,
    ) -> Result<StoreConfig, Self::Error> {
        let server = ServerOptions::from_view(options.server())?;
        let mapping = MappingOptions::from_view(options.mapping())?;
        server.into_store_config(mapping)
    }
}

#[derive(Default)]
pub(crate) struct ServerOptions<'a> {
    provider: Option<&'a str>,
    scope: Option<&'a str>,
    region: Option<&'a str>,
    endpoint: Option<&'a str>,
    allow_http: Option<&'a str>,
    virtual_hosted_style_request: Option<&'a str>,
    skip_signature: Option<&'a str>,
    base_url: Option<&'a str>,
    account: Option<&'a str>,
    use_emulator: Option<&'a str>,
}

impl<'a> ServerOptions<'a> {
    pub(crate) fn from_view(
        options: ForeignOptionView<'a>,
    ) -> Result<Self, ConnectorError> {
        let mut parsed = Self::default();
        for option in options.iter() {
            let name = option.name().to_str().map_err(|_| {
                ConnectorError::invalid_option("server option", "must be valid UTF-8")
            })?;
            let value = option.value_str().map_err(|_| {
                ConnectorError::invalid_option(name, "must be valid UTF-8")
            })?;
            match name {
                "provider" => Self::set(&mut parsed.provider, name, value)?,
                "scope" => Self::set(&mut parsed.scope, name, value)?,
                "region" => Self::set(&mut parsed.region, name, value)?,
                "endpoint" => Self::set(&mut parsed.endpoint, name, value)?,
                "allow_http" => Self::set(&mut parsed.allow_http, name, value)?,
                "virtual_hosted_style_request" => {
                    Self::set(&mut parsed.virtual_hosted_style_request, name, value)?
                }
                "skip_signature" => {
                    Self::set(&mut parsed.skip_signature, name, value)?
                }
                "base_url" => Self::set(&mut parsed.base_url, name, value)?,
                "account" => Self::set(&mut parsed.account, name, value)?,
                "use_emulator" => Self::set(&mut parsed.use_emulator, name, value)?,
                _ => {
                    return Err(ConnectorError::invalid_option(
                        name,
                        "is not a supported foreign server option",
                    ));
                }
            }
        }
        Ok(parsed)
    }

    pub(crate) fn provider(&self) -> Result<Option<StorageProvider>, ConnectorError> {
        self.provider.map(StorageProvider::parse).transpose()
    }

    pub(crate) fn scope(&self) -> Option<&str> {
        self.scope
    }

    fn set(
        slot: &mut Option<&'a str>,
        name: &str,
        value: &'a str,
    ) -> Result<(), ConnectorError> {
        if slot.replace(value).is_some() {
            return Err(ConnectorError::invalid_option(
                name,
                "must not be specified more than once",
            ));
        }
        Ok(())
    }

    fn into_store_config(
        self,
        mapping: MappingOptions<'a>,
    ) -> Result<StoreConfig, ConnectorError> {
        let provider = self.provider()?.ok_or_else(|| {
            ConnectorError::invalid_option("provider", "is required")
        })?;
        mapping.validate_provider_independent()?;
        if let Some(scope) = self.scope {
            StorageScope::parse(scope, provider)?;
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
                reject_skip_signature_credentials(skip_signature, &mapping)?;
                S3StoreConfig {
                    region: self.region.map(str::to_owned),
                    endpoint: self.endpoint.map(str::to_owned),
                    access_key_id: mapping.access_key_id.map(SecretString::new),
                    secret_access_key: mapping
                        .secret_access_key
                        .map(SecretString::new),
                    token: mapping.token.map(SecretString::new),
                    allow_http,
                    virtual_hosted_style_request: virtual_hosted,
                    skip_signature,
                }
                .into_canonical()
            }
            StorageProvider::S3Compatible => {
                reject_server_option(self.base_url, "base_url")?;
                reject_server_option(self.account, "account")?;
                reject_server_option(self.use_emulator, "use_emulator")?;
                reject_mapping_options(&mapping, provider)?;
                reject_skip_signature_credentials(skip_signature, &mapping)?;
                let endpoint = self.endpoint.ok_or_else(|| {
                    ConnectorError::invalid_option(
                        "endpoint",
                        "is required for s3_compatible",
                    )
                })?;
                StoreConfig::S3Compatible(S3CompatibleStoreConfig {
                    endpoint: endpoint.to_owned(),
                    region: self.region.map(str::to_owned),
                    access_key_id: mapping.access_key_id.map(SecretString::new),
                    secret_access_key: mapping
                        .secret_access_key
                        .map(SecretString::new),
                    token: mapping.token.map(SecretString::new),
                    allow_http,
                    virtual_hosted_style_request: virtual_hosted,
                    skip_signature,
                })
            }
            StorageProvider::Gcs => {
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
                    skip_signature,
                })
            }
            StorageProvider::Azure => {
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
                    client_id: mapping.client_id.map(str::to_owned),
                    client_secret: mapping.client_secret.map(SecretString::new),
                    tenant_id: mapping.tenant_id.map(str::to_owned),
                    allow_http,
                    use_emulator,
                })
            }
        };
        config.validate()?;
        Ok(config)
    }

    fn validate_endpoint_transport(
        &self,
        allow_http: bool,
    ) -> Result<(), ConnectorError> {
        let Some(endpoint) = self.endpoint else {
            return Ok(());
        };
        let parsed = Url::parse(endpoint).map_err(|_| {
            ConnectorError::invalid_option("endpoint", "must be a valid HTTPS URL")
        })?;
        let valid_scheme =
            parsed.scheme() == "https" || (allow_http && parsed.scheme() == "http");
        if !valid_scheme {
            return Err(ConnectorError::invalid_option(
                "endpoint",
                "must use HTTPS unless allow_http is true",
            ));
        }
        if parsed.host_str().is_none() {
            return Err(ConnectorError::invalid_option(
                "endpoint",
                "must include a host",
            ));
        }
        if endpoint_has_userinfo(endpoint)
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(ConnectorError::invalid_option(
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

#[derive(Default)]
struct MappingOptions<'a> {
    access_key_id: Option<&'a str>,
    secret_access_key: Option<&'a str>,
    token: Option<&'a str>,
    service_account_path: Option<&'a str>,
    service_account_key: Option<&'a str>,
    application_credentials_path: Option<&'a str>,
    access_key: Option<&'a str>,
    bearer_token: Option<&'a str>,
    client_id: Option<&'a str>,
    client_secret: Option<&'a str>,
    tenant_id: Option<&'a str>,
}

impl<'a> MappingOptions<'a> {
    fn parse(options: &'a [Option<String>]) -> Result<Self, ConnectorError> {
        let mut parsed = Self::default();
        for option in options.iter().flatten() {
            let (name, value) = option.split_once('=').ok_or_else(|| {
                ConnectorError::invalid_option(
                    "user mapping option",
                    "expected name=value",
                )
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    fn from_view(options: ForeignOptionView<'a>) -> Result<Self, ConnectorError> {
        let mut parsed = Self::default();
        for option in options.iter() {
            let name = option.name().to_str().map_err(|_| {
                ConnectorError::invalid_option(
                    "user mapping option",
                    "must be valid UTF-8",
                )
            })?;
            let value = option.value_str().map_err(|_| {
                ConnectorError::invalid_option(name, "must be valid UTF-8")
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    fn set(&mut self, name: &str, value: &'a str) -> Result<(), ConnectorError> {
        let slot = match name {
            "access_key_id" => &mut self.access_key_id,
            "secret_access_key" => &mut self.secret_access_key,
            "token" => &mut self.token,
            "service_account_path" => &mut self.service_account_path,
            "service_account_key" => &mut self.service_account_key,
            "application_credentials_path" => &mut self.application_credentials_path,
            "access_key" => &mut self.access_key,
            "bearer_token" => &mut self.bearer_token,
            "client_id" => &mut self.client_id,
            "client_secret" => &mut self.client_secret,
            "tenant_id" => &mut self.tenant_id,
            _ => {
                return Err(ConnectorError::invalid_option(
                    name,
                    "is not a supported user mapping option",
                ));
            }
        };
        if slot.replace(value).is_some() {
            return Err(ConnectorError::invalid_option(
                name,
                "must not be specified more than once",
            ));
        }
        Ok(())
    }

    fn validate_provider_independent(&self) -> Result<(), ConnectorError> {
        let has_s3 = self.access_key_id.is_some()
            || self.secret_access_key.is_some()
            || self.token.is_some();
        let gcs_sources = usize::from(self.service_account_path.is_some())
            + usize::from(self.service_account_key.is_some())
            + usize::from(self.application_credentials_path.is_some());
        let has_gcs = gcs_sources != 0;
        let azure_secret_fields = usize::from(self.client_id.is_some())
            + usize::from(self.client_secret.is_some())
            + usize::from(self.tenant_id.is_some());
        let has_azure = self.access_key.is_some()
            || self.bearer_token.is_some()
            || azure_secret_fields != 0;

        if usize::from(has_s3) + usize::from(has_gcs) + usize::from(has_azure) > 1 {
            return Err(ConnectorError::invalid_option(
                "user mapping credentials",
                "credential sources for different providers cannot be combined",
            ));
        }
        if self.access_key_id.is_some() != self.secret_access_key.is_some() {
            return Err(ConnectorError::invalid_option(
                "access_key_id/secret_access_key",
                "must be specified together",
            ));
        }
        if self.token.is_some()
            && (self.access_key_id.is_none() || self.secret_access_key.is_none())
        {
            return Err(ConnectorError::invalid_option(
                "token",
                "requires access_key_id and secret_access_key",
            ));
        }
        if gcs_sources > 1 {
            return Err(ConnectorError::invalid_option(
                "GCS credential source",
                "only one of service_account_path, service_account_key, or application_credentials_path is allowed",
            ));
        }
        if azure_secret_fields != 0 && azure_secret_fields != 3 {
            return Err(ConnectorError::invalid_option(
                "client_id/client_secret/tenant_id",
                "must be specified together",
            ));
        }
        if usize::from(self.access_key.is_some())
            + usize::from(self.bearer_token.is_some())
            + usize::from(azure_secret_fields == 3)
            > 1
        {
            return Err(ConnectorError::invalid_option(
                "Azure credential source",
                "access_key, bearer_token, and client-secret auth are mutually exclusive",
            ));
        }
        Ok(())
    }
}

fn parse_bool(value: Option<&str>, name: &str) -> Result<bool, ConnectorError> {
    match value {
        None => Ok(false),
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(ConnectorError::invalid_option(
            name,
            "must be true or false",
        )),
    }
}

fn reject_server_option(
    value: Option<&str>,
    name: &'static str,
) -> Result<(), ConnectorError> {
    if value.is_some() {
        return Err(ConnectorError::invalid_option(
            name,
            "is not valid for the selected provider",
        ));
    }
    Ok(())
}

fn reject_mapping_options(
    mapping: &MappingOptions<'_>,
    provider: StorageProvider,
) -> Result<(), ConnectorError> {
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
        Err(ConnectorError::invalid_option(
            name,
            "is not valid for the selected provider",
        ))
    })
}

fn reject_skip_signature_credentials(
    skip_signature: bool,
    mapping: &MappingOptions<'_>,
) -> Result<(), ConnectorError> {
    if skip_signature
        && (mapping.access_key_id.is_some()
            || mapping.secret_access_key.is_some()
            || mapping.token.is_some()
            || mapping.service_account_path.is_some()
            || mapping.service_account_key.is_some()
            || mapping.application_credentials_path.is_some())
    {
        return Err(ConnectorError::invalid_option(
            "skip_signature",
            "cannot be combined with an explicit credential",
        ));
    }
    Ok(())
}

pub(crate) fn validate_storage_options(
    options: &[Option<String>],
    catalog: Option<pg_sys::Oid>,
) -> Result<(), ConnectorError> {
    match catalog {
        Some(catalog) if catalog == pg_sys::ForeignDataWrapperRelationId => {
            if options.iter().flatten().next().is_some() {
                return Err(ConnectorError::invalid_option(
                    "FDW option",
                    "wrapper-level options are not supported",
                ));
            }
        }
        Some(catalog) if catalog == pg_sys::ForeignServerRelationId => {
            let mut server = ServerOptions::default();
            for option in options.iter().flatten() {
                let (name, value) = option.split_once('=').ok_or_else(|| {
                    ConnectorError::invalid_option(
                        "server option",
                        "expected name=value",
                    )
                })?;
                match name {
                    "provider" => {
                        ServerOptions::set(&mut server.provider, name, value)?
                    }
                    "scope" => ServerOptions::set(&mut server.scope, name, value)?,
                    "region" => ServerOptions::set(&mut server.region, name, value)?,
                    "endpoint" => {
                        ServerOptions::set(&mut server.endpoint, name, value)?
                    }
                    "allow_http" => {
                        ServerOptions::set(&mut server.allow_http, name, value)?
                    }
                    "virtual_hosted_style_request" => ServerOptions::set(
                        &mut server.virtual_hosted_style_request,
                        name,
                        value,
                    )?,
                    "skip_signature" => {
                        ServerOptions::set(&mut server.skip_signature, name, value)?
                    }
                    "base_url" => {
                        ServerOptions::set(&mut server.base_url, name, value)?
                    }
                    "account" => {
                        ServerOptions::set(&mut server.account, name, value)?
                    }
                    "use_emulator" => {
                        ServerOptions::set(&mut server.use_emulator, name, value)?
                    }
                    _ => {
                        return Err(ConnectorError::invalid_option(
                            name,
                            "is not a supported foreign server option",
                        ));
                    }
                }
            }
            server.into_store_config(MappingOptions::default())?;
        }
        Some(catalog) if catalog == pg_sys::UserMappingRelationId => {
            MappingOptions::parse(options)?.validate_provider_independent()?;
        }
        _ => {
            if options.iter().flatten().next().is_some() {
                return Err(ConnectorError::invalid_option(
                    "FDW option",
                    "options are not supported at this catalog layer",
                ));
            }
        }
    }
    Ok(())
}
