use super::defs;
use crate::options::schema;
use pg_lakebase_storage::{
    AzureStoreConfig, GcsStoreConfig, S3StoreConfig, SecretString, StorageError,
    StoreConfig, StoreId,
};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TablespaceStorageError {
    #[error(
        "unsupported storage protocol '{protocol}' for tablespace '{tablespace}'"
    )]
    UnsupportedProtocol {
        tablespace: String,
        protocol: String,
    },

    #[error(
        "distributed tablespace '{tablespace}' using {protocol} requires option '{option}'"
    )]
    MissingRequiredOption {
        tablespace: String,
        protocol: &'static str,
        option: &'static str,
    },

    #[error(
        "option '{option}' is not supported for {protocol} tablespace '{tablespace}'"
    )]
    UnsupportedOption {
        tablespace: String,
        protocol: &'static str,
        option: String,
    },

    #[error(
        "option '{option}' is specified more than once for tablespace '{tablespace}'"
    )]
    DuplicateOption { tablespace: String, option: String },

    #[error(
        "invalid value for option '{option}' on tablespace '{tablespace}': {message}"
    )]
    InvalidOption {
        tablespace: String,
        option: &'static str,
        message: String,
    },

    #[error("invalid storage config for tablespace '{tablespace}': {source}")]
    InvalidStoreConfig {
        tablespace: String,
        #[source]
        source: StorageError,
    },

    #[error("invalid storage store id for tablespace '{tablespace}': {source}")]
    InvalidStoreId {
        tablespace: String,
        #[source]
        source: StorageError,
    },
}

pub(crate) fn store_id_from_tablespace_name(
    tablespace: &str,
) -> Result<StoreId, TablespaceStorageError> {
    StoreId::new(tablespace).map_err(|source| {
        TablespaceStorageError::InvalidStoreId {
            tablespace: tablespace.to_string(),
            source,
        }
    })
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum StorageProtocol {
    S3,
    Gcs,
    Azure,
}

impl StorageProtocol {
    pub(crate) const ALL: &[&str] = &["s3", "gcs", "azure"];

    fn parse(tablespace: &str, value: &str) -> Result<Self, TablespaceStorageError> {
        match value {
            "s3" => Ok(Self::S3),
            "gcs" => Ok(Self::Gcs),
            "azure" => Ok(Self::Azure),
            unknown => Err(TablespaceStorageError::UnsupportedProtocol {
                tablespace: tablespace.to_string(),
                protocol: unknown.to_string(),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gcs",
            Self::Azure => "azure",
        }
    }

    pub fn url_scheme(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gs",
            Self::Azure => "az",
        }
    }

    fn namespace_option_name(self) -> &'static str {
        match self {
            Self::S3 | Self::Gcs => defs::OPT_BUCKET,
            Self::Azure => defs::OPT_CONTAINER,
        }
    }

    fn supported_options(self) -> &'static [&'static str] {
        match self {
            Self::S3 => &[
                defs::OPT_PROTOCOL,
                defs::OPT_BUCKET,
                defs::OPT_REGION,
                defs::OPT_ENDPOINT,
                defs::OPT_ALLOW_HTTP,
                defs::OPT_ACCESS_KEY_ID,
                defs::OPT_SECRET_ACCESS_KEY,
                defs::OPT_TOKEN,
                defs::OPT_VIRTUAL_HOSTED_STYLE_REQUEST,
                defs::OPT_SKIP_SIGNATURE,
            ],
            Self::Gcs => &[
                defs::OPT_PROTOCOL,
                defs::OPT_BUCKET,
                defs::OPT_BASE_URL,
                defs::OPT_SERVICE_ACCOUNT_PATH,
                defs::OPT_SERVICE_ACCOUNT_KEY,
                defs::OPT_APPLICATION_CREDENTIALS_PATH,
                defs::OPT_SKIP_SIGNATURE,
            ],
            Self::Azure => &[
                defs::OPT_PROTOCOL,
                defs::OPT_CONTAINER,
                defs::OPT_ACCOUNT,
                defs::OPT_ENDPOINT,
                defs::OPT_ACCESS_KEY,
                defs::OPT_BEARER_TOKEN,
                defs::OPT_CLIENT_ID,
                defs::OPT_CLIENT_SECRET,
                defs::OPT_TENANT_ID,
                defs::OPT_ALLOW_HTTP,
                defs::OPT_USE_EMULATOR,
            ],
        }
    }
}

#[derive(Debug, Clone)]
pub enum TablespaceStorage {
    S3 {
        bucket: String,
        config: S3StoreConfig,
    },
    Gcs {
        bucket: String,
        config: GcsStoreConfig,
    },
    Azure {
        container: String,
        config: AzureStoreConfig,
    },
}

impl TablespaceStorage {
    pub fn from_catalog_options(
        tablespace: &str,
        options_vec: Vec<String>,
    ) -> Result<Option<Self>, TablespaceStorageError> {
        let options = CatalogStorageOptions::parse(tablespace, &options_vec)?;
        if options.is_empty() {
            return Ok(None);
        }

        let protocol = options.protocol()?;
        options.reject_unsupported_options(protocol)?;

        let storage = match protocol {
            StorageProtocol::S3 => Self::from_s3_options(&options)?,
            StorageProtocol::Gcs => Self::from_gcs_options(&options)?,
            StorageProtocol::Azure => Self::from_azure_options(&options)?,
        };
        storage.validate_store_config(tablespace)?;

        Ok(Some(storage))
    }

    pub fn protocol(&self) -> StorageProtocol {
        match self {
            Self::S3 { .. } => StorageProtocol::S3,
            Self::Gcs { .. } => StorageProtocol::Gcs,
            Self::Azure { .. } => StorageProtocol::Azure,
        }
    }

    pub fn protocol_name(&self) -> &'static str {
        self.protocol().as_str()
    }

    pub fn url_scheme(&self) -> &'static str {
        self.protocol().url_scheme()
    }

    pub fn object_namespace(&self) -> &str {
        match self {
            Self::S3 { bucket, .. } | Self::Gcs { bucket, .. } => bucket,
            Self::Azure { container, .. } => container,
        }
    }

    pub fn namespace_option_name(&self) -> &'static str {
        self.protocol().namespace_option_name()
    }

    pub fn base_url(&self) -> String {
        format!("{}://{}", self.url_scheme(), self.object_namespace())
    }

    pub fn store_config(&self) -> StoreConfig {
        match self {
            Self::S3 { config, .. } => StoreConfig::S3(config.clone()),
            Self::Gcs { config, .. } => StoreConfig::Gcs(config.clone()),
            Self::Azure { config, .. } => StoreConfig::Azure(config.clone()),
        }
    }

    fn from_s3_options(
        options: &CatalogStorageOptions<'_>,
    ) -> Result<Self, TablespaceStorageError> {
        let bucket = options.required(StorageProtocol::S3, defs::OPT_BUCKET)?;
        let config = S3StoreConfig {
            region: Some(
                options
                    .optional(defs::OPT_REGION)
                    .unwrap_or(defs::DEFAULT_S3_REGION)
                    .to_string(),
            ),
            endpoint: options.owned(defs::OPT_ENDPOINT),
            access_key_id: options.secret(defs::OPT_ACCESS_KEY_ID),
            secret_access_key: options.secret(defs::OPT_SECRET_ACCESS_KEY),
            token: options.secret(defs::OPT_TOKEN),
            allow_http: options
                .bool_or_default(defs::OPT_ALLOW_HTTP, defs::DEFAULT_ALLOW_HTTP)?,
            virtual_hosted_style_request: options
                .bool_or_default(defs::OPT_VIRTUAL_HOSTED_STYLE_REQUEST, false)?,
            skip_signature: options
                .bool_or_default(defs::OPT_SKIP_SIGNATURE, false)?,
        };

        Ok(Self::S3 { bucket, config })
    }

    fn from_gcs_options(
        options: &CatalogStorageOptions<'_>,
    ) -> Result<Self, TablespaceStorageError> {
        let bucket = options.required(StorageProtocol::Gcs, defs::OPT_BUCKET)?;
        let config = GcsStoreConfig {
            base_url: options.owned(defs::OPT_BASE_URL),
            service_account_path: options.owned(defs::OPT_SERVICE_ACCOUNT_PATH),
            service_account_key: options.secret(defs::OPT_SERVICE_ACCOUNT_KEY),
            application_credentials_path: options
                .owned(defs::OPT_APPLICATION_CREDENTIALS_PATH),
            skip_signature: options
                .bool_or_default(defs::OPT_SKIP_SIGNATURE, false)?,
        };

        Ok(Self::Gcs { bucket, config })
    }

    fn from_azure_options(
        options: &CatalogStorageOptions<'_>,
    ) -> Result<Self, TablespaceStorageError> {
        let container =
            options.required(StorageProtocol::Azure, defs::OPT_CONTAINER)?;
        let config = AzureStoreConfig {
            account: options.owned(defs::OPT_ACCOUNT),
            endpoint: options.owned(defs::OPT_ENDPOINT),
            access_key: options.secret(defs::OPT_ACCESS_KEY),
            bearer_token: options.secret(defs::OPT_BEARER_TOKEN),
            client_id: options.owned(defs::OPT_CLIENT_ID),
            client_secret: options.secret(defs::OPT_CLIENT_SECRET),
            tenant_id: options.owned(defs::OPT_TENANT_ID),
            allow_http: options
                .bool_or_default(defs::OPT_ALLOW_HTTP, defs::DEFAULT_ALLOW_HTTP)?,
            use_emulator: options.bool_or_default(defs::OPT_USE_EMULATOR, false)?,
        };

        Ok(Self::Azure { container, config })
    }

    fn validate_store_config(
        &self,
        tablespace: &str,
    ) -> Result<(), TablespaceStorageError> {
        self.store_config().validate().map_err(|source| {
            TablespaceStorageError::InvalidStoreConfig {
                tablespace: tablespace.to_string(),
                source,
            }
        })
    }
}

struct CatalogStorageOptions<'a> {
    tablespace: &'a str,
    values: HashMap<&'a str, String>,
}

impl<'a> CatalogStorageOptions<'a> {
    fn parse(
        tablespace: &'a str,
        options_vec: &'a [String],
    ) -> Result<Self, TablespaceStorageError> {
        let mut values = HashMap::new();

        for option in options_vec {
            let Some((key, value)) = option.split_once('=') else {
                continue;
            };
            if !defs::is_tablespace_option(key) {
                continue;
            }
            if values.insert(key, value.to_string()).is_some() {
                return Err(TablespaceStorageError::DuplicateOption {
                    tablespace: tablespace.to_string(),
                    option: key.to_string(),
                });
            }
        }

        Ok(Self { tablespace, values })
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn protocol(&self) -> Result<StorageProtocol, TablespaceStorageError> {
        let value = self
            .optional(defs::OPT_PROTOCOL)
            .unwrap_or(defs::DEFAULT_PROTOCOL);
        StorageProtocol::parse(self.tablespace, value)
    }

    fn reject_unsupported_options(
        &self,
        protocol: StorageProtocol,
    ) -> Result<(), TablespaceStorageError> {
        for option in self.values.keys() {
            if !protocol.supported_options().contains(option) {
                return Err(TablespaceStorageError::UnsupportedOption {
                    tablespace: self.tablespace.to_string(),
                    protocol: protocol.as_str(),
                    option: (*option).to_string(),
                });
            }
        }

        Ok(())
    }

    fn optional(&self, key: &'static str) -> Option<&str> {
        self.values
            .get(key)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }

    fn owned(&self, key: &'static str) -> Option<String> {
        self.optional(key).map(str::to_string)
    }

    fn secret(&self, key: &'static str) -> Option<SecretString> {
        self.owned(key).map(SecretString::new)
    }

    fn required(
        &self,
        protocol: StorageProtocol,
        key: &'static str,
    ) -> Result<String, TablespaceStorageError> {
        self.owned(key)
            .ok_or_else(|| TablespaceStorageError::MissingRequiredOption {
                tablespace: self.tablespace.to_string(),
                protocol: protocol.as_str(),
                option: key,
            })
    }

    fn bool_or_default(
        &self,
        key: &'static str,
        default: bool,
    ) -> Result<bool, TablespaceStorageError> {
        let Some(value) = self.optional(key) else {
            return Ok(default);
        };

        schema::parse_bool(value).ok_or_else(|| {
            TablespaceStorageError::InvalidOption {
                tablespace: self.tablespace.to_string(),
                option: key,
                message: format!("invalid boolean value \"{}\"", value),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(
        tablespace_name: &str,
        options: &[&str],
    ) -> Result<Option<TablespaceStorage>, TablespaceStorageError> {
        TablespaceStorage::from_catalog_options(
            tablespace_name,
            options.iter().map(|option| option.to_string()).collect(),
        )
    }

    #[test]
    fn parses_s3_store_config() {
        let storage = parse(
            "lake_spc",
            &[
                "protocol=s3",
                "bucket=my-lake",
                "region=us-east-1",
                "allow_http=true",
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(storage.protocol_name(), "s3");
        assert_eq!(storage.url_scheme(), "s3");
        assert_eq!(storage.object_namespace(), "my-lake");
        assert_eq!(storage.base_url(), "s3://my-lake");
        assert!(matches!(
            storage.store_config(),
            StoreConfig::S3(S3StoreConfig {
                region: Some(region),
                allow_http: true,
                ..
            }) if region == "us-east-1"
        ));
    }

    #[test]
    fn parses_gcs_store_config() {
        let storage = parse(
            "gcs_spc",
            &[
                "protocol=gcs",
                "bucket=my-lake",
                "base_url=http://gcs.local",
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(storage.protocol_name(), "gcs");
        assert_eq!(storage.url_scheme(), "gs");
        assert_eq!(storage.object_namespace(), "my-lake");
        assert_eq!(storage.base_url(), "gs://my-lake");
        assert!(matches!(
            storage.store_config(),
            StoreConfig::Gcs(GcsStoreConfig {
                base_url: Some(base_url),
                ..
            }) if base_url == "http://gcs.local"
        ));
    }

    #[test]
    fn parses_azure_store_config() {
        let storage = parse(
            "azure_spc",
            &[
                "protocol=azure",
                "container=my-container",
                "account=my-account",
            ],
        )
        .unwrap()
        .unwrap();

        assert_eq!(storage.protocol_name(), "azure");
        assert_eq!(storage.url_scheme(), "az");
        assert_eq!(storage.object_namespace(), "my-container");
        assert_eq!(storage.base_url(), "az://my-container");
        assert!(matches!(
            storage.store_config(),
            StoreConfig::Azure(AzureStoreConfig {
                account: Some(account),
                ..
            }) if account == "my-account"
        ));
    }

    #[test]
    fn defaults_bucket_only_to_s3() {
        let storage = parse("lake_spc", &["bucket=my-lake"]).unwrap().unwrap();

        assert_eq!(storage.protocol_name(), "s3");
        assert_eq!(storage.object_namespace(), "my-lake");
    }

    #[test]
    fn missing_namespace_is_an_error() {
        let error = parse("lake_spc", &["protocol=gcs"]).unwrap_err();

        assert!(matches!(
            error,
            TablespaceStorageError::MissingRequiredOption {
                tablespace,
                protocol: "gcs",
                option: "bucket",
            } if tablespace == "lake_spc"
        ));
    }

    #[test]
    fn rejects_protocol_specific_option_mismatch() {
        let error = parse(
            "gcs_spc",
            &["protocol=gcs", "bucket=my-lake", "region=us-east-1"],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TablespaceStorageError::UnsupportedOption {
                tablespace,
                protocol: "gcs",
                option,
            } if tablespace == "gcs_spc" && option == "region"
        ));
    }

    #[test]
    fn gcs_rejects_s3_azure_endpoint_options() {
        let error = parse(
            "gcs_spc",
            &["protocol=gcs", "bucket=my-lake", "allow_http=true"],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TablespaceStorageError::UnsupportedOption {
                tablespace,
                protocol: "gcs",
                option,
            } if tablespace == "gcs_spc" && option == "allow_http"
        ));
    }

    #[test]
    fn native_tablespace_options_are_not_distributed_storage_options() {
        let opts = parse("local_spc", &["seq_page_cost=1.1"]).unwrap();

        assert!(opts.is_none());
    }
}
