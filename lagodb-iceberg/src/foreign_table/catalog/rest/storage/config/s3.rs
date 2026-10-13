//! S3 connection resolution and complete AWS identity parsing.

use std::borrow::Cow;

use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_storage::{
    AwsCredentialOptions, S3Authentication, S3Encryption, S3StoreConfig,
    SecretString, StoreConfig,
};
use md5::{Digest as _, Md5};

use super::super::routes::ObjectProvider;
use super::{
    ASSUME_ROLE_PROPERTIES, ProviderConfig, S3_CREDENTIAL_EXPIRY, S3_PROPERTIES,
};

impl<'a> ProviderConfig<'a> {
    pub(super) fn s3(&self) -> Result<Cow<'a, StoreConfig>> {
        self.ensure_supported("s3.", S3_PROPERTIES)?;
        self.ensure_supported("client.assume-role.", ASSUME_ROLE_PROPERTIES)?;
        if self.overrides.is_none()
            && !S3_PROPERTIES
                .iter()
                .any(|&key| key != S3_CREDENTIAL_EXPIRY && self.has(key))
            && !self.has("client.region")
            && !ASSUME_ROLE_PROPERTIES.iter().any(|&key| self.has(key))
            && let Some(profile @ (StoreConfig::S3(_) | StoreConfig::S3Compatible(_))) =
                self.profile
        {
            return Ok(Cow::Borrowed(profile));
        }
        let mut config = match self.profile {
            Some(StoreConfig::S3(config)) => config.clone(),
            Some(StoreConfig::S3Compatible(config)) => S3StoreConfig {
                region: config.region.clone(),
                endpoint: Some(config.endpoint.clone()),
                authentication: config.authentication.clone(),
                allow_http: config.allow_http,
                virtual_hosted_style_request: config.virtual_hosted_style_request,
                encryption: config.encryption.clone(),
            },
            Some(_) => return Err(self.profile_provider_mismatch("S3")),
            None => S3StoreConfig::default(),
        };
        if self.has("s3.endpoint") {
            config.endpoint = self.value("s3.endpoint");
            config.allow_http = config
                .endpoint
                .as_deref()
                .is_some_and(|endpoint| endpoint.starts_with("http://"));
        }
        if self.has("s3.path-style-access") {
            config.virtual_hosted_style_request =
                !self.truthy("s3.path-style-access");
        }
        let (identity, anonymous) = self.identity(ObjectProvider::S3);
        if let Some(identity) = identity {
            config.authentication =
                identity.s3_authentication(anonymous.unwrap_or(false))?;
        } else if anonymous == Some(false)
            && matches!(config.authentication, S3Authentication::Anonymous)
        {
            config.authentication = S3Authentication::default();
        }
        if self.has("s3.sse.type") {
            config.encryption = self.s3_encryption()?;
        }
        // Iceberg's generic client region takes precedence over the S3-specific value.
        if self.has("client.region") || self.has("s3.region") {
            config.region = self
                .value("client.region")
                .or_else(|| self.value("s3.region"));
        }

        Ok(Cow::Owned(config.into_canonical()))
    }

    fn s3_authentication(&self, anonymous: bool) -> Result<S3Authentication> {
        AwsCredentialOptions {
            access_key_id: self.get("s3.access-key-id"),
            secret_access_key: self.get("s3.secret-access-key"),
            token: self.get("s3.session-token"),
            role_arn: self.get("client.assume-role.arn"),
            external_id: self.get("client.assume-role.external-id"),
            role_session_name: self.get("client.assume-role.session-name"),
            ..AwsCredentialOptions::default()
        }
        .into_authentication(anonymous)
        .map_err(|error| {
            Error::new(ErrorKind::DataInvalid, "invalid S3 authentication")
                .with_source(error)
        })
    }

    fn s3_encryption(&self) -> Result<Option<S3Encryption>> {
        let Some(encryption_type) = self.get("s3.sse.type") else {
            return Ok(None);
        };
        match encryption_type.to_ascii_lowercase().as_str() {
            "none" => Ok(None),
            "s3" => Ok(Some(S3Encryption::S3)),
            "kms" => Ok(Some(S3Encryption::Kms {
                key_id: self.value("s3.sse.key"),
            })),
            "custom" => {
                let encoded_key = self.get("s3.sse.key").ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "s3.sse.key is required when s3.sse.type is custom",
                    )
                })?;
                let decoded_key =
                    BASE64_STANDARD.decode(encoded_key).map_err(|error| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "s3.sse.key must be a Base64-encoded customer key",
                        )
                        .with_source(error)
                    })?;
                if let Some(expected_md5) = self.get("s3.sse.md5") {
                    let actual_md5 = BASE64_STANDARD.encode(Md5::digest(decoded_key));
                    if actual_md5 != expected_md5 {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "s3.sse.md5 does not match s3.sse.key",
                        ));
                    }
                }
                Ok(Some(S3Encryption::Custom {
                    key: SecretString::new(encoded_key),
                }))
            }
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                "s3.sse.type must be one of none, s3, kms or custom",
            )),
        }
    }
}
