//! Borrowed user-mapping options and provider ownership validation.
use super::StorageProfileError;
use crate::storage::foreign::ForeignOptionView;
use lagodb_storage::AwsCredentialOptions;

#[derive(Default)]
pub(super) struct MappingOptions<'a> {
    pub(super) access_key_id: Option<&'a str>,
    pub(super) secret_access_key: Option<&'a str>,
    pub(super) token: Option<&'a str>,
    pub(super) aws_profile: Option<&'a str>,
    pub(super) role_arn: Option<&'a str>,
    pub(super) external_id: Option<&'a str>,
    pub(super) role_session_name: Option<&'a str>,

    pub(super) service_account_path: Option<&'a str>,
    pub(super) service_account_key: Option<&'a str>,
    pub(super) application_credentials_path: Option<&'a str>,
    pub(super) access_key: Option<&'a str>,
    pub(super) bearer_token: Option<&'a str>,
    pub(super) client_id: Option<&'a str>,
    pub(super) client_secret: Option<&'a str>,
    pub(super) tenant_id: Option<&'a str>,
}

impl<'a> MappingOptions<'a> {
    pub(super) fn parse(
        options: &'a [Option<String>],
    ) -> Result<Self, StorageProfileError> {
        let mut parsed = Self::default();
        for option in options.iter().flatten() {
            let (name, value) = option.split_once('=').ok_or_else(|| {
                StorageProfileError::invalid_option(
                    "user mapping option",
                    "expected name=value",
                )
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    pub(super) fn from_view(
        options: ForeignOptionView<'a>,
    ) -> Result<Self, StorageProfileError> {
        let mut parsed = Self::default();
        for option in options.iter() {
            let name = option.name().to_str().map_err(|_| {
                StorageProfileError::invalid_option(
                    "user mapping option",
                    "must be valid UTF-8",
                )
            })?;
            let value = option.value_str().map_err(|_| {
                StorageProfileError::invalid_option(name, "must be valid UTF-8")
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    pub(super) fn accepts_option(name: &str) -> bool {
        Self::default().slot(name).is_some()
    }

    fn slot(&mut self, name: &str) -> Option<&mut Option<&'a str>> {
        Some(match name {
            "aws_profile" => &mut self.aws_profile,
            "role_arn" => &mut self.role_arn,
            "external_id" => &mut self.external_id,
            "role_session_name" => &mut self.role_session_name,
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
            _ => return None,
        })
    }

    fn set(&mut self, name: &str, value: &'a str) -> Result<(), StorageProfileError> {
        let slot = self.slot(name).ok_or_else(|| {
            StorageProfileError::invalid_option(
                name,
                "is not a supported user mapping option",
            )
        })?;
        if slot.replace(value).is_some() {
            return Err(StorageProfileError::invalid_option(
                name,
                "must not be specified more than once",
            ));
        }
        Ok(())
    }

    pub(super) fn aws_options(&self) -> AwsCredentialOptions<'a> {
        AwsCredentialOptions {
            access_key_id: self.access_key_id,
            secret_access_key: self.secret_access_key,
            token: self.token,
            aws_profile: self.aws_profile,
            role_arn: self.role_arn,
            external_id: self.external_id,
            role_session_name: self.role_session_name,
        }
    }

    pub(super) fn reject_aws_options(&self) -> Result<(), StorageProfileError> {
        if self.aws_options().is_configured() {
            return Err(StorageProfileError::invalid_option(
                "AWS credentials",
                "are not valid for the selected provider",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_provider_independent(
        &self,
    ) -> Result<(), StorageProfileError> {
        let has_s3 = self.aws_options().is_configured();
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
            return Err(StorageProfileError::invalid_option(
                "user mapping credentials",
                "credential sources for different providers cannot be combined",
            ));
        }
        self.aws_options().validate()?;
        if gcs_sources > 1 {
            return Err(StorageProfileError::invalid_option(
                "GCS credential source",
                "only one of service_account_path, service_account_key, or application_credentials_path is allowed",
            ));
        }
        if azure_secret_fields != 0 && azure_secret_fields != 3 {
            return Err(StorageProfileError::invalid_option(
                "client_id/client_secret/tenant_id",
                "must be specified together",
            ));
        }
        if usize::from(self.access_key.is_some())
            + usize::from(self.bearer_token.is_some())
            + usize::from(azure_secret_fields == 3)
            > 1
        {
            return Err(StorageProfileError::invalid_option(
                "Azure credential source",
                "access_key, bearer_token, and client-secret auth are mutually exclusive",
            ));
        }
        Ok(())
    }
}
