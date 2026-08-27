use lagodb_core::fdw::ForeignValidationError;
use lagodb_core::storage::profile::StorageProfileConfig;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::schema::OptionLayer;

pub(crate) struct IcebergFdwOptions;

impl IcebergFdwOptions {
    pub(crate) fn validate_catalog(
        options: &[Option<String>],
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), ForeignValidationError> {
        let layer = OptionLayer::from_catalog(catalog);
        match layer {
            OptionLayer::Server if Self::is_storage_server(options) => {
                StorageProfileConfig::validate_scoped_options(options, catalog)
                    .map_err(ForeignValidationError::provider)?;
            }
            OptionLayer::Mapping => {
                Self::validate_mapping(options, catalog)?;
            }
            _ => layer.validate(options)?,
        }
        Ok(())
    }

    fn is_storage_server(options: &[Option<String>]) -> bool {
        options.iter().flatten().any(|option| {
            option.split_once('=').is_some_and(|(name, _)| {
                StorageProfileConfig::is_server_discriminator(name)
            })
        })
    }

    fn validate_mapping(
        options: &[Option<String>],
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), ForeignValidationError> {
        let mut has_rest_only = false;
        let mut has_storage_only = false;
        for option in options.iter().flatten() {
            let (name, _) = option.split_once('=').ok_or_else(|| {
                IcebergFdwError::invalid_option(
                    "foreign option",
                    "expected name=value",
                )
            })?;
            let rest = OptionLayer::Mapping.accepts(name);
            let storage = StorageProfileConfig::accepts_user_mapping_option(name);
            if !rest && !storage {
                return Err(IcebergFdwError::unsupported_option(name).into());
            }
            has_rest_only |= rest && !storage;
            has_storage_only |= storage && !rest;
        }
        if has_rest_only && has_storage_only {
            return Err(IcebergFdwError::invalid_option(
                "user mapping",
                "REST authentication and storage credentials must use separate servers",
            )
            .into());
        }
        if has_storage_only {
            StorageProfileConfig::validate_options(options, catalog)
                .map_err(ForeignValidationError::provider)?;
        } else {
            OptionLayer::Mapping.validate(options)?;
        }
        Ok(())
    }
}
