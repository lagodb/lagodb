//! PostgreSQL catalog-layer validation for the shared storage option schema.

use pgrx::pg_sys;

use crate::storage::foreign::ForeignOptionView;

use super::config::{StorageProfileConfig, StorageServerOptions};
use super::mapping::MappingOptions;
use super::{ObjectUri, StorageProfileError};

impl StorageProfileConfig {
    pub fn is_server_discriminator(name: &str, value: &str) -> bool {
        name == "provider"
            || (name == "scope" && ObjectUri::is_supported_prefix(value.as_bytes()))
    }

    pub fn accepts_user_mapping_option(name: &str) -> bool {
        MappingOptions::accepts_option(name)
    }

    pub fn validate_options(
        options: ForeignOptionView<'_>,
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), StorageProfileError> {
        Self::validate_catalog_options(options, catalog, false)
    }

    pub fn validate_scoped_options(
        options: ForeignOptionView<'_>,
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), StorageProfileError> {
        Self::validate_catalog_options(options, catalog, true)
    }

    fn validate_catalog_options(
        options: ForeignOptionView<'_>,
        catalog: Option<pg_sys::Oid>,
        require_scope: bool,
    ) -> Result<(), StorageProfileError> {
        match catalog {
            Some(catalog) if catalog == pg_sys::ForeignDataWrapperRelationId => {
                if !options.is_empty() {
                    return Err(StorageProfileError::invalid_option(
                        "FDW option",
                        "wrapper-level options are not supported",
                    ));
                }
            }
            Some(catalog) if catalog == pg_sys::ForeignServerRelationId => {
                let server = StorageServerOptions::from_view(options)?;
                if require_scope && server.scope().is_none() {
                    return Err(StorageProfileError::invalid_option(
                        "scope",
                        "is required for a scoped storage profile",
                    ));
                }
                server.into_profile(MappingOptions::default())?.validate()?;
            }
            Some(catalog) if catalog == pg_sys::UserMappingRelationId => {
                MappingOptions::from_view(options)?
                    .validate_provider_independent()?;
            }
            _ => {
                if !options.is_empty() {
                    return Err(StorageProfileError::invalid_option(
                        "FDW option",
                        "options are not supported at this catalog layer",
                    ));
                }
            }
        }
        Ok(())
    }
}
