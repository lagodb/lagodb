//! PostgreSQL catalog-layer validation for the shared storage option schema.

use pgrx::pg_sys;

use super::StorageProfileError;
use super::config::{MappingOptions, StorageProfileConfig, StorageServerOptions};

impl StorageProfileConfig {
    pub fn is_server_discriminator(name: &str) -> bool {
        name == "provider"
    }

    pub fn accepts_user_mapping_option(name: &str) -> bool {
        matches!(
            name,
            "access_key_id"
                | "secret_access_key"
                | "token"
                | "service_account_path"
                | "service_account_key"
                | "application_credentials_path"
                | "access_key"
                | "bearer_token"
                | "client_id"
                | "client_secret"
                | "tenant_id"
        )
    }

    pub fn validate_options(
        options: &[Option<String>],
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), StorageProfileError> {
        Self::validate_catalog_options(options, catalog, false)
    }

    pub fn validate_scoped_options(
        options: &[Option<String>],
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), StorageProfileError> {
        Self::validate_catalog_options(options, catalog, true)
    }

    fn validate_catalog_options(
        options: &[Option<String>],
        catalog: Option<pg_sys::Oid>,
        require_scope: bool,
    ) -> Result<(), StorageProfileError> {
        match catalog {
            Some(catalog) if catalog == pg_sys::ForeignDataWrapperRelationId => {
                if options.iter().flatten().next().is_some() {
                    return Err(StorageProfileError::invalid_option(
                        "FDW option",
                        "wrapper-level options are not supported",
                    ));
                }
            }
            Some(catalog) if catalog == pg_sys::ForeignServerRelationId => {
                let mut server = StorageServerOptions::default();
                for option in options.iter().flatten() {
                    let (name, value) = option.split_once('=').ok_or_else(|| {
                        StorageProfileError::invalid_option(
                            "server option",
                            "expected name=value",
                        )
                    })?;
                    match name {
                        "provider" => StorageServerOptions::set(
                            &mut server.provider,
                            name,
                            value,
                        )?,
                        "scope" => {
                            StorageServerOptions::set(&mut server.scope, name, value)?
                        }
                        "region" => StorageServerOptions::set(
                            &mut server.region,
                            name,
                            value,
                        )?,
                        "endpoint" => StorageServerOptions::set(
                            &mut server.endpoint,
                            name,
                            value,
                        )?,
                        "allow_http" => StorageServerOptions::set(
                            &mut server.allow_http,
                            name,
                            value,
                        )?,
                        "virtual_hosted_style_request" => StorageServerOptions::set(
                            &mut server.virtual_hosted_style_request,
                            name,
                            value,
                        )?,
                        "skip_signature" => StorageServerOptions::set(
                            &mut server.skip_signature,
                            name,
                            value,
                        )?,
                        "base_url" => StorageServerOptions::set(
                            &mut server.base_url,
                            name,
                            value,
                        )?,
                        "account" => StorageServerOptions::set(
                            &mut server.account,
                            name,
                            value,
                        )?,
                        "use_emulator" => StorageServerOptions::set(
                            &mut server.use_emulator,
                            name,
                            value,
                        )?,
                        _ => {
                            return Err(StorageProfileError::invalid_option(
                                name,
                                "is not a supported foreign server option",
                            ));
                        }
                    }
                }
                if require_scope && server.scope().is_none() {
                    return Err(StorageProfileError::invalid_option(
                        "scope",
                        "is required for a scoped storage profile",
                    ));
                }
                server.into_profile(MappingOptions::default())?;
            }
            Some(catalog) if catalog == pg_sys::UserMappingRelationId => {
                MappingOptions::parse(options)?.validate_provider_independent()?;
            }
            _ => {
                if options.iter().flatten().next().is_some() {
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
