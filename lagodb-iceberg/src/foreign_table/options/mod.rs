//! PostgreSQL foreign-catalog options for Iceberg catalogs and scoped storage.

mod catalog;
mod catalog_type;
mod identifier;
mod schema;
mod table;
mod validation;

pub(crate) use catalog::{CatalogConnection, CatalogRuntimeConfig, ServerBindingKey};
pub(crate) use identifier::ForeignIdentifier;
pub(crate) use table::{ForeignImportOptions, ForeignTableIdentity};
pub(crate) use validation::IcebergFdwOptions;
