//! PostgreSQL foreign-option parsing and validation for Iceberg catalogs and
//! tables.

mod catalog_type;
mod identifier;
mod schema;
mod table;
mod validation;

pub(super) use catalog_type::{CatalogServerOptions, CatalogType};
pub(crate) use identifier::ForeignIdentifier;
pub(super) use schema::{OptionLayer, ParsedOptions};
pub(crate) use table::{
    ForeignImportOptions, ForeignTableCreateOptions, ForeignTableIdentity,
};
pub(crate) use validation::IcebergFdwOptions;
