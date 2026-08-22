//! PostgreSQL foreign-catalog options for REST catalogs and scoped storage.

mod catalog;
mod schema;
mod table;
mod validation;

pub(crate) use catalog::{
    CatalogBindingKey, CatalogRuntimeConfig, RestCatalogConnection, ServerBindingKey,
};
pub(crate) use table::{
    ForeignTableIdentity, ForeignTableMode, MaterializedForeignOptions,
};
pub(crate) use validation::IcebergFdwOptions;
