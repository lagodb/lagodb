//! Catalog adapters owned by the Iceberg FDW.

mod binding;
pub mod rest;
mod sqlite;

pub(crate) use binding::{ForeignCatalog, PreparedCatalogCommit};
pub(crate) use sqlite::PgSqliteCatalogBuilder;
