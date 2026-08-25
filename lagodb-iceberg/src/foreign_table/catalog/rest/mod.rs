//! PostgreSQL-specific composition for the synchronous REST catalog.

mod builder;
mod http;
mod storage;

pub use builder::PgRestCatalogBuilder;
