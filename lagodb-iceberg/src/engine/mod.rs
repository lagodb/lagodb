//! Iceberg format data plane shared by the built-in AM and FDW adapters.
//!
//! Modules in this layer operate on already-resolved PostgreSQL relation
//! shapes and already-loaded Iceberg tables. Catalog selection, authorization,
//! transaction commit, and FFI reporting belong to the caller adapters.

pub(crate) mod predicate;
pub(crate) mod scan;
pub(crate) mod schema;
pub(crate) mod write;
