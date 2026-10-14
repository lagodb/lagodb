pub mod access_method;
pub mod bridge;
pub mod error;
pub(crate) mod local_storage;
pub mod metadata_table;
pub mod metadata_tracker;
pub(crate) mod schema_evolution;
mod sql_api;
pub(crate) mod table_creation;
pub(crate) mod table_drop;
pub(crate) mod table_location;
pub(crate) mod table_properties;
pub(crate) mod table_truncate;

// Managed type policy and schema construction remain provider-owned.
pub(crate) mod numeric_type;
mod schema_builder;
pub(crate) mod table_definition;
mod type_mapping;

// AM identity is the one cross-cutting concept every layer of this crate
// reaches for, so it is the only re-export hoisted to the `catalog` facade.
// All other items must be referenced through their owning submodule, so that
// the module hierarchy makes the domain boundaries explicit at call sites.
pub use access_method::{IcebergAccessMethod, IcebergRelationExt};
