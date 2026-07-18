pub mod access_method;
pub mod bridge;
pub mod metadata_table;
pub mod metadata_tracker;
pub(crate) mod row_mutations;
pub(crate) mod schema_evolution;
pub(crate) mod table_drop;
pub mod table_lifecycle;
pub(crate) mod table_properties;

// `schema_builder` is intentionally crate-private: PostgreSQL → Iceberg type
// conversion has exactly one supported entry point, [`schema_builder::tuple_desc_to_schema`].
// Keeping the module itself private (and `PgType` / `SchemaBuilder` along
// with it) means the "single field-id counter" invariant is enforced by the
// type system rather than by a doc comment. The only consumer,
// `table_lifecycle`, is a sibling module and reaches in via `super::`.
mod schema_builder;

// AM identity is the one cross-cutting concept every layer of this crate
// reaches for, so it is the only re-export hoisted to the `catalog` facade.
// All other items must be referenced through their owning submodule, so that
// the module hierarchy makes the domain boundaries explicit at call sites.
pub use access_method::{IcebergAccessMethod, IcebergRelationExt};
