//! Catalog access helpers and LagoDB catalog object IDs.

mod access;
mod database;
mod foreign_config;
mod foreign_server;
mod ids;
mod inheritance;
mod lookup;
mod privilege;
mod relation;
mod scan;
mod type_dependencies;
mod write;

pub use access::{CatalogRelation, CatalogScanKey, CatalogSnapshot};
pub use database::{LockedDatabase, current_database_name_bytes, database_path};
pub use foreign_config::{
    ForeignConfigInvalidation, ForeignConfigInvalidationKeys,
    register_foreign_config_invalidation_observer,
};
pub use foreign_server::ForeignServerDdlTarget;
pub use ids::{
    LAGODB_SCHEMA, TABLE_OPTIONS_PKEY, TABLE_OPTIONS_TABLE, get_lagodb_namespace_oid,
    get_table_options_oid, get_table_options_pkey_oid,
};
pub(crate) use ids::{MaintenanceCatalogIds, get_maintenance_catalog_ids};
pub use inheritance::{
    find_all_inheritors, has_inheritance_children, has_inheritance_parents,
};
pub use lookup::{
    alter_table_lookup_relation, get_extension_schema, get_namespace_name_bytes,
    get_namespace_oid, get_relation_oid, get_tablespace_oid, range_var_get_relid,
};
pub use privilege::CatalogObject;
pub use relation::{RelationCatalogEntry, RenameColumnTarget};
pub use scan::{CatalogOrderedScan, CatalogScan};
pub use type_dependencies::TypeDependencies;
pub use write::{CatalogUpdateResult, CatalogWriter};
