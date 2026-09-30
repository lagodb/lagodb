use pgrx::prelude::*;

mod config;
pub mod error;
pub mod foreign_table;
mod managed_table;
pub(crate) mod predicate;
mod query_offload;
pub(crate) mod scan;
pub(crate) mod schema;
mod storage;
pub(crate) mod write;

pub use managed_table::{IcebergTableAm, get_iceberg_am_routine_ptr};

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    // Preserve the established initialization order: the REST TLS provider is
    // ready before any extension hooks, AM configuration/hooks precede the FDW
    // utility hook, and executor/table-provider facets are registered last.
    foreign_table::initialize_crypto_provider();
    config::init();
    managed_table::initialize_configuration_and_hooks();
    foreign_table::register();
    // Stage both PostgreSQL scan adapters and the shared query-offload facet
    // before table-provider registration atomically publishes this provider DSO.
    managed_table::register_scan_provider();
    query_offload::register();
    managed_table::register_table_provider();
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // noop
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'lagodb_base'",
            "lagodb.provider_libraries = 'lagodb_iceberg'",
        ]
    }
}
