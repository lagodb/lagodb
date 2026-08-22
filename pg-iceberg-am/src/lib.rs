use pgrx::prelude::*;

mod config;
mod engine;
pub mod error;
pub mod foreign_table;
mod managed_table;
mod storage;

pub use managed_table::{IcebergTableAm, get_iceberg_am_routine_ptr};

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    // Preserve the established initialization order: the REST TLS provider is
    // ready before any extension hooks, AM configuration/hooks precede the FDW
    // utility hook, and executor/maintenance providers are registered last.
    foreign_table::initialize_crypto_provider();
    config::init();
    managed_table::initialize_configuration_and_hooks();
    foreign_table::register();
    managed_table::register_providers();
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // noop
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'pg_lakebase_runtime'",
            "pg_lakebase.provider_libraries = 'pg_iceberg_am'",
        ]
    }
}
