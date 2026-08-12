//! LagoDB connector extension entry point.
//!
//! The PostgreSQL FDW adapter and its format implementations live below
//! [`fdw`].  A single FDW provider selects one of the supported formats for
//! each foreign table.

mod copy;
mod error;
mod fdw;
mod format;
mod gucs;
mod storage;

use pg_lakebase_core::hooks::freeze_hooks;
use pg_lakebase_core::runtime_api::ProviderIdentity;
use pgrx::prelude::*;

pgrx::pg_module_magic!();

extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    gucs::init();
    copy::register();
    fdw::register_ddl_hooks();
    let identity = ProviderIdentity::foreign_data_wrapper(
        c"lagodb",
        c"lagodb_connectors",
        c"lagodb_connectors",
    );
    freeze_hooks(&identity).unwrap_or_else(|error| {
        panic!("failed to publish LagoDB connector hooks: {error}")
    });
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'pg_lakebase_runtime'",
            "pg_lakebase.provider_libraries = 'lagodb_connectors'",
        ]
    }
}
