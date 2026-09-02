//! Built-in Iceberg table access method adapter.
//!
//! This layer owns PostgreSQL TableAM callbacks, the local metadata catalog,
//! transaction tracking, maintenance, and AM-specific storage policy. It
//! may depend on `crate::engine`; the shared engine must not depend on this
//! module.

mod access;
pub(crate) mod catalog;
mod constants;
mod customscan;
mod gucs;
mod hooks;
mod maintenance;
mod options;
mod provider;
mod query_source;
pub(crate) mod storage;

pub use provider::{IcebergTableAm, get_iceberg_am_routine_ptr};

use crate::storage::local_file_wal;

pub(crate) fn initialize_configuration_and_hooks() {
    gucs::init();
    hooks::init_hooks();
}

pub(crate) fn register_providers() {
    local_file_wal::init_wal_rmgr();

    // Stage every planner facet before publishing the provider transaction.
    customscan::register();
    query_source::register();
    lagodb_core::table_maintenance::register_provider::<
        maintenance::IcebergTableMaintenanceProvider,
    >();
}
