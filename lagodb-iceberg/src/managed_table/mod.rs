//! Built-in Iceberg table access method adapter.
//!
//! This layer owns PostgreSQL TableAM callbacks, the local metadata catalog,
//! transaction tracking, maintenance, and AM-specific storage policy. It
//! may depend on the shared scan, predicate, schema, and write modules; those
//! modules must not depend on this adapter.

mod access;
pub(crate) mod catalog;
mod constants;
mod customscan;
mod gucs;
mod hooks;
mod maintenance;
mod options;
mod provider;
mod source;
pub(crate) mod storage;

pub(crate) use constants::ICEBERG_AM_NAME;
pub(crate) use source::{ManagedAnalyzeSnapshot, ManagedTableSnapshot};
pub(crate) use storage::StorageContext;

pub use provider::{IcebergTableAm, get_iceberg_am_routine_ptr};

use crate::storage::local_file_wal;

pub(crate) fn initialize_configuration_and_hooks() {
    gucs::init();
    hooks::init_hooks();
}

pub(crate) fn register_scan_provider() {
    local_file_wal::init_wal_rmgr();
    customscan::register();
}

pub(crate) fn register_maintenance_provider() {
    lagodb_core::table_maintenance::register_provider::<
        maintenance::IcebergTableMaintenanceProvider,
    >();
}
