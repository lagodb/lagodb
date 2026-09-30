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
pub(crate) mod maintenance;
mod options;
mod provider;
mod read_view;
pub(crate) mod storage;

pub(crate) use constants::ICEBERG_AM_NAME;
pub(crate) use read_view::ManagedTableReadView;
pub(crate) use storage::StorageContext;

pub(crate) use provider::IcebergTableProvider;
pub use provider::{IcebergTableAm, get_iceberg_am_routine_ptr};

use crate::storage::local_file_wal;
use lagodb_core::table_provider::register_provider;

pub(crate) fn initialize_configuration_and_hooks() {
    gucs::init();
    hooks::init_hooks();
}

pub(crate) fn register_scan_provider() {
    local_file_wal::init_wal_rmgr();
    customscan::register();
}

pub(crate) fn register_table_provider() {
    register_provider::<IcebergTableProvider>();
}
