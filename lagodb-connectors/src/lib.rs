//! LagoDB connector extension entry point.
//!
//! [`copy`] and [`fdw`] adapt PostgreSQL execution, [`query_offload`] serves
//! Parquet foreign tables to the query engine, [`access`] resolves catalog
//! bindings and privileges, [`format`] owns schemas and shared file codecs,
//! and [`storage`] owns file discovery, I/O, and publication.

mod access;
mod cache;
mod copy;
mod error;
mod fdw;
mod format;
mod gucs;
mod query_offload;
mod storage;

use std::ffi::CStr;

use lagodb_core::hooks::freeze_hooks;
use lagodb_core::runtime_api::ProviderIdentity;
use pgrx::prelude::*;

pub(crate) const CONNECTOR_FDW_NAME: &CStr = c"lagodb_connectors";

pgrx::pg_module_magic!();

extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    gucs::init();
    copy::register();
    fdw::register_ddl_hooks();
    query_offload::register();
    let identity = ProviderIdentity::foreign_data_wrapper(
        c"lagodb",
        c"lagodb_connectors",
        c"lagodb_connectors",
    );
    freeze_hooks(&identity).unwrap_or_else(|error| {
        panic!("failed to publish LagoDB connector hooks: {error}")
    });
}
