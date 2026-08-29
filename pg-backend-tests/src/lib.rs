//! Backend (`#[pg_test]`) integration tests for the LagoDB framework
//! library crates.
//!
//! `lagodb-core` and `pg-arrow-conv` are pure library crates (`rlib`, no
//! `pg_module_magic!()`, no `.control` file), so backend tests that need a live
//! PostgreSQL backend (Datum conversions, memory contexts, slot writes, SPI,
//! catalog access) cannot live in their own crates. This single companion
//! extension crate carries the plumbing for all of them, organised one module
//! per crate under test:
//!
//! - `lagodb_core` — backend tests for `lagodb-core`
//! - `arrow_conv`    — backend tests for `pg-arrow-conv`
//!
//! Adding a new framework `rlib` does **not** require a new test crate: add a
//! sibling module here and depend on the crate. See `README.md`.
//!
//! Run with:
//! ```bash
//! cargo pgrx test pg17 --package pg-backend-tests
//! ```

use pgrx::prelude::*;

#[cfg(any(test, feature = "pg_test"))]
use ::lagodb_core::{hooks as core_hooks, runtime_api as core_runtime_api};

pg_module_magic!();

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    #[cfg(any(test, feature = "pg_test"))]
    {
        lagodb_core::init_pg_test_extension();
        let identity = core_runtime_api::ProviderIdentity::foreign_data_wrapper(
            c"pg-backend-tests",
            c"pg_backend_tests",
            c"pg_backend_tests",
        );
        core_hooks::freeze_hooks(&identity).unwrap_or_else(|error| {
            panic!("failed to publish backend-test planning hooks: {error}")
        });
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod lagodb_core;

#[cfg(any(test, feature = "pg_test"))]
mod arrow_conv;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'lagodb_base'",
            "lagodb.provider_libraries = 'pg_backend_tests'",
        ]
    }
}
