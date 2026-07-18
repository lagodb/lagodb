//! Backend (`#[pg_test]`) integration tests for the pg-lakebase framework
//! library crates.
//!
//! `pg-lakebase-core` and `pg-arrow-conv` are pure library crates (`rlib`, no
//! `pg_module_magic!()`, no `.control` file), so backend tests that need a live
//! PostgreSQL backend (Datum conversions, memory contexts, slot writes, SPI,
//! catalog access) cannot live in their own crates. This single companion
//! extension crate carries the plumbing for all of them, organised one module
//! per crate under test:
//!
//! - `lakebase_core` — backend tests for `pg-lakebase-core`
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

pg_module_magic!();

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    #[cfg(any(test, feature = "pg_test"))]
    lakebase_core::init_pg_test_extension();
}

#[cfg(any(test, feature = "pg_test"))]
mod lakebase_core;

#[cfg(any(test, feature = "pg_test"))]
mod arrow_conv;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_lakebase_runtime'"]
    }
}
