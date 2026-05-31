//! PostgreSQL integration tests for `pg-lakebase-core`.
//!
//! This crate exists solely to run `#[pg_test]` tests that exercise
//! `pg-lakebase-core` functionality requiring a live PostgreSQL backend
//! (e.g. Datum conversions, PG output functions, catalog operations).
//!
//! Tests are run with:
//! ```bash
//! cargo pgrx test pg17 --package pg-lakebase-core-tests
//! ```

use pgrx::prelude::*;

pg_module_magic!();

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    #[cfg(any(test, feature = "pg_test"))]
    customscan::init_pg_test_extension();
}

#[cfg(any(test, feature = "pg_test"))]
mod customscan;

#[cfg(any(test, feature = "pg_test"))]
mod expr;

#[cfg(any(test, feature = "pg_test"))]
mod support;

#[cfg(any(test, feature = "pg_test"))]
mod tuple;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
