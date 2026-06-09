//! Backend `#[pg_test]` tests for `pg-lakebase-core`.
//!
//! These exercise framework code that requires a live PostgreSQL backend
//! (Datum round-tripping, planner node construction, custom-scan hooks, memory
//! contexts). Pure mapping/parsing logic stays as ordinary `#[test]` inside
//! `pg-lakebase-core` itself.

mod customscan;
mod expr;
mod support;
mod tuple;

/// Registers the test-only custom-scan provider used by the customscan tests.
/// Called from the crate's `_PG_init`.
pub(crate) fn init_pg_test_extension() {
    customscan::init_pg_test_extension();
}
