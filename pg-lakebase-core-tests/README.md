# pg-lakebase-core-tests

**PostgreSQL integration tests for `pg-lakebase-core`.**

This crate is a test-only PostgreSQL extension that exercises
`pg-lakebase-core` functionality requiring a live PostgreSQL backend — things
like Datum round-tripping, `palloc`/`copyObject`, planner node construction,
and PG output function calls that cannot be tested with ordinary `cargo test`.

## Why a separate crate?

These are three different kinds of artifact in the workspace:

| Workspace artifact | Kind | Role |
|--------------------|------|------|
| `pg-lakebase-core` | Rust library crate (`lib`) | Shared framework code consumed by extension crates |
| `pg-iceberg-am` | PostgreSQL extension crate (`cdylib`) | Iceberg Table Access Method — a product extension built on top of `pg-lakebase-core` |
| `pg-lakebase-core-tests` | Test-only PostgreSQL extension crate (`cdylib`) | Runs `#[pg_test]` for `pg-lakebase-core` |

`pg-lakebase-core` is **not** a PostgreSQL extension. It has no
`pg_module_magic!()`, no `.control` file, and is not loaded by PostgreSQL.
Downstream extensions such as `pg-iceberg-am` link against it as a normal Rust
dependency.

`#[pgrx::pg_test]` is different from ordinary `#[test]`: the test function
body must be compiled into a loadable extension shared library that PostgreSQL
can install and call via SQL. A plain library crate cannot satisfy that
requirement on its own.

Keeping the extension plumbing in a dedicated crate means:

- `pg-lakebase-core` stays a clean library with no `cdylib` / `pg_module_magic`
- Framework tests are not coupled to a product extension such as
  `pg-iceberg-am` (which has its own `_PG_init`, WAL rmgr, background worker,
  and `shared_preload_libraries` requirements — see
  [`pg-iceberg-am/docs/testing.md`](../pg-iceberg-am/docs/testing.md))
- Zero risk of symbol conflicts (duplicate `Pg_magic_func`) with downstream
  extensions that also define `pg_module_magic!()`

For the full rationale behind host-side vs backend-side testing, see
[`pg-iceberg-am/docs/testing.md`](../pg-iceberg-am/docs/testing.md). The same
`#[test]` vs `#[pg_test]` boundary applies here; that document was written
for `pg-iceberg-am` but the underlying pgrx execution model is identical.

## Running tests

```bash
cargo pgrx test pg17 --package pg-lakebase-core-tests
```

Or as part of the CI-grade full workspace test suite:

```bash
cargo xtask test-all pg17
```

`cargo pgrx test` is **not** a mode where all Rust tests run inside PostgreSQL.
It produces two artifacts:

1. A host-side Rust test executable (built by `cargo test`) that runs ordinary
   `#[test]` functions and the generated wrapper functions for `#[pg_test]`.
2. A PostgreSQL extension shared library (`pg_lakebase_core_tests.so` on Linux,
   `pg_lakebase_core_tests.dylib` on macOS) built and installed when a
   `#[pg_test]` wrapper needs to call into PostgreSQL. Only the `#[pg_test]`
   function bodies run there.

Do **not** expect `cargo test -p pg-lakebase-core-tests --lib` alone to exercise
backend-dependent paths — those require `cargo pgrx test`.

## Test placement rules

The real boundary is not "Rust test" versus "SQL test". It is:

> Can this code path run correctly in an ordinary host process, or does it
> require PostgreSQL backend process semantics?

| Use | When |
|-----|------|
| Ordinary `#[test]` in `pg-lakebase-core` | Pure Rust logic; no `pg_sys` backend calls, SPI, `palloc`, or Datum infrastructure |
| `#[pg_test]` in this crate | Any path that calls `pg_sys` backend functions, uses memory contexts, depends on real PG OIDs/types, or transitively pulls in code that requires backend symbols |

When in doubt, prefer `#[pg_test]` for PostgreSQL-facing paths. Keep pure
mapping and parsing logic as ordinary `#[test]` in `pg-lakebase-core` so it
stays fast and does not require a PostgreSQL cluster.

If a host-side `cargo test` binary fails to start with a dynamic loader error
such as `undefined symbol: PG_exception_stack`, the tested code path belongs
in this crate as a `#[pg_test]`, not in an ordinary `#[test]`.

## Adding new tests

1. Create a file under `src/` mirroring the `pg-lakebase-core` module you are
   testing (for example, tests for `pg_lakebase_core::tuple::Cell` live at
   `src/tuple/cell.rs`).

2. Gate the module with `#[cfg(any(test, feature = "pg_test"))]` — **not**
   `#[cfg(test)]` alone. During `cargo pgrx test`, pgrx builds the extension
   shared library with the `pg_test` feature but without `cfg(test)`. Gating
   only on `test` would leave the SQL functions absent from the installed
   extension.

3. Declare a `#[pgrx::pg_schema] mod tests { ... }` block and place each test
   inside it. In pgrx 0.18, `pgrx-tests` invokes test functions as:

   ```sql
   SELECT "tests"."<function_name>"();
   ```

   The Rust module named `tests` annotated with `#[pgrx::pg_schema]` is what
   makes pgrx emit `CREATE SCHEMA IF NOT EXISTS tests;` and register the SQL
   functions there.

Example:

```rust
//! Tests for `pg_lakebase_core::tuple::Cell`.

use pg_lakebase_core::tuple::Cell;

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;
    use pgrx::pg_test;

    #[pg_test]
    fn test_cell_roundtrip() {
        // Runs inside PostgreSQL after the extension is installed and loaded.
    }
}
```

4. Register the module in its parent `mod.rs`, and add a top-level `mod` line
   to `src/lib.rs` if it is a new area:

   ```rust
   #[cfg(any(test, feature = "pg_test"))]
   mod tuple;
   ```

## Prerequisites

- pgrx initialized: `cargo pgrx init --pg17=/path/to/pg_config`
- No Docker required for these tests (unlike the full `cargo xtask test-all`
  suite, which also runs Docker-based regress tests for `pg-iceberg-am`)
