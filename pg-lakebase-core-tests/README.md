# pg-lakebase-core-tests

**PostgreSQL integration tests for `pg-lakebase-core`.**

This crate is a test-only PostgreSQL extension that exercises
`pg-lakebase-core` functionality requiring a live PostgreSQL backend — things
like Datum round-tripping, PG output function calls, and catalog operations
that cannot be tested with `cargo test` alone.

## Why a separate crate?

`pg-lakebase-core` is a pure library consumed by downstream extensions (like
`pg-iceberg-am`). pgrx's `#[pg_test]` requires the test code to live in a
loadable PostgreSQL extension (`cdylib` with `pg_module_magic!()`). Keeping
that extension plumbing in a separate crate means:

- `pg-lakebase-core` stays a clean library with no `cdylib` / `pg_module_magic`
- Zero risk of symbol conflicts with downstream extensions
- Clear separation of concerns

## Running tests

```bash
cargo pgrx test pg17 --package pg-lakebase-core-tests
```

Or as part of the full workspace test suite:

```bash
cargo xtask test-all pg17
```

## Directory structure

Test modules mirror the `pg-lakebase-core` source structure:

```text
src/
├── lib.rs          # Extension boilerplate (pg_module_magic, pg_test setup)
└── tuple/
    ├── mod.rs
    └── cell.rs     # Tests for pg_lakebase_core::tuple::Cell
```

## Adding new tests

1. Create a file mirroring the module you're testing:

```rust
// src/catalog/access.rs
use pg_lakebase_core::catalog::*;
use pgrx::prelude::*;

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_catalog_scan_key_oid_eq() {
        // Runs inside PostgreSQL
    }
}
```

2. Register the module in its parent `mod.rs`:

```rust
// src/catalog/mod.rs
mod access;
```

3. If it's a new top-level module, add it to `src/lib.rs`:

```rust
#[cfg(any(test, feature = "pg_test"))]
mod catalog;
```

## Prerequisites

- pgrx initialized: `cargo pgrx init --pg17=/path/to/pg_config`
- No Docker required for these tests
