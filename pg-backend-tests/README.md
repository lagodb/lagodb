# pg-backend-tests

**Backend (`#[pg_test]`) integration tests for the pg-lakebase framework library crates.**

This crate is a single test-only PostgreSQL extension that hosts the
`#[pg_test]` tests for every framework *library* crate that needs a live
PostgreSQL backend. These are the tests that cannot run under an ordinary
`cargo test` because they call into PostgreSQL backend functions — Datum
round-tripping, `palloc`/memory contexts, slot writes, planner node
construction, SPI, and PostgreSQL output functions.

It currently aggregates the backend tests for `pg-lakebase-core` and
`pg-arrow-conv`, one module per crate.

## Why this crate exists

The workspace has two kinds of Rust code that look similar but are built very
differently:

- **Framework library crates** (`pg-lakebase-core`, `pg-arrow-conv`) are plain
  Rust libraries (`rlib`). They are *not* PostgreSQL extensions: they have no
  `pg_module_magic!()`, no `.control` file, and are never loaded by PostgreSQL
  on their own. They are consumed by the extension crates.
- **Extension crates** (`pg-iceberg-am`) are loadable shared libraries
  (`cdylib`) that PostgreSQL installs and calls via SQL.

The problem is that pgrx's `#[pg_test]` bodies must be compiled into a loadable
extension that PostgreSQL can install and invoke — something a plain `rlib`
cannot be. So the framework libraries have backend-dependent code that they
cannot test from inside their own crate.

This crate solves that. Rather than give every framework library its own
`*-tests` companion extension (which would multiply crate count and test runs),
a **single** aggregator hosts all of their backend tests. A test crate sits at
the very top of the dependency graph — nothing depends on it — so it can depend
on several libraries at once without creating a cycle or inverting the
production layering. Adding a new framework library means adding a module here,
not a new crate.

Product extensions such as `pg-iceberg-am` are themselves `cdylib`s and keep
their own `#[pg_test]` tests inline; they do not belong here.

## What goes where

The boundary is not "Rust test" versus "SQL test". It is whether the code path
can run in an ordinary host process or requires PostgreSQL backend semantics:

- Pure Rust logic with no backend calls stays as an ordinary `#[test]` in the
  library crate, so it stays fast and needs no PostgreSQL cluster.
- Anything that calls `pg_sys` backend functions, uses memory contexts, depends
  on real PostgreSQL OIDs/types, or transitively pulls in backend symbols
  belongs here as a `#[pg_test]`.

When in doubt, prefer `#[pg_test]` for PostgreSQL-facing paths. See
[`pg-iceberg-am/docs/testing.md`](../pg-iceberg-am/docs/testing.md) for the full
host-versus-backend rationale.

## Running tests

```bash
cargo pgrx test pg17 --package pg-backend-tests
```

Or as part of the full workspace suite:

```bash
cargo xtask test-all pg17
```

## Prerequisites

- pgrx initialized: `cargo pgrx init --pg17=/path/to/pg_config`
- No Docker required for these tests (unlike the full `cargo xtask test-all`
  suite, which also runs Docker-based regress tests for `pg-iceberg-am`)
