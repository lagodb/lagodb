# pgrx Testing Design

This document defines how tests should be written for `pg-iceberg-am`.
It is intentionally a design note, not just a troubleshooting note: the
same rules should guide future tests.

## Problem Summary

`pg-iceberg-am` is a PostgreSQL extension. Some code paths reference
PostgreSQL backend symbols such as:

- `PG_exception_stack`
- `CurrentMemoryContext`
- `palloc`
- `pfree`
- other `pg_sys` backend symbols

Those symbols are available when PostgreSQL loads the extension shared
library inside a backend process. They are not available in an ordinary
Linux process running a Rust test binary.

The observed failure looked like this:

```text
symbol lookup error: undefined symbol: PG_exception_stack
```

This is not a Rust assertion failure and not a PostgreSQL SQL error. On
Linux it is a dynamic loader failure while starting the host-side Rust test
binary. The process cannot resolve a PostgreSQL backend symbol that was
pulled into the executable.

## Rust `cargo test`

`cargo test` compiles the crate in Rust test mode. That mode is roughly the
same as invoking `rustc --test`: Rust builds a normal executable with a test
runner, also called the test harness.

The test harness is the generated program that:

- discovers `#[test]` functions,
- parses test-runner arguments such as `--nocapture`,
- prints output like `running N tests`,
- executes tests and reports `test name ... ok`.

When a crate is compiled in this test mode, Rust enables the cfg flag
`test`. Therefore this module declaration:

```rust
#[cfg(test)]
mod tests;
```

means: include `tests.rs` only when this crate is being compiled for the
Rust test harness. The code in that module runs in the host-side test
process, not inside PostgreSQL.

For `pg-iceberg-am`, ordinary `#[test]` functions must only exercise logic
that can run in a normal process without PostgreSQL backend state.

The `test` cfg is crate-local. If a test target depends on another workspace
crate, that dependency is normally compiled without `cfg(test)`. It must not
be used as a general "host process versus PostgreSQL backend" capability flag
across crate boundaries.

Linking also follows reachability, not execution. A host test does not need to
call a backend callback to retain it: constructing a reachable descriptor that
stores a `#[pg_guard]` function pointer is enough. The linker must then retain
the expanded guard and resolve its PostgreSQL backend symbols.

## pgrx `#[pg_test]`

`#[pgrx::pg_test]` is different from ordinary `#[test]`. The macro expands
into two pieces:

1. A SQL-callable function registered through pgrx, compiled into the
   extension shared library.
2. A host-side Rust `#[test]` wrapper that asks `pgrx-tests` to run that
   SQL function in PostgreSQL.

The wrapper still runs in the Rust test binary, but the body of the
`#[pg_test]` function runs inside PostgreSQL after the extension has been
installed and loaded.

In pgrx 0.18, `pgrx-tests` calls test functions through SQL using the
`tests` schema:

```sql
SELECT "tests"."<function_name>"();
```

That detail matters when tests are placed outside a Rust module literally
named `tests`.

## `cargo pgrx test`

`cargo pgrx test pg17 --package pg-iceberg-am` is not a magic mode where all
Rust tests run inside PostgreSQL.

It produces and uses two different artifacts:

1. A host-side Rust test executable, built by `cargo test`. This executable
   runs the ordinary `#[test]` functions and the generated wrapper functions
   for `#[pg_test]`.
2. A PostgreSQL extension shared library, `pg_iceberg_am.so`, built and
   installed by `pgrx-tests` when a `#[pg_test]` wrapper needs to call into
   PostgreSQL. PostgreSQL loads this `.so`, and the actual `#[pg_test]`
   function bodies run there.

It first invokes a normal Rust test build with pgrx features enabled:

```bash
cargo test --features "pg17 pg_test" --no-default-features --package pg-iceberg-am
```

Ordinary `#[test]` functions still run in the host-side Rust test binary.
Only `#[pg_test]` function bodies are executed inside PostgreSQL.

During a `#[pg_test]`, `pgrx-tests` installs the extension using the
`pg_test` feature so the SQL functions exist in `pg_iceberg_am.so`. That
second build is not a Rust test-harness build, so `cfg(test)` is not the
right condition for code that must be compiled into the extension for
`#[pg_test]`.

## cfg Rules

Use these cfgs intentionally:

```rust
#[cfg(test)]
mod tests;
```

This is for ordinary Rust unit tests. These tests run in the host-side Rust
test process.

```rust
#[cfg(feature = "pg_test")]
mod pg_test;
```

This is for pgrx backend tests. The `pg_test` Cargo feature is enabled by
`cargo pgrx test` both when compiling the host-side wrappers and when
building the extension shared library for test installation.

Do not gate `#[pg_test]` modules only with `#[cfg(test)]`; the SQL functions
would be visible to the host test wrapper but absent from the extension
shared library built for PostgreSQL.

For code that prepares callback descriptors, keep registry ownership and
descriptor lifecycle logic independent from the backend callback binding.
Production registration should supply the real `#[pg_guard]` trampolines;
ordinary unit tests should supply host-safe callbacks to the same preparation
logic. Test the real trampolines, and any logic that genuinely needs backend
state, with `#[pg_test]`. Do not replace production callback bodies with
`cfg(test)` panic stubs.

## Current Module Layout

For `pg-iceberg-am/src/access/conversion`, the intended layout is:

```rust
#[cfg(test)]
mod tests;

#[cfg(feature = "pg_test")]
mod pg_test;
```

The files have distinct responsibilities:

- `tests.rs`: ordinary Rust unit tests for pure logic.
- `pg_test.rs`: pgrx backend tests for code paths that require PostgreSQL.

This keeps the Rust module names understandable while preserving the pgrx
execution model.

## Why `pg_test.rs` Declares an Empty `tests` Schema

`pg_test.rs` contains:

```rust
#[pgrx::pg_schema]
mod tests {}

#[pgrx::pg_test(schema = "tests")]
fn test_rows_to_record_batch_empty() {
    // ...
}
```

This is required for pgrx SQL generation.

`pgrx-tests` calls pg tests as:

```sql
SELECT "tests"."<function_name>"();
```

The functions in `pg_test.rs` therefore explicitly target the SQL schema
`tests`:

```rust
#[pgrx::pg_test(schema = "tests")]
```

However, pgrx also requires manually targeted schemas to be declared in its
generated extension SQL graph. If the schema is not declared, schema
generation fails with:

```text
Got manual `schema = "tests"` setting, but that schema did not exist.
```

The empty module:

```rust
#[pgrx::pg_schema]
mod tests {}
```

does not exist to organize Rust test code. Its purpose is to make pgrx
generate:

```sql
CREATE SCHEMA IF NOT EXISTS tests;
```

That gives the manually placed `#[pg_test(schema = "tests")]` functions a
valid PostgreSQL schema.

## Root Cause in `access::conversion`

The failure was traced to Row-to-Arrow tests that called:

```rust
RowRecordBatchBuilder::build()
```

That call path pulls in conversion code that can reference pgrx/PostgreSQL
backend behavior, including numeric and memory-context related support.
When those tests were ordinary `#[test]` functions, the host-side Rust test
binary pulled in unresolved PostgreSQL backend symbols and failed to start
on Linux.

The fix was to move only those Row-to-Arrow tests into `pg_test.rs` and make
them `#[pgrx::pg_test]`.

The Iceberg-schema-to-Arrow-schema tests remained ordinary `#[test]`
functions because they are pure mapping logic and do not need PostgreSQL
backend state.

## `shared_preload_libraries`

`pg_lakebase_runtime` registers the shared launcher and storage worker, while
`pg-iceberg-am` registers its custom WAL resource manager.

For pgrx tests, PostgreSQL must load the extension at postmaster start:

```rust
vec!["shared_preload_libraries = 'pg_lakebase_runtime,pg_iceberg_am'"]
```

Without this, PostgreSQL may reject initialization that must happen before
normal database connections are accepted, such as postmaster-level GUCs or
static background worker registration.

## Test Placement Rules

Use ordinary `#[test]` when all of the following are true:

- The code is pure Rust logic.
- The code can run in a normal Linux process.
- The test does not call SPI, catalog APIs, memory-context APIs, `palloc`,
  or other PostgreSQL backend functions.
- The test does not pull in implementation paths that require PostgreSQL
  backend symbols.

Use `#[pgrx::pg_test]` when any of the following are true:

- The test calls `pg_sys` backend functions.
- The test uses SPI.
- The test depends on Datum conversion, memory contexts, or PostgreSQL error
  handling.
- The test depends on PostgreSQL catalogs, runtime type information, or real
  backend OID semantics.
- The tested function transitively pulls in pgrx code that requires backend
  symbols.

When in doubt, prefer `#[pg_test]` for PostgreSQL-facing paths. Keep pure
mapping and parsing logic as ordinary `#[test]` so it stays fast and does not
require a PostgreSQL cluster.

## Practical Commands

Run host-side unit tests:

```bash
cargo test -p pg-iceberg-am --lib
```

Run pgrx tests with PostgreSQL:

```bash
cargo pgrx test pg17 --package pg-iceberg-am
```

The expected split is:

- host `cargo test`: validates pure Rust unit tests;
- `cargo pgrx test`: validates ordinary Rust tests plus `#[pg_test]`
  wrappers, and runs `#[pg_test]` bodies inside PostgreSQL.

## Design Principle

The boundary is not "Rust test" versus "SQL test". The real boundary is:

> Can this code path run correctly in an ordinary host process, or does it
> require PostgreSQL backend process semantics?

Host-safe code belongs in ordinary `#[test]`. PostgreSQL-dependent code
belongs in `#[pg_test]`.
