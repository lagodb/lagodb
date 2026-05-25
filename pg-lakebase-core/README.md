# pg-lakebase-core

[![Rust](https://img.shields.io/badge/rust-1.95.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

**Rust framework primitives for PostgreSQL Table Access Methods.**

`pg-lakebase-core` is the framework crate used by PostgreSQL storage
extensions in this workspace. It wraps PostgreSQL's C-facing TableAM callbacks
behind Rust traits, typed handles, tuple value abstractions, lifecycle helpers,
catalog utilities, and WAL/resource cleanup infrastructure.

The crate currently focuses on **Table Access Method (TAM)** development. FDW
support is a workspace-level direction, but it is not part of this crate's
current public API.

The reference consumer is [pg-iceberg-am](../pg-iceberg-am), which implements
an Apache Iceberg TAM on top of these traits.

## Architecture

`pg-lakebase-core` sits between PostgreSQL/pgrx and a concrete access-method
implementation:

```text
PostgreSQL TableAmRoutine
        |
        v
pg-lakebase-core
  api traits | access shims | handles | tuple values
  options    | catalog      | WAL     | resource/transaction cleanup
        |
        v
concrete TAM implementation
```

The framework owns the unsafe PostgreSQL callback boundary where possible. TAM
implementations work with Rust traits and typed handles instead of raw C
pointers in their main business logic.

## Module Map

| Module | Purpose |
|--------|---------|
| `api` | Public TAM trait surface: scan, relation, index, DML, and DDL facets. |
| `access` | PostgreSQL callback shims that adapt `TableAmRoutine` calls to the trait API. |
| `batch` | DML batch abstractions: owned row batches and slot/datum columnar batch interfaces. |
| `handles` | Typed wrappers around PostgreSQL-owned FFI objects such as relations, snapshots, scan keys, tuple slots, and DML state. |
| `tuple` | Owned tuple values plus short-lived tuple-slot/datum views for DML hot paths. |
| `options` | Table and tablespace option parsing, persistence, and cache helpers. |
| `catalog` | Catalog scan/update helpers and Lakebase catalog object IDs. |
| `wal` | Custom WAL resource-manager registration and WAL record helpers. |
| `resource` | ResourceOwner-scoped cleanup for owner-lifetime resources and ERROR paths. |
| `transaction` | Transaction and subtransaction lifecycle callbacks for transaction-scoped resources. |
| `hooks` | PostgreSQL hook helpers used by framework features. |
| `diag` | PostgreSQL error reporting and diagnostic helpers. |
| `registry` | `TableAmRoutine` construction and registration support. |

Internal PostgreSQL wrapper modules are intentionally not public API.

## TAM Trait Model

The public API separates callbacks by lifecycle:

- Stateless facets are implemented on the AM identity type:
  `AmScan`, `AmRelation`, `AmIndexCallbacks`, and `AmDdl`.
- Stateful operation lifecycles are associated session types:
  `AmScanSession`, `AmIndexFetchSession`, and `AmDmlSession`.

This keeps relation-level and DDL callbacks from requiring empty marker state,
while preserving real per-operation state for scans, index fetches, and DML.

`AmResult<T>` is fixed to a small PostgreSQL error handle that owns an
`ErrorReport`. AM implementations can use richer internal error types, but they
should convert to PostgreSQL errors at the callback boundary.

## DML Lifecycle

PostgreSQL exposes tuple-level write callbacks, but lakehouse-style storage
usually needs a broader write frame for writers, metadata staging, and cleanup.
The DML framework derives that lifecycle from PostgreSQL execution boundaries:

```text
frame starts
  relation-local session starts on first write
  tuple callbacks are dispatched to the session
frame succeeds
  touched sessions are finalized
frame fails, aborts, or rolls back
  unfinalized sessions discard their work
```

`ResourceOwner` cleanup handles ERROR, abort, and rollback paths that normal
Rust returns cannot observe reliably. Transaction-scoped publication should use
the `transaction` module instead of relying on per-tuple callbacks.

DML tuple flow is slot-first. The callback shims pass `TupleSlotRow` or
`TupleSlotBatch` views into the AM session. Row-oriented AMs use the default
fallback to materialize owned `Row` values and store them in `RowBatchBuffer`;
columnar AMs can override the slot methods and append `PgDatumRef` values
directly into format-specific column builders. Core does not depend on Arrow,
Parquet, Iceberg, or any other concrete batch representation.

See [access/dml/README.md](src/access/dml/README.md) for the lifecycle
principles.

## Quick Start

Most users import the prelude and register an AM with the re-exported
`#[pg_table_am]` macro:

```rust
use pg_lakebase_core::prelude::*;

#[pg_table_am(
    version = "0.1.0",
    author = "Example",
    website = "https://example.com"
)]
pub struct MyTableAm;

impl TableAccessMethod for MyTableAm {
    type ScanSession = MyScanSession;
    type IndexFetchSession = MyIndexFetchSession;
    type DmlSession = MyDmlSession;
}
```

The AM type also implements the stateless facet traits, while the associated
session types implement their corresponding lifecycle traits. See
[pg-iceberg-am](../pg-iceberg-am) for a complete implementation.

## Requirements

- Rust 1.95.0 or later
- PostgreSQL 16 or 17
- pgrx 0.18.x

## Testing

This crate has two categories of tests:

### Pure Rust unit tests (no PostgreSQL required)

These tests exercise logic that does not call PostgreSQL backend functions
(e.g. batch buffer sizing, scan key copy semantics, option parsing):

```bash
cargo test -p pg-lakebase-core
```

### PostgreSQL integration tests (`#[pg_test]`)

Some functionality — like `Cell`'s `IntoDatum`/`FromDatum` round-tripping and
`Display` formatting via `pg_sys::date_out` etc. — requires a real PostgreSQL
backend. These tests live in a **dedicated test extension crate**
[`pg-lakebase-core-tests`](../pg-lakebase-core-tests) and are run with:

```bash
cargo pgrx test pg17 --package pg-lakebase-core-tests
```

#### Why a separate test crate?

`pg-lakebase-core` is a pure library (`lib`) consumed by downstream extension
crates like `pg-iceberg-am`. pgrx's `#[pg_test]` requires loading the code as
a PostgreSQL extension (`cdylib` with `pg_module_magic!()`). Embedding these
extension artifacts directly in `pg-lakebase-core` would risk symbol conflicts
(duplicate `Pg_magic_func`) with downstream extensions that also define
`pg_module_magic!()`.

The separate `pg-lakebase-core-tests` crate isolates all extension plumbing
(`pg_module_magic!()`, `.control` file) so that `pg-lakebase-core` remains a
clean library with zero chance of downstream conflicts.

#### Writing new `#[pg_test]` tests

Test modules in `pg-lakebase-core-tests` mirror this crate's source structure.
For example, tests for `pg_lakebase_core::tuple::Cell` live at
`pg-lakebase-core-tests/src/tuple/cell.rs`.

See the [pg-lakebase-core-tests README](../pg-lakebase-core-tests/README.md)
for the full guide on adding new test modules.

#### Prerequisites

Initialize pgrx with the target PostgreSQL installation:

```bash
cargo pgrx init --pg17=/path/to/pg_config
```

## Building

From the workspace root:

```bash
cargo check --workspace
cargo build -p pg-lakebase-core
```

## Design Notes

- This crate is a framework layer, not a storage format implementation.
- Public APIs should model PostgreSQL lifecycle boundaries explicitly rather
  than leaking ad hoc helper functions into AM code.
- DML hot paths should not materialize owned `Row` values until the AM has
  chosen a row-oriented strategy. Columnar AMs should consume slot/datum views
  directly and keep target-format type decisions outside core.
- ResourceOwner cleanup and transaction callbacks are separate mechanisms and
  should stay separate.
- Unsupported PostgreSQL write paths should fail clearly instead of bypassing
  managed lifecycle handling.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE)
for details.
