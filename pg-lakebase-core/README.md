# pg-lakebase-core

[![Rust](https://img.shields.io/badge/rust-1.95.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

**Rust framework primitives for PostgreSQL Table Access Methods.**

`pg-lakebase-core` is the framework crate used by PostgreSQL storage
extensions in this workspace. It wraps PostgreSQL's C-facing callback surface
behind Rust traits, typed handles, and lifecycle helpers so that storage
extensions can be written in safe Rust instead of against raw C pointers.

The crate provides two cooperating frameworks:

- A **Table Access Method (TAM)** framework that models PostgreSQL's scan,
  relation, index, DML, and DDL callbacks as Rust traits.
- A generic **CustomScan filter-pushdown** framework that lets a storage
  provider push SQL `WHERE` predicates down into its own scan, so it can prune
  data files and filter rows before they reach the executor.

FDW support is a workspace-level direction, but it is not part of this crate's
current public API.

The reference consumer is [pg-iceberg-am](../pg-iceberg-am), which implements
an Apache Iceberg TAM and CustomScan provider on top of these frameworks.

## Architecture

`pg-lakebase-core` sits between PostgreSQL/pgrx and a concrete storage
implementation. It owns the unsafe callback boundary so that providers can stay
in safe Rust for their business logic.

```text
        PostgreSQL planner / executor / TableAM
                          |
                          v
                  pg-lakebase-core
   TAM traits + handles  |  CustomScan pushdown framework
   tuple values + batch  |  Expr translation + classification
   options / catalog     |  WAL / resource / transaction cleanup
                          |
                          v
            concrete storage implementation
```

Two seams connect a provider to PostgreSQL:

- The **TableAM seam** handles the storage engine callbacks: scanning,
  inserting, updating, deleting, and the relation/index/DDL lifecycle.
- The **CustomScan seam** plugs into the planner (`set_rel_pathlist_hook`) and
  executor. It is independent of the TableAM callbacks and is what carries
  predicate pushdown.

## What the Framework Provides

Rather than expose ad hoc helpers, the crate is organized around the
PostgreSQL lifecycle boundaries a storage extension has to respect.

**Table access.** A trait-based surface mirrors PostgreSQL's `TableAmRoutine`.
Stateless behavior (relation sizing, index callbacks, DDL) is implemented on
the access-method identity type, while stateful operations (scans, index
fetches, DML) live on associated session types. Callback shims adapt the raw C
entry points to these traits, and typed handles wrap PostgreSQL-owned objects
such as relations, snapshots, scan keys, and tuple slots so that providers
never juggle raw pointers in their main logic.

**Tuple values.** Owned tuple representations (`Cell`, `Row`) coexist with
short-lived slot and datum views used on DML and scan hot paths, so a provider
can choose between materializing rows and consuming columns directly.

**Filter pushdown.** A generic CustomScan framework turns SQL predicates into
provider-native scan predicates. See [Filter Pushdown](#filter-pushdown-customscan)
below.

**Catalog and options.** Helpers for catalog scans/updates, Lakebase catalog
object IDs, and parsing/persisting table and tablespace options (with caching).

**Durability and cleanup.** A custom WAL resource-manager registration path,
plus two distinct cleanup mechanisms: `ResourceOwner`-scoped cleanup for
owner-lifetime resources and ERROR paths, and transaction/subtransaction
callbacks for transaction-scoped publication.

**Supporting infrastructure.** PostgreSQL hook helpers, error reporting and
diagnostics, background-worker scaffolding, and the registration support that
builds and installs a `TableAmRoutine`.

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
the transaction callbacks instead of relying on per-tuple callbacks.

DML tuple flow is slot-first. The callback shims pass slot or batch views into
the AM session. Row-oriented AMs use the default fallback to materialize owned
`Row` values into a row batch buffer; columnar AMs can override the slot
methods and append datum references directly into format-specific column
builders. Core does not depend on Arrow, Parquet, Iceberg, or any other
concrete batch representation.

See [access/dml/README.md](src/access/dml/README.md) for the lifecycle
principles.

## Filter Pushdown (CustomScan)

A normal PostgreSQL TableAM scan never sees ordinary `WHERE` quals — the
executor evaluates them after the scan returns rows. For lakehouse formats this
is wasteful: the storage layer could skip whole files, row groups, or pages if
it knew the predicate. The CustomScan framework closes that gap without binding
the design to any single format.

The framework is a planner-and-executor seam:

```text
SQL WHERE
  -> planner (set_rel_pathlist_hook)
       core classifies each predicate as pushed / residual / recheck
       core enumerates plain and parameterized CustomPath variants
  -> planner picks a path on cost
  -> CustomScan plan
       residual quals stay in plan.qual for PostgreSQL to re-check
       pushed/recheck predicates travel as copyObject-safe PG Expr nodes
  -> executor Begin / ReScan
       core translates the pushed predicates into the provider's
       native predicate, which drives file/row-group pruning
  -> executor scan
       provider returns rows; PostgreSQL applies residual quals
```

The design keeps responsibilities split:

- **Core owns the dangerous parts.** Path enumeration, the planner safety gates
  (movability and security promotion of join clauses), classification,
  `RestrictInfo` unwrapping, cost modeling, and the runtime predicate
  translation seam all live in core, in one place, so providers cannot
  accidentally break query semantics.
- **The provider owns format knowledge.** It decides whether a given predicate
  can be pushed and under which contract, shapes the cost estimate, and
  translates the pushed predicate into its own scan filter at runtime.

Pushdown is governed by an explicit contract so correctness never depends on a
provider getting cost estimates right:

- *Exact row filter* — the provider must apply true row-level filtering; the
  predicate is removed from the residual quals.
- *Conservative pruning* — the provider may only prune candidates with no false
  negatives; the original predicate stays in the residual quals so PostgreSQL
  still guarantees correctness.

The framework supports both plain and parameterized scans, so a pushdown-aware
scan can sit on the inner side of a nested-loop join and re-translate its
predicate as outer-tuple values change. The `pg_lakebase.customscan_mode` GUC
(`off` / `auto` / `force`) controls path emission: `auto` lets the planner
choose on cost, `off` disables the framework entirely, and `force` biases cost
for regression testing without relaxing any gate or changing SQL semantics.

Providers register from `_PG_init` (register all providers first, then call
the framework's `init` to install the planner hook). The full design rationale,
including the PostgreSQL-internals facts it relies on, lives in
[src/customscan/README.md](src/customscan/README.md).

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
session types implement their corresponding lifecycle traits. To add predicate
pushdown, implement the CustomScan provider trait and register it from
`_PG_init`. See [pg-iceberg-am](../pg-iceberg-am) for a complete implementation
of both the TAM and the CustomScan provider.

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
- Predicate pushdown keeps the planner gates, classification, and cost model in
  core; providers only decide format-specific pushability and translation, so a
  provider bug cannot silently change query results.
- ResourceOwner cleanup and transaction callbacks are separate mechanisms and
  should stay separate.
- Unsupported PostgreSQL write paths should fail clearly instead of bypassing
  managed lifecycle handling.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE)
for details.
