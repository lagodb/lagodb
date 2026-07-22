# pg-lakebase-core

[![Rust](https://img.shields.io/badge/rust-1.95.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

**Rust framework primitives for PostgreSQL Table Access Methods.**

`pg-lakebase-core` is the framework crate used by PostgreSQL storage
extensions in this workspace. Its central job is to **own the FFI boundary**:
PostgreSQL's access-method and planner integration is a large surface of C
callbacks over raw pointers, manual memory contexts, and error paths that bypass
normal Rust control flow. This crate concentrates that unsafe glue in one
reviewed place — behind Rust traits, typed handles, and lifecycle helpers — so a
storage provider writes its logic in **safe Rust**, the correctness-critical
parts are implemented once instead of by every provider, and a provider bug
stays confined to that provider's business logic.

On top of that boundary, the crate exposes two pluggable frameworks a provider
implements:

- A **Table Access Method (TAM)** framework that models PostgreSQL's scan,
  relation, index, DML, and DDL callbacks as Rust traits, and adapts the raw C
  entry points to them.
- A generic **CustomScan filter-pushdown** framework that lets a storage
  provider push SQL `WHERE` predicates down into its own scan, so it can prune
  data files and filter rows before they reach the executor.

These rest on a broader set of supporting facilities the crate also owns: a
row/column tuple-value substrate (`Cell`/`Row`, zero-copy slot and datum views,
columnar batch buffers) so providers can materialize rows or consume columns
directly; typed handles wrapping PostgreSQL-owned objects (relations, snapshots,
scan keys, slots); a typed `Expr` layer with walkers and classification that
backs predicate pushdown; catalog scan/update helpers and table/tablespace
option parsing with caching; a custom WAL resource-manager registration path for
crash recovery; PostgreSQL hook helpers, error reporting/diagnostics, and
background-worker scaffolding; and two distinct cleanup mechanisms —
`ResourceOwner`-scoped and transaction-scoped — for the ERROR, abort, and
rollback paths normal Rust returns cannot observe. The
["What the Framework Provides"](#what-the-framework-provides) section breaks
these down.

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
diagnostics, background-worker scaffolding, typed native injection-point call
sites, and the registration support that builds and installs a
`TableAmRoutine`.

## Injection Points

`injection_point::InjectionPoint` represents a statically named PostgreSQL
injection point without allocating at the Rust call site. Concrete subsystems
own their points as associated constants on a subsystem namespace type; core
does not maintain a central catalog of product-specific names.

PostgreSQL 17 points execute only when the target server was configured with
`--enable-injection-points`. PostgreSQL 16 and standard PostgreSQL 17 builds
compile `InjectionPoint::run` to an inline no-op. On an enabled build,
PostgreSQL may allocate while loading or caching an attached callback, so call
sites belong at coarse transaction or process lifecycle boundaries—not in
per-row, per-tuple, or per-write paths. Calls must run on a PostgreSQL backend
or background-worker main thread because the pgrx error boundary uses
PostgreSQL's exception stack.

The C compatibility adapter records PostgreSQL 18's two-argument injection
point ABI, but this does not enable PG18 framework support. There is no `pg18`
Cargo feature, and the shared compatibility gate continues to reject PG18
until every Lakebase C fork has been ported and tested for that major line.

Lakebase deliberately does not expose a production attach/detach API. Tests
install and use PostgreSQL's upstream `injection_points` test extension; see
the workspace [testing instructions](../README.md#testing).

Internal PostgreSQL wrapper modules are intentionally not public API.

## TAM Trait Model

The public API separates callbacks by lifecycle:

- Stateless facets are implemented on the AM identity type:
  `AmScan`, `AmRelation`, `AmIndexCallbacks`, and `AmDdl`.
- Stateful operation lifecycles are associated session types:
  `AmScanSession`, `AmIndexFetchSession`, `AmModifyQueryState`, and
  `AmModifyState`.

This keeps relation-level and DDL callbacks from requiring empty marker state,
while preserving real per-operation state for scans, index fetches, and DML.

`AmResult<T>` is fixed to a small PostgreSQL error handle that owns an
`ErrorReport`. AM implementations can use richer internal error types, but they
should convert to PostgreSQL errors at the callback boundary.

## DML Lifecycle

PostgreSQL exposes tuple-level write callbacks, but lakehouse-style storage
needs query-shared row identity, relation-local writers, metadata staging, and
cleanup. Core owns one typed `ModifyQueryState` per `EState` and AM; every
outer wrapper owns a `ModifyNodeState`, and each matching `ResultRelInfo` owns
one provider `ModifyState`. COPY FROM bypasses ModifyTable and has its own
utility-scoped frame:

```text
ModifyTable starts
  core acquires the EState-scoped AM ModifyQueryState
  core associates target scans with stable ResultRelationState objects
  target scans register physical identity sources in that query state
  each matching relation lazily constructs its AM ModifyState
  C executor callbacks dispatch directly to the cached relation state
ModifyTable succeeds
  constructed ModifyState objects are finalized
ModifyTable fails, aborts, or rolls back
  unfinalized ModifyState objects discard their work
```

`ResourceOwner` cleanup handles ERROR, abort, and rollback paths that normal
Rust returns cannot observe reliably. Transaction-scoped publication should use
the transaction callbacks instead of relying on per-tuple callbacks. AFTER ROW
OLD/NEW values already carried by the executor are retained in a PostgreSQL
tuplestore, rather than re-read from object storage.

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
    type ModifyQueryState = MyModifyQueryState;
    type ModifyState = MyModifyState;
    type CopySession = MyCopySession;
}
```

The AM type also implements the stateless facet traits, while the associated
session types implement their corresponding lifecycle traits. To add predicate
pushdown, implement the CustomScan provider trait and register it from
`_PG_init`. See [pg-iceberg-am](../pg-iceberg-am) for a complete implementation
of both the TAM and the CustomScan provider.

## Requirements

- Rust 1.96.0 or later
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
backend. Because `pg-lakebase-core` is a plain library (`rlib`) and not a
loadable extension, its `#[pg_test]` tests cannot run from inside this crate.
They live in the workspace's shared backend-test extension
[`pg-backend-tests`](../pg-backend-tests), which aggregates the `#[pg_test]`
tests for every framework library crate, and are run with:

```bash
cargo pgrx test pg17 --package pg-backend-tests
```

Test modules there mirror this crate's source structure under the
`lakebase_core` module — for example, tests for `pg_lakebase_core::tuple::Cell`
live under `pg-backend-tests/src/lakebase_core/tuple/`. See the
[pg-backend-tests README](../pg-backend-tests/README.md) for why the backend
tests are aggregated in one crate and how to add new modules.

#### Prerequisites

Initialize pgrx with the target PostgreSQL installation:

```bash
cargo pgrx init --pg17=download \
  --configure-flag=--enable-injection-points
```

The flag is required by the workspace's native fault-injection tests.
`cargo xtask test-all pg17` validates the capability and installs the upstream
PostgreSQL test extension used to attach callbacks.

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
