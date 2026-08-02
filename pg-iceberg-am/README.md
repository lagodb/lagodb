# pg-iceberg-am

[![Rust](https://img.shields.io/badge/rust-1.96.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

## PostgreSQL-native Apache Iceberg tables

`pg-iceberg-am` is the current SQL-facing extension in
[`pg-lakebase`](../README.md). It exposes Apache Iceberg tables through
PostgreSQL's **Table Access Method (TAM)** interface, so applications use
ordinary SQL and PostgreSQL transaction semantics while metadata and data files
are managed as Iceberg and Parquet.

> [!WARNING]
> This extension is under active development and is not recommended for
> production workloads. The repository's primary runnable format is Iceberg;
> Delta and Hudi are future format targets, not interchangeable implementations
> in this extension.

## What it provides

- **Table Access Method integration.** Iceberg tables are PostgreSQL relations
  created with `USING iceberg`, rather than tables accessed through a separate
  query API.
- **Transactional DML.** The current SQL path covers `SELECT`, `INSERT`,
  `UPDATE`, `DELETE`, `MERGE`, and `COPY`, with transaction-local staging
  and commit-boundary publication.
- **Predicate pushdown.** The `pg-lakebase-core` CustomScan framework can
  prune Iceberg files and row groups before rows reach the executor, while
  PostgreSQL retains residual predicates when required for correctness.
- **Local and object-backed storage.** Local tables use PostgreSQL's file path
  and optional access-method WAL integration. Object-backed tables use the
  shared storage-volume runtime and cache service.
- **Iceberg metadata and Parquet files.** `iceberg-lite` supplies the
  synchronous PostgreSQL-oriented Iceberg metadata and I/O path.

## Quick start

Build and install the runtime and access method by following the repository
[build-from-source guide](../docs/build-from-source.md). The runtime is a
separate extension and is required by `pg-iceberg-am`; install both packages
and preload both libraries before creating the extensions.

After PostgreSQL has been restarted with:

```conf
shared_preload_libraries = 'pg_lakebase_runtime,pg_iceberg_am'
```

Create the extensions and an Iceberg table:

```sql
CREATE EXTENSION IF NOT EXISTS pg_lakebase_runtime;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

CREATE TABLE events (
    event_time  timestamptz NOT NULL,
    device_id   bigint      NOT NULL,
    temperature double precision
) USING iceberg;

INSERT INTO events VALUES
    (now(), 101, 21.5),
    (now(), 102, 22.0);

SELECT *
FROM events
WHERE device_id = 101;
```

Use `EXPLAIN (VERBOSE)` to inspect whether the planner selected the Iceberg
CustomScan path:

```sql
EXPLAIN (VERBOSE)
SELECT *
FROM events
WHERE device_id = 101;
```

## Current scope

The following table describes this extension's current boundary. It does not
claim external-reader interoperability until that is tested explicitly.

| Capability | Status | Boundary |
|---|---|---|
| Create and query `USING iceberg` tables | Available | Covered by SQL regression tests |
| `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY` | Available | Covered by DML and partitioned-write tests |
| Transaction-local visibility, rollback, and savepoint cleanup | Available | Covered by transaction and resource-lifecycle tests |
| CustomScan predicate pushdown | Available, limited | Float and numeric comparisons remain disabled where PostgreSQL semantics would diverge |
| Local filesystem storage with optional WAL integration | Available | Local crash/replay and cleanup paths are tested |
| S3-compatible object storage through storage volumes | Available, tested | Covered by the repository's object-storage fixture |
| GCS and Azure storage providers | Experimental | Provider implementations exist without the same end-to-end coverage as S3 |
| `ALTER TABLE` schema evolution | Available, limited | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `DROP NOT NULL` are covered; unsupported forms are rejected |
| `VACUUM`, `VACUUM FULL`, and automatic maintenance | Available, limited | Maintenance and vacuum paths are tested; operational limits still apply |
| Spark/Flink/Trino interoperability | Planned validation | No external-reader interoperability suite is currently in this repository |

## How it works

`pg-iceberg-am` combines PostgreSQL lifecycle integration with Iceberg-specific
metadata and storage logic:

- **TAM callbacks** connect PostgreSQL scans, DML, relation operations, and DDL
  to Iceberg-backed sessions.
- **CustomScan planning** classifies predicates, creates plain or parameterized
  scan paths, and preserves residual quals when pushdown is only conservative.
- **Transaction-local state** stages schema and file actions across statements,
  then publishes the Iceberg metadata update at the PostgreSQL transaction
  boundary.
- **Storage routing** uses PostgreSQL's local file path for local tables and the
  shared runtime/storage service for object-backed tables.
- **WAL integration** connects local Iceberg file operations to PostgreSQL WAL
  where the relation's WAL policy requires it, including standby/archive replay
  and post-commit cleanup.

The reusable framework boundary is
[`pg-lakebase-core`](../pg-lakebase-core). This extension supplies the
Iceberg-specific provider logic; core owns the generic TAM, CustomScan, typed
handle, and PostgreSQL lifecycle abstractions.

## Predicate pushdown

Without pushdown, a normal TableAM scan returns every row and PostgreSQL
evaluates the `WHERE` clause afterwards. The CustomScan provider can prune
files and row groups before they are read, then let PostgreSQL re-check the
original predicate when necessary.

Two contracts govern what may be pushed:

- **Exact row filter:** the provider applies true row-level filtering, so the
  predicate does not need to be re-evaluated by PostgreSQL.
- **Conservative pruning:** the provider only skips candidates that cannot
  contain a match; PostgreSQL keeps the original predicate as a residual qual.

Pushdown of `float4` / `float8` and `numeric` comparisons is disabled because
the available Arrow filters would diverge from PostgreSQL semantics. `IS NULL`
and `IS NOT NULL` for those columns are unaffected. The
`pg_lakebase.customscan_mode` GUC (`off`, `auto`, or `force`) controls
framework path emission, and
`pg_iceberg_am.customscan_min_scan_fraction` prevents an implausibly small
estimated scan fraction from making the path look free to the planner.

## Build and server configuration

### Requirements

- Rust 1.96.0 or later
- PostgreSQL 17 (current product target; PostgreSQL 16, 18, and 19 are planned
  targets; see [PostgreSQL support](../README.md#postgresql-support))
- `cargo-pgrx` 0.18.1

The repository-level [build-from-source guide](../docs/build-from-source.md)
contains the installation, package, and pgrx-managed-server commands.

### Runtime dependency and preload

`pg-iceberg-am` declares `pg_lakebase_runtime` in its control file, but
PostgreSQL extension dependencies do not install Cargo/pgrx artifacts. Install
`pg-lakebase-runtime` and `pg-iceberg-am` separately.

Both extensions must be loaded at postmaster start:

```conf
shared_preload_libraries = 'pg_lakebase_runtime,pg_iceberg_am'
```

`pg_lakebase_runtime` owns the shared workers and storage service;
`pg-iceberg-am` registers the Iceberg custom WAL resource manager. After
restarting PostgreSQL, create `pg_lakebase_runtime` before
`pg_iceberg_am` in each database that uses Iceberg tables.

`LOAD` and creating the extensions after server start do not replace
`shared_preload_libraries`: the background workers and WAL resource-manager
registration require postmaster startup.

> `cargo pgrx run pg17` is useful for isolated pgrx function work, but it does
> not configure the shared runtime workers and Iceberg WAL resource manager.
> Use the source-install path with explicit preload settings for a realistic
> `pg-iceberg-am` run.

## Testing

The pgrx testing model is documented in
[docs/testing.md](docs/testing.md). Before running the focused AM pgrx test,
install `pg-lakebase-runtime` into the target pgrx PostgreSQL installation:

```bash
cargo pgrx install \
  --package pg-lakebase-runtime \
  --pg-config "$(cargo pgrx info pg-config pg17)"

cargo pgrx test pg17 --package pg-iceberg-am
```

The workspace-level [contributor guide](../CONTRIBUTING.md) contains the full
test command, isolation tests, and object-storage E2E prerequisites.

## Object-backed tablespaces

Object-backed Iceberg tables are bound to a storage volume through a PostgreSQL
tablespace. See the root README's
[object-storage example](../README.md#use-object-storage).

Storage-volume administration requires a superuser and must be issued as a
standalone top-level statement. The tablespace `LOCATION` must be an existing,
empty absolute path that PostgreSQL can use for tablespace metadata.

Storage volumes use a stable internal identity, so renaming the PostgreSQL
tablespace does not rename the backing storage identity. Storage-volume binding
options are immutable after binding; moving an Iceberg relation between
tablespaces is not supported.

The storage-volume configuration is persisted in the PostgreSQL data directory's
protected configuration file. PostgreSQL does not encrypt this file; use the
deployment's credential and filesystem security controls.

## Type mapping limitations

PostgreSQL types are mapped to Iceberg types when a column is added by
`CREATE TABLE` or `ALTER TABLE ... ADD COLUMN`. The mapping is not always
lossless:

- **`numeric` without `(p, s)`:** Iceberg decimal requires fixed precision
  and scale. The extension falls back to `decimal(38, 18)` and emits a
  warning per column at `CREATE TABLE` time. Values outside that representable
  range fail at `INSERT` time; declare `numeric(p, s)` explicitly to avoid
  this.
- **`numeric(p, -k)`:** maps to `decimal(p + |k|, 0)`; table creation is
  rejected when the resulting precision exceeds 38.
- **`numeric(p, s)` with `p > 38`:** rejected because Iceberg decimal
  precision is capped at 38.
- **`json`:** stored as Iceberg `string` using PostgreSQL's textual
  `json_out` representation.
- **`jsonb`:** stored as Iceberg `binary` using PostgreSQL's internal
  `jsonb` varlena format. This is a private `pg-iceberg-am` codec and is
  not portable to other Iceberg readers.
- **Unsupported types:** types without an explicit mapping are rejected.
  Examples include `pg_lsn`, `tsvector`, range types, geometric types,
  custom enum types, and composite types.

The full mapping is defined in
[`src/catalog/schema_builder.rs`](src/catalog/schema_builder.rs), with
position-aware data-path mapping in
[`src/access/column_mapping.rs`](src/access/column_mapping.rs).

## Further reading

- [Root project README](../README.md)
- [Core framework README](../pg-lakebase-core/README.md)
- [Build from source](../docs/build-from-source.md)
- [Testing design](docs/testing.md)

## License

This project is licensed under the Apache License 2.0. See
[LICENSE](../LICENSE) for details.
