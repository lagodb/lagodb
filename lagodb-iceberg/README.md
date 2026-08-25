# lagodb-iceberg

[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

## PostgreSQL-native Apache Iceberg tables

`lagodb-iceberg` is the Apache Iceberg extension in
[`pg-lakebase`](../README.md). It exposes two PostgreSQL-native integration
modes backed by one Iceberg engine: managed tables created with `USING iceberg`
and foreign tables bound to an Iceberg REST catalog through `iceberg_fdw`.
Applications use ordinary SQL and PostgreSQL transaction semantics in both
modes while table metadata and data files remain Iceberg and Parquet.

> [!WARNING]
> This extension is under active development and is not recommended for
> production workloads. The repository's primary runnable format is Iceberg;
> Delta and Hudi are future format targets, not interchangeable implementations
> in this extension.

## What it provides

- **Managed Iceberg tables.** PostgreSQL relations created with `USING iceberg`
  use the Table Access Method integration, PostgreSQL-backed metadata catalog,
  and local or storage-volume-backed data.
- **Iceberg foreign tables.** `iceberg_fdw` binds PostgreSQL foreign tables to
  existing tables in an Iceberg REST catalog, including schema import and
  explicit read-only or read-write modes.
- **Transactional DML.** Managed tables cover `SELECT`, `INSERT`, `UPDATE`,
  `DELETE`, `MERGE`, and `COPY`; writable foreign tables cover the tested
  `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `COPY FROM` paths. Mutations use
  transaction-local staging and commit-boundary publication.
- **Predicate pushdown.** The `pg-lakebase-core` CustomScan framework can
  prune Iceberg files and row groups before rows reach the executor, while
  PostgreSQL retains residual predicates when required for correctness.
- **Local and object-backed storage.** Local tables use PostgreSQL's file path
  and optional access-method WAL integration. Object-backed tables use the
  shared storage-volume runtime and cache service.
- **Iceberg metadata and Parquet files.** `iceberg-lite` supplies the
  synchronous PostgreSQL-oriented Iceberg metadata and I/O path.

## Quick start

Build and install the runtime and Iceberg extension by following the repository
[build-from-source guide](../docs/build-from-source.md). The runtime is a
separate extension and is required by `lagodb-iceberg`; install both packages
and configure Iceberg as a runtime-loaded provider before creating the extensions.

After PostgreSQL has been restarted with:

```conf
shared_preload_libraries = 'pg_lakebase_runtime'
pg_lakebase.provider_libraries = 'lagodb_iceberg'
```

Create the extensions and a managed Iceberg table:

```sql
CREATE EXTENSION IF NOT EXISTS pg_lakebase_runtime;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

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

The same extension also installs `iceberg_fdw`. A REST-catalog table can be
bound as a foreign table without transferring catalog ownership to PostgreSQL:

```sql
CREATE SERVER iceberg_catalog
TYPE 'rest'
FOREIGN DATA WRAPPER iceberg_fdw
OPTIONS (uri 'https://catalog.example.com');

CREATE USER MAPPING FOR CURRENT_USER SERVER iceberg_catalog;

CREATE FOREIGN TABLE external_events (
    event_time  timestamptz NOT NULL,
    device_id   bigint      NOT NULL,
    temperature double precision
)
SERVER iceberg_catalog
OPTIONS (
    catalog_name 'production',
    catalog_namespace 'analytics',
    catalog_table_name 'events',
    mode 'read_only'
);
```

REST-catalog connection properties belong to the foreign server and
role-specific credentials belong to its user mapping.

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
| Managed tables created with `USING iceberg` | Available | PostgreSQL-backed catalog; covered by SQL regression tests |
| REST-catalog foreign tables through `iceberg_fdw` | Available | Read-only/read-write modes and `IMPORT FOREIGN SCHEMA`; covered by object-storage regression tests |
| Managed-table `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY` | Available | Covered by DML and partitioned-write tests |
| Foreign-table `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `COPY FROM` | Available | Covered by REST-catalog regression tests |
| Transaction-local visibility, rollback, and savepoint cleanup | Available | Covered by transaction and resource-lifecycle tests |
| Managed CustomScan and foreign-scan predicate pushdown | Available, limited | Float and numeric comparisons remain disabled where PostgreSQL semantics would diverge |
| Local filesystem storage with optional WAL integration | Available | Local crash/replay and cleanup paths are tested |
| S3-compatible object storage through storage volumes | Available, tested | Covered by the repository's object-storage fixture |
| GCS and Azure storage providers | Experimental | Provider implementations exist without the same end-to-end coverage as S3 |
| `ALTER TABLE` schema evolution | Available, limited | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `DROP NOT NULL` are covered; unsupported forms are rejected |
| `VACUUM`, `VACUUM FULL`, and automatic maintenance | Available, limited | Maintenance and vacuum paths are tested; operational limits still apply |
| Spark/Flink/Trino interoperability | Planned validation | No external-reader interoperability suite is currently in this repository |

## How it works

`lagodb-iceberg` combines two PostgreSQL adapters with shared Iceberg-specific
planning, metadata, transaction, and storage logic:

- **Managed-table callbacks** connect PostgreSQL TableAM scans, DML, relation
  operations, DDL, maintenance, and local WAL to Iceberg-backed sessions.
- **Foreign-table callbacks** connect FDW scan, modify, analyze, schema import,
  and DDL validation to external Iceberg REST catalogs.
- **Shared engine code** owns predicate binding, schema conversion, scan
  execution, mutation planning, and data-file writing for both adapters.
- **CustomScan planning** classifies predicates, creates plain or parameterized
  scan paths, and preserves residual quals when pushdown is only conservative.
- **Transaction-local state** stages schema and file actions across statements,
  then publishes the Iceberg metadata update at the PostgreSQL transaction
  boundary.
- **Storage routing** uses PostgreSQL's local file path for local tables and the
  shared worker/storage service for object-backed tables.
- **WAL integration** connects local Iceberg file operations to PostgreSQL WAL
  where the relation's WAL policy requires it, including standby/archive replay
  and post-commit cleanup.

The reusable framework boundary is
[`pg-lakebase-core`](../pg-lakebase-core). This extension supplies the
Iceberg-specific provider logic; core owns the generic TAM, CustomScan, typed
handle, and PostgreSQL lifecycle abstractions.

## Predicate pushdown

Without pushdown, an Iceberg scan returns every row and PostgreSQL evaluates the
`WHERE` clause afterwards. The managed-table CustomScan provider and the
foreign-table scan provider can prune files and row groups before they are
read, then let PostgreSQL re-check the original predicate when necessary.

Two contracts govern what may be pushed:

- **Exact row filter:** the provider applies true row-level filtering, so the
  predicate does not need to be re-evaluated by PostgreSQL.
- **Conservative pruning:** the provider only skips candidates that cannot
  contain a match; PostgreSQL keeps the original predicate as a residual qual.

Pushdown of `float4` / `float8` and `numeric` comparisons is disabled because
the available Arrow filters would diverge from PostgreSQL semantics. `IS NULL`
and `IS NOT NULL` for those columns are unaffected. The
`pg_lakebase.customscan_mode` GUC (`off`, `auto`, or `force`) controls
managed-table CustomScan path emission, and
`lagodb_iceberg.customscan_min_scan_fraction` prevents an implausibly small
estimated managed-table scan fraction from making that path look free to the
planner. Foreign tables use PostgreSQL's FDW path callbacks instead.

## Build and server configuration

### Requirements

- Rust 1.97.1 or later
- PostgreSQL 17 (current product target; PostgreSQL 16, 18, and 19 are planned
  targets; see [PostgreSQL support](../README.md#postgresql-support))
- `cargo-pgrx` 0.19.2

The repository-level [build-from-source guide](../docs/build-from-source.md)
contains the installation, package, and pgrx-managed-server commands.

### Runtime dependency and preload

`lagodb-iceberg` declares `pg_lakebase_runtime` in its control file, but
PostgreSQL extension dependencies do not install Cargo/pgrx artifacts. Install
`pg-lakebase-runtime` and `lagodb-iceberg` separately.

The runtime must be preloaded and must load the Iceberg provider at postmaster
start:

```conf
shared_preload_libraries = 'pg_lakebase_runtime'
pg_lakebase.provider_libraries = 'lagodb_iceberg'
```

`pg_lakebase_runtime` owns the shared workers and storage service;
`lagodb-iceberg` registers the Iceberg custom WAL resource manager. After
restarting PostgreSQL, create `pg_lakebase_runtime` before
`lagodb_iceberg` in each database that uses Iceberg tables.

`LOAD` and creating the extensions after server start do not replace the
runtime bootstrap configuration: the background workers and WAL
resource-manager registration require postmaster startup.

> `cargo pgrx run pg17` is useful for isolated pgrx function work, but it does
> not configure the shared Lakebase workers and Iceberg WAL resource manager.
> Use the source-install path with explicit preload settings for a realistic
> `lagodb-iceberg` run.

## Testing

The pgrx testing model is documented in
[docs/testing.md](docs/testing.md). Before running the focused Iceberg pgrx test,
install `pg-lakebase-runtime` into the target pgrx PostgreSQL installation:

```bash
cargo pgrx install \
  --package pg-lakebase-runtime \
  --pg-config "$(cargo pgrx info pg-config pg17)"

cargo pgrx test pg17 --package lagodb-iceberg
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
  `jsonb` varlena format. This is a private `lagodb-iceberg` codec and is
  not portable to other Iceberg readers.
- **Unsupported types:** types without an explicit mapping are rejected.
  Examples include `pg_lsn`, `tsvector`, range types, geometric types,
  custom enum types, and composite types.

Managed-table schema construction is defined in
[`src/managed_table/catalog/schema_builder.rs`](src/managed_table/catalog/schema_builder.rs),
with the shared position-aware data-path mapping in
[`src/engine/schema/column_mapping.rs`](src/engine/schema/column_mapping.rs).

## Further reading

- [Root project README](../README.md)
- [Core framework README](../pg-lakebase-core/README.md)
- [Build from source](../docs/build-from-source.md)
- [Testing design](docs/testing.md)

## License

This project is licensed under the Apache License 2.0. See
[LICENSE](../LICENSE) for details.
