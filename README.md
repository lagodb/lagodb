# pg-lakebase

[![Build Status](https://github.com/robertmu/pg-lakebase/workflows/CI/badge.svg)](https://github.com/robertmu/pg-lakebase/actions)
[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## A lakehouse database built on PostgreSQL

`pg-lakebase` is building a lakehouse database on
[PostgreSQL](https://www.postgresql.org/). The goal is to bring PostgreSQL's SQL
interface, transaction model, and ecosystem to open lakehouse table formats.

The long-term vision includes first-class support for the
[Apache Iceberg](https://iceberg.apache.org/),
[Delta Lake](https://delta.io/), and
[Apache Hudi](https://hudi.apache.org/) lakehouse table formats.

Beyond table formats, one future goal is to add time-series database
capabilities to `pg-lakebase`, using lake tables as the storage foundation for
ingesting, managing, and analyzing time-series data at scale. Another future
goal is to support vector data and exact and approximate nearest-neighbor
search through the PostgreSQL SQL interface, with lake tables in object storage
as the durable data layer. Neither the time-series nor vector capabilities are
implemented today.

Apache Iceberg is the first and currently the only implemented lakehouse table
format. The other formats and capabilities are planned product directions, not
current capabilities.

> [!WARNING]
> This project is under active development and is not recommended for
> production workloads. [`lagodb-iceberg`](./lagodb-iceberg) is the primary
> runnable extension. `pg-delta-am` is only a framework skeleton, not a Delta
> Lake implementation.

## Why pg-lakebase?

- **PostgreSQL as the database interface.** Use ordinary PostgreSQL SQL,
  transactions, drivers, and tools instead of adopting a separate interface
  for lakehouse data.
- **Writable lakehouse tables.** Work with Iceberg-backed PostgreSQL tables
  using common DML, transaction control, and supported schema changes, rather
  than treating lakehouse data as read-only external files.
- **Local and object storage.** Keep Iceberg metadata and Parquet data on the
  local filesystem for simple deployments, or place object-backed tables in
  S3-compatible object storage through storage volumes while using the same
  PostgreSQL SQL interface. GCS and Azure providers are experimental.
- **A unified database direction.** Build from the current Iceberg support
  toward additional open lakehouse formats, time-series database capabilities
  backed by lake tables, and vector search with lake tables in object storage
  as the durable data layer.

## Current Iceberg capabilities

The following capabilities are backed by implementation and regression or
isolation tests in this repository. They do not imply complete coverage of
every Iceberg specification feature or cross-engine interoperability.
Format-version terminology follows the
[Apache Iceberg table specification](https://iceberg.apache.org/spec/#format-versioning),
and isolation terminology follows the
[PostgreSQL 17 transaction isolation documentation](https://www.postgresql.org/docs/17/transaction-iso.html).

| Area | What works today | Current boundary |
|---|---|---|
| PostgreSQL integration | Managed tables through `USING iceberg`; external REST-catalog tables through `iceberg_fdw` | Managed tables use the PostgreSQL-backed catalog; foreign tables preserve ownership in the external catalog. |
| Iceberg format versions | Create Iceberg v1, v2, and v3 tables | Feature coverage varies by version. This is not a blanket claim that every feature in each specification is implemented; for example, row-level `UPDATE` and `DELETE` are rejected for v1 tables. |
| SQL operations | Managed-table `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY`; foreign-table `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `COPY FROM` | Row-level changes use Iceberg delete semantics and therefore depend on the selected format version. |
| Transactions and isolation | Statement-level Iceberg metadata visibility under `READ COMMITTED`, read-your-own-writes, commit, rollback, and savepoints | `SERIALIZABLE` currently strengthens Iceberg write-conflict validation but does not yet provide full PostgreSQL SSI semantics. `REPEATABLE READ` is not supported. |
| Schema evolution | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `DROP NOT NULL` | Other `ALTER TABLE` schema changes are rejected. |
| Storage | Local filesystem and S3-compatible object storage through storage volumes | S3-compatible storage has repository end-to-end coverage. GCS and Azure providers exist but remain experimental. |
| Partitioned tables | PostgreSQL declarative partitioning with partition-routed `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY` | Each Iceberg leaf is managed as its own relation. |
| Scan optimization | Predicate pushdown plus Iceberg file and Parquet row-group pruning for supported expressions | PostgreSQL retains residual predicates when required for correctness; some comparisons are deliberately not pushed when semantics could differ. |
| Maintenance | `VACUUM`, `VACUUM FULL`, and scheduled automatic Iceberg maintenance | Maintenance remains subject to operational limits while the project is under development. |

The managed-table path uses a PostgreSQL-backed Iceberg metadata catalog. The
foreign-table path binds to existing Iceberg REST catalogs and supports
`IMPORT FOREIGN SCHEMA`. Broader interoperability validation with engines such
as Spark, Flink, and Trino remains planned.

## Quick start

The current quick start builds the extensions from source. See
[Build from source](docs/build-from-source.md) for pgrx setup details and the
full installation variants.

Initialize PostgreSQL 17 with pgrx using an existing `pg_config`:

```bash
cargo pgrx init --pg17=/path/to/pg_config
```

Install the shared Lakebase services and the Iceberg extension into that PostgreSQL
installation:

```bash
cargo pgrx install --package pg-lakebase-runtime --pg-config /path/to/pg_config
cargo pgrx install --package lagodb-iceberg --pg-config /path/to/pg_config
```

`lagodb-iceberg` depends on `pg-lakebase-runtime`. `cargo pgrx install`
installs the package named by `--package`, so both commands are required;
installing the Iceberg extension does not install the runtime artifacts.

Preload the runtime, configure the provider libraries it owns, and restart
PostgreSQL:

```conf
shared_preload_libraries = 'pg_lakebase_runtime'
pg_lakebase.provider_libraries = 'lagodb_iceberg'
```

List every enabled Lakebase AM and FDW provider in the second setting. For
example, a cluster using Iceberg and the LagoDB connectors uses:

```conf
pg_lakebase.provider_libraries = 'lagodb_iceberg,lagodb_connectors'
```

Provider libraries are loaded by the runtime during the same postmaster
startup window. Adding or removing one requires a PostgreSQL restart.
An object-URI COPY that no configured provider claims fails explicitly; it is
never passed to PostgreSQL's server-local file COPY implementation.

Then connect to a database and run:

```sql
CREATE EXTENSION IF NOT EXISTS pg_lakebase_runtime;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

CREATE TABLE events (
    event_time  timestamptz NOT NULL,
    device_id   bigint      NOT NULL,
    temperature double precision
) USING iceberg;

BEGIN;

INSERT INTO events VALUES
    (now(), 101, 21.5),
    (now(), 102, 22.0);

UPDATE events
SET temperature = 22.1
WHERE device_id = 101;

COMMIT;

SELECT *
FROM events
WHERE device_id = 101;
```

This creates an Iceberg-backed table that applications can access through
ordinary PostgreSQL SQL.

## Use object storage

Object-backed Iceberg tables are configured through a **storage volume** and a
PostgreSQL tablespace. Storage-volume administration requires a superuser and
is a nontransactional operation. Invoke the administration function as the
only expression in a standalone top-level `SELECT`; do not call it from an
explicit transaction, function, procedure, trigger, `DO` block, CTE, subquery,
or pipelined batch.

The following example uses an S3 bucket and the provider's default credential
chain. Replace the bucket, prefix, region, and local tablespace path for the
deployment. The `LOCATION` directory must be an existing, empty absolute path
that PostgreSQL can use for tablespace metadata.

```sql
SELECT lakebase.create_storage_volume(
    'events-lake',
    's3://my-lake-bucket/pg-lakebase',
    '{"type":"default_chain"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
);

CREATE TABLESPACE lake_s3
LOCATION '/path/to/local/tablespace'
WITH (storage_volume = 'events-lake');

CREATE TABLE object_events (
    event_time timestamptz NOT NULL,
    device_id bigint NOT NULL,
    payload text
) USING iceberg TABLESPACE lake_s3;
```

The same storage-volume API includes experimental providers for `gs://`
locations in Google Cloud Storage and `az://` locations in Azure Blob Storage.
These providers do not yet have the same end-to-end test coverage as the
S3-compatible path. Credentials and provider options are validated by the
runtime and persisted in the PostgreSQL data directory's protected
storage-volume configuration. They are not encrypted by PostgreSQL; use the
deployment's credential and filesystem security controls.

## Roadmap

### Current — Reliable Iceberg tables

Make writable Iceberg tables reliable, interoperable, and straightforward to
deploy from PostgreSQL, with stronger format coverage, object-storage
reliability, compatibility testing, packaging, and performance validation.

### Next — Broader lakehouse format support

Expand the database beyond Iceberg with first-class Delta Lake and Apache Hudi
implementations while preserving a consistent PostgreSQL experience.

### Future — Time-series and vector capabilities

Use lake tables as the storage foundation for time-series ingestion and
analytics, and add vector data and similarity search through the PostgreSQL SQL
interface, with lake tables in object storage as the durable data layer.

These roadmap items describe intended product outcomes. Their implementation
designs will be documented separately as they are validated.

## Architecture

At a high level, `pg-lakebase` integrates lakehouse table implementations with
PostgreSQL's table access, planning, execution, and transaction lifecycle, then
routes table data to local or object storage.

```text
                 PostgreSQL SQL and transactions
                               |
                               v
             +--------------------------------------+
             | lagodb-iceberg                       |
             |                                      |
             | managed tables     foreign tables    |
             | TableAM/CustomScan  iceberg_fdw      |
             +----------+---------------+-----------+
                        |               |
                        |               v
                        |       Iceberg REST catalog
                        |
                        v
              Shared Iceberg scan/write engine
                        |
              +---------+--------------------------+
              |                                    |
              v                                    v
     Local filesystem                         Object storage
                                            storage volumes
                                                   |
                                                   v
                                          pg-lakebase-storage
                                         S3 / GCS / Azure
```

- The managed-table adapter makes `USING iceberg` tables PostgreSQL relations
  and owns their PostgreSQL-backed metadata lifecycle.
- The foreign-table adapter maps `iceberg_fdw` relations to existing tables in
  an Iceberg REST catalog.
- Managed CustomScan and foreign scan paths share predicate and projection
  planning while preserving PostgreSQL evaluation wherever correctness
  requires a residual predicate.
- Transaction-local state provides statement-consistent reads and stages data
  and schema changes until the PostgreSQL transaction boundary.
- The shared worker and storage services route object-backed tables through
  configured storage volumes.

## PostgreSQL support

PostgreSQL 17 is the only currently supported version. Support for PostgreSQL
16, 18, and 19 is planned.

## Project components

- [`lagodb-iceberg`](lagodb-iceberg) provides managed Iceberg tables and
  REST-catalog Iceberg foreign tables through one shared engine.
- [`pg-lakebase-core`](pg-lakebase-core) provides the reusable PostgreSQL
  TableAM, CustomScan, and FDW frameworks, lifecycle adapters, and transaction
  boundaries.
- [`pg-lakebase-runtime`](pg-lakebase-runtime) provides shared workers,
  runtime coordination, and storage-volume administration.
- [`pg-arrow-conv`](pg-arrow-conv) provides Arrow/PostgreSQL value conversion.
- [`iceberg-lite`](iceberg-lite) is the synchronous, PostgreSQL-oriented
  Iceberg library derived from
  [`iceberg-rust`](https://github.com/apache/iceberg-rust).
- [`pg-lakebase-storage`](pg-lakebase-storage) provides the local cache and
  object-storage service used by object-backed tables.

`iceberg-lite` is adapted for PostgreSQL's synchronous execution model and
custom I/O path. Changes to it should preserve a manageable merge path from the
upstream `iceberg-rust` project.

## Documentation and development

- [Build from source](docs/build-from-source.md)
- [Contributing and test commands](CONTRIBUTING.md)
- [`lagodb-iceberg` details](lagodb-iceberg/README.md)
- [`lagodb-iceberg` testing design](lagodb-iceberg/docs/testing.md)
- [`pg-lakebase-core` framework](pg-lakebase-core/README.md)
- [`pg-lakebase-storage` service](pg-lakebase-storage/README.md)
- [`iceberg-lite` adaptation](iceberg-lite/README.md)

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE)
for details.
