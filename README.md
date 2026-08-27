# LagoDB

[![Build Status](https://github.com/robertmu/pg-lakebase/workflows/CI/badge.svg)](https://github.com/robertmu/pg-lakebase/actions)
[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## A lakehouse database built on PostgreSQL

LagoDB brings PostgreSQL's SQL interface, transaction model, and ecosystem to open lakehouse table formats and cloud object storage.

With LagoDB, you can:
- **Manage native Iceberg tables** directly within PostgreSQL (`USING iceberg`) with ACID transactions and local or object storage.
- **Directly query and write external Iceberg tables** in an Iceberg REST catalog as foreign tables without migrating data.
- **Query and exchange object storage files** (Parquet, CSV, JSON, Avro, Text) directly via foreign tables and high-performance object-URI `COPY` commands.

The long-term vision includes first-class support for the
[Apache Iceberg](https://iceberg.apache.org/),
[Delta Lake](https://delta.io/), and
[Apache Hudi](https://hudi.apache.org/) lakehouse table formats.

Beyond table formats, one future goal is to add time-series database
capabilities to LagoDB, using lake tables as the storage foundation for
ingesting, managing, and analyzing time-series data at scale. Another future
goal is to support vector data and exact and approximate nearest-neighbor
search through the PostgreSQL SQL interface, with lake tables in object storage
as the durable data layer. Neither the time-series nor vector capabilities are
implemented today.

Apache Iceberg and LagoDB object storage connectors are currently implemented. The other formats and capabilities are planned product directions, not current capabilities.

> [!WARNING]
> This project is under active development and is not recommended for
> production workloads. [`lagodb-iceberg`](./lagodb-iceberg) and [`lagodb-connectors`](./lagodb-connectors) are the primary
> runnable extensions. `pg-delta-am` is only a framework skeleton, not a Delta
> Lake implementation.

## Why LagoDB?

- **PostgreSQL as the database interface.** Use ordinary PostgreSQL SQL,
  transactions, drivers, and tools instead of adopting a separate interface
  for lakehouse data.
- **Managed and foreign Iceberg tables.** Work with PostgreSQL-managed Iceberg
  tables (`USING iceberg`) with ACID DML and transaction control, or directly
  query external tables in an Iceberg REST catalog as foreign tables without
  moving data.
- **Direct object storage query & data interchange.** Query raw data files in S3,
  GCS, or Azure Blob Storage (Parquet, CSV, JSON, Avro, Text) via foreign tables,
  or perform fast parallel imports and exports using object-URI `COPY` commands
  with `lagodb_connectors`.
- **Local and object storage.** Keep Iceberg metadata and Parquet data on the
  local filesystem for simple deployments, or place object-backed tables in
  S3-compatible object storage through storage volumes while using the same
  PostgreSQL SQL interface. GCS and Azure providers are experimental.
- **A unified database direction.** Build from the current Iceberg and object
  connector support toward additional open lakehouse formats, time-series
  capabilities, and vector search with lake tables in object storage.

## Current capabilities

The following capabilities are backed by implementation and regression or
isolation tests in this repository. They do not imply complete coverage of
every format specification feature or cross-engine interoperability.
Format-version terminology follows the
[Apache Iceberg table specification](https://iceberg.apache.org/spec/#format-versioning),
and isolation terminology follows the
[PostgreSQL 17 transaction isolation documentation](https://www.postgresql.org/docs/17/transaction-iso.html).

| Area | What works today | Current boundary |
|---|---|---|
| PostgreSQL integration | Managed tables through `USING iceberg`; external REST-catalog Iceberg foreign tables; object storage foreign tables for raw data files | Managed tables use the PostgreSQL-backed catalog; foreign tables preserve ownership in the external catalog or object store. |
| Object storage & data interchange | Direct object-URI `COPY TO` / `COPY FROM` and foreign tables over S3, GCS, and Azure for Parquet, CSV, JSON, Avro, and Text | Foreign tables support reading exact files or prefix directories, and append-only `INSERT`. Row-level `UPDATE`/`DELETE` are not supported for raw file foreign tables. |
| Iceberg format versions | Create Iceberg v1, v2, and v3 tables | Feature coverage varies by version. Row-level `UPDATE` and `DELETE` are rejected for v1 tables. |
| SQL operations | Managed-table `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY`; foreign-table `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `COPY FROM` | Row-level changes use Iceberg delete semantics and therefore depend on the selected format version. |
| Transactions and isolation | Statement-level Iceberg metadata visibility under `READ COMMITTED`, read-your-own-writes, commit, rollback, and savepoints | `SERIALIZABLE` currently strengthens Iceberg write-conflict validation but does not yet provide full PostgreSQL SSI semantics. `REPEATABLE READ` is not supported. |
| Schema evolution | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `DROP NOT NULL` | Other `ALTER TABLE` schema changes are rejected. |
| Storage & Volumes | Local filesystem and S3-compatible object storage through storage volumes | S3-compatible storage has repository end-to-end coverage. GCS and Azure providers exist but remain experimental. |
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

Install the LagoDB base extension, the Iceberg extension, and the LagoDB connectors into that PostgreSQL installation:

```bash
cargo pgrx install --package lagodb-base --pg-config /path/to/pg_config
cargo pgrx install --package lagodb-iceberg --pg-config /path/to/pg_config
cargo pgrx install --package lagodb-connectors --pg-config /path/to/pg_config
```

Preload the runtime and configure the provider libraries in `postgresql.conf`, then restart PostgreSQL:

```conf
shared_preload_libraries = 'lagodb_base'
lagodb.provider_libraries = 'lagodb_iceberg,lagodb_connectors'
```

Provider libraries are loaded by the runtime during the postmaster startup window. Adding or removing one requires a PostgreSQL restart. An object-URI COPY that no configured provider claims fails explicitly; it is never passed to PostgreSQL's server-local file COPY implementation.

### Try it in SQL

Connect to your database and enable the extensions:

```sql
CREATE EXTENSION IF NOT EXISTS lagodb_base;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;
CREATE EXTENSION IF NOT EXISTS lagodb_connectors;
```

#### 1. Managed Iceberg tables (`USING iceberg`)

Create and modify PostgreSQL-managed Iceberg tables with full ACID transactional semantics. Managed tables support two storage foundations: **object storage** (via a storage volume tablespace) and the **local filesystem** (via standard local tablespaces or the default tablespace).

**Object-storage backed**

Bind the table to a PostgreSQL tablespace linked to a storage volume so table data (Parquet) and Iceberg metadata live in object storage:

```sql
-- 1. Create a storage volume pointing to your object store (e.g. S3 / MinIO)
SELECT lagodb.create_storage_volume(
    'events-lake',
    's3://my-lake-bucket/lagodb',
    '{"type":"default_chain"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
);

-- 2. Bind the storage volume to a PostgreSQL tablespace
CREATE TABLESPACE lake_s3
LOCATION '/path/to/local/tablespace'
WITH (storage_volume = 'events-lake');

-- 3. Create the managed Iceberg table in that tablespace
CREATE TABLE events (
    event_time  timestamptz NOT NULL,
    device_id   bigint      NOT NULL,
    temperature double precision
) USING iceberg TABLESPACE lake_s3;

-- 4. ACID transactions (writes commit metadata snapshots and Parquet data directly to object storage)
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

**Local filesystem backed**

Store Iceberg tables directly on the local filesystem by using a standard local tablespace (created without a `storage_volume`) or PostgreSQL's default tablespace:

```sql
-- Standard local tablespace or default database storage:
CREATE TABLE local_events (
    event_time  timestamptz NOT NULL,
    device_id   bigint      NOT NULL,
    temperature double precision
) USING iceberg;
```

#### 2. Iceberg foreign tables

Query external Iceberg tables registered in an Iceberg REST catalog:

```sql
CREATE SERVER iceberg_catalog
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri 'https://catalog.example.com');

CREATE USER MAPPING FOR CURRENT_USER
SERVER iceberg_catalog
OPTIONS (credential 'my_client_id:my_client_secret');

-- Automatically fetches and binds columns from the Iceberg REST catalog schema
CREATE FOREIGN TABLE ext_iceberg_events ()
SERVER iceberg_catalog
OPTIONS (
    catalog_name 'production',
    catalog_namespace 'analytics',
    catalog_table_name 'events',
    mode 'read_only'
);

SELECT *
FROM ext_iceberg_events
WHERE device_id = 101;
```

#### 3. LagoDB connectors (Object storage and direct COPY)

Query raw data files (Parquet, CSV, JSON, Avro, Text) on object storage or export/import data directly with object URIs:

```sql
CREATE SERVER s3_store
FOREIGN DATA WRAPPER lagodb_connectors
OPTIONS (
    provider 's3_compatible',
    endpoint 'http://127.0.0.1:9000',
    allow_http 'true',
    scope 's3://analytics/'  -- Enables automatic server matching for URIs under this scope
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER s3_store
OPTIONS (
    access_key_id 'minioadmin',
    secret_access_key 'minioadmin'
);

-- Fast export directly to object storage as Parquet
-- (Server is automatically matched by the longest matching scope 's3://analytics/',
--  and format is automatically inferred from the '.parquet' suffix)
COPY events
TO 's3://analytics/exports/events.parquet';

-- Or query raw Parquet files in object storage as a foreign table
-- (An empty column list '()' automatically infers columns and types from Parquet schema)
CREATE FOREIGN TABLE s3_logs ()
SERVER s3_store
OPTIONS (
    path 's3://analytics/logs/',
    format 'parquet'
);

SELECT *
FROM s3_logs
WHERE id >= 100;
```

## Roadmap

### Current — Reliable Iceberg tables and object storage connectors

Make writable Iceberg tables and LagoDB object storage connectors reliable, interoperable, and straightforward to deploy from PostgreSQL, with strong format coverage, object-storage reliability, compatibility testing, packaging, and performance validation.

### Next — Broader lakehouse format support

Expand the database beyond Iceberg with first-class Delta Lake and Apache Hudi
implementations while preserving a consistent PostgreSQL experience.

### Future — Time-series and vector capabilities

Use lake tables as the storage foundation for time-series ingestion and
analytics, and add vector data and similarity search through the PostgreSQL SQL
interface, with lake tables in object storage as the durable data layer.

These roadmap items describe intended product outcomes. Their implementation
designs will be documented separately as they are validated.

## PostgreSQL support

PostgreSQL 17 is the only currently supported version. Support for PostgreSQL
16, 18, and 19 is planned.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE)
for details.
