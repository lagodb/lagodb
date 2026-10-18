# LagoDB

[![Build Status](https://github.com/lagodb/lagodb/workflows/CI/badge.svg)](https://github.com/lagodb/lagodb/actions)
[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## A lakehouse database built on PostgreSQL

LagoDB brings PostgreSQL's SQL interface, transaction model, and ecosystem to open lakehouse table formats and cloud object storage.

The long-term vision includes first-class support for open lakehouse table
formats such as [Apache Iceberg](https://iceberg.apache.org/),
[Delta Lake](https://delta.io/), and
[Apache Hudi](https://hudi.apache.org/). Currently, Apache Iceberg and LagoDB
object storage connectors are implemented; other formats and future capabilities
are planned product directions.

> [!WARNING]
> This project is under active development and is not recommended for
> production workloads. [`lagodb-iceberg`](./lagodb-iceberg) and [`lagodb-connectors`](./lagodb-connectors) are the primary
> runnable extensions. `pg-delta-am` is only a framework skeleton, not a Delta
> Lake implementation.

## Why LagoDB?

- **PostgreSQL as the database interface.** Use ordinary PostgreSQL SQL,
  transactions, drivers, and tools instead of adopting a separate interface
  for lakehouse data.
- **Vectorized query execution with Apache DataFusion.** Accelerate analytical
  queries by offloading eligible operations—such as aggregations, joins, and
  filters—to an embedded Apache DataFusion vectorized engine for high-throughput
  Arrow-native processing.
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
- **A unified database direction.** Build from the current Iceberg, object
  connector, and query offload support toward additional open lakehouse formats,
  time-series capabilities, and vector search with lake tables in object storage.

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
| Partitioned tables | PostgreSQL partitioned table syntax with support for `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY` | Stored as a single partitioned Iceberg table. |
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

#### 1. Managed Iceberg tables on local storage

Create an Iceberg table, write rows, and query it with ordinary PostgreSQL SQL.
LagoDB manages the Iceberg metadata and Parquet files in the selected tablespace.

```sql
CREATE TABLESPACE lake_local LOCATION '/path/to/lake_local';

CREATE TABLE events (
    event_time  timestamptz NOT NULL,
    device_id   bigint      NOT NULL,
    temperature double precision
) USING iceberg TABLESPACE lake_local;

BEGIN;
INSERT INTO events VALUES
    (now(), 101, 21.5),
    (now(), 102, 22.0);
UPDATE events
SET temperature = 22.1
WHERE device_id = 101;
COMMIT;

SELECT device_id, temperature FROM events ORDER BY device_id;
-- 101 | 22.1
-- 102 | 22

-- Changes can also be rolled back.
BEGIN;
DELETE FROM events WHERE device_id = 102;
ROLLBACK;
SELECT count(*) FROM events; -- 2
```

Omit `TABLESPACE lake_local` to use the database's default tablespace instead.

Partition by day with `RANGE` on a date column. LagoDB manages the Iceberg
partitions automatically:

```sql
CREATE TABLE daily_events (
    event_date  date NOT NULL,
    device_id   bigint NOT NULL,
    temperature double precision
) PARTITION BY RANGE (event_date) USING iceberg TABLESPACE lake_local;

INSERT INTO daily_events VALUES
    ('2026-10-01', 101, 21.5),
    ('2026-10-02', 102, 22.0);

SELECT * FROM daily_events WHERE event_date = DATE '2026-10-01';
```

#### 2. Managed Iceberg tables on object storage

Use the same table definition and SQL with an S3-backed tablespace. First create
a dedicated, empty tablespace, then bind it to a storage volume **before creating
any tables in it**. Prepare its local directory as above; Iceberg metadata and
Parquet files will be stored under the object-storage location.

```sql
CREATE TABLESPACE lake_s3 LOCATION '/path/to/lake_s3';

SELECT lagodb.create_storage_volume(
    'events-lake',
    'lake_s3',
    's3://my-lake-bucket/lagodb',
    '{"type":"default_chain"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
);

CREATE TABLE cloud_events (
    event_time  timestamptz NOT NULL,
    device_id   bigint      NOT NULL,
    temperature double precision
) USING iceberg TABLESPACE lake_s3;

INSERT INTO cloud_events SELECT * FROM events;

SELECT device_id, avg(temperature) AS avg_temperature
FROM cloud_events
GROUP BY device_id
ORDER BY device_id;
```

Partition object-backed data by region with `LIST`:

```sql
CREATE TABLE regional_events (
    region      text NOT NULL,
    device_id   bigint NOT NULL,
    temperature double precision
) PARTITION BY LIST (region) USING iceberg TABLESPACE lake_s3;

INSERT INTO regional_events VALUES
    ('east', 101, 21.5),
    ('west', 102, 22.0);

SELECT * FROM regional_events WHERE region = 'east';
```

#### 3. Query existing Iceberg tables through a REST catalog

Query existing Iceberg tables without copying data or defining columns by hand.

```sql
CREATE SERVER iceberg_catalog
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri 'https://catalog.example.com');

-- Replace the URI above with your catalog endpoint.
-- An empty column list infers columns and types from the remote Iceberg schema.
CREATE FOREIGN TABLE ext_iceberg_events ()
SERVER iceberg_catalog
OPTIONS (table 'analytics.events');

SELECT *
FROM ext_iceberg_events
WHERE device_id = 101;
```

#### 4. Query files and move data with LagoDB connectors

Read and write Parquet, CSV, JSON, Avro, and Text on the local filesystem or
object storage. Start with a file on the PostgreSQL server:

```sql
-- Export the events table to a local Parquet file.
COPY events TO '/tmp/lagodb-events.parquet';

CREATE SERVER local_files FOREIGN DATA WRAPPER lagodb_connectors;

-- The file extension selects the format; columns are inferred automatically.
CREATE FOREIGN TABLE file_events ()
SERVER local_files
OPTIONS (path '/tmp/lagodb-events.parquet');

SELECT * FROM file_events WHERE device_id = 101;

-- Import the file into another managed Iceberg table.
CREATE TABLE imported_events (LIKE events) USING iceberg;
COPY imported_events FROM '/tmp/lagodb-events.parquet';
```

The same workflow works with S3. Configure AWS access for the PostgreSQL server
process, then use your own bucket and region:

```sql
CREATE SERVER s3_store
FOREIGN DATA WRAPPER lagodb_connectors
OPTIONS (provider 's3', region 'us-east-1');

COPY events TO 's3://my-lake-bucket/exports/events.parquet'
WITH (server 's3_store');

CREATE FOREIGN TABLE s3_events ()
SERVER s3_store
OPTIONS (path 's3://my-lake-bucket/exports/events.parquet');

SELECT * FROM s3_events WHERE device_id = 101;
```

## Roadmap

### Current — Reliable Iceberg tables, object storage connectors, and query offload

Make writable Iceberg tables, LagoDB object storage connectors, and DataFusion query offload reliable, interoperable, and straightforward to deploy from PostgreSQL, with strong format coverage, object-storage reliability, compatibility testing, packaging, and performance validation.

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
