# LagoDB

[![Build Status](https://github.com/lagodb/lagodb/actions/workflows/ci.yml/badge.svg)](https://github.com/lagodb/lagodb/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17%20%7C%2018-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**A lakehouse database built on PostgreSQL.**

LagoDB brings PostgreSQL's SQL, transactions, and ecosystem to open lakehouse
tables. Your data stays in open formats on local disk or object storage, and
engines such as Apache Spark can read the same tables from object storage.

[Apache Iceberg](https://iceberg.apache.org/) is supported today.
[Delta Lake](https://delta.io/) and [Apache Hudi](https://hudi.apache.org/) are planned.

> [!WARNING]
> LagoDB is under active development and is not ready for production
> workloads. [`lagodb-iceberg`](lagodb-iceberg) and
> [`lagodb-connectors`](lagodb-connectors) provide the user-facing features.
> [`pg-delta-am`](pg-delta-am) is a framework skeleton, not a Delta Lake
> implementation.

## Why LagoDB?

- **PostgreSQL is the interface.** Use the SQL, transactions, drivers, and
  tools you already know instead of adopting a separate interface for
  lakehouse data.
- **Transactional Iceberg tables.** `USING iceberg` tables support `INSERT`,
  `UPDATE`, `DELETE`, and `MERGE` with PostgreSQL commit, rollback, and
  savepoints.
- **Open tables that other engines can use.** Tables are stored as Iceberg
  metadata and Parquet files. Spark can read the tables that LagoDB manages in
  object storage, and both engines can read and modify tables in an Iceberg
  REST catalog.
- **Analytics run in a vectorized engine.** Eligible filters, joins, and
  aggregations run in an embedded [Apache DataFusion](https://datafusion.apache.org/)
  engine instead of PostgreSQL's row-at-a-time executor.
- **Work with data where it already lives.** Query Iceberg tables in an external
  catalog as foreign tables, query Parquet, CSV, JSON, Avro, and text files in
  S3, GCS, or Azure Blob Storage without loading them first, and import or
  export them with `COPY`.

## Current capabilities

Regression and isolation tests in this repository cover the capabilities below,
except where the boundary column says otherwise.
Format versions follow the
[Iceberg table specification](https://iceberg.apache.org/spec/#format-versioning),
and isolation levels follow
[PostgreSQL's documentation](https://www.postgresql.org/docs/current/transaction-iso.html).

| Area | What works today | Current boundary |
|---|---|---|
| Managed tables | `USING iceberg` tables with `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY` | Stored in a PostgreSQL-backed Iceberg catalog. |
| Iceberg format versions | v1, v2, and v3 tables | `UPDATE` and `DELETE` need v2 or later. They write position deletes in v2 and deletion vectors in v3. |
| Transactions | Statement-level snapshots under `READ COMMITTED`, read-your-own-writes, commit, rollback, and savepoints | `SERIALIZABLE` adds stricter write-conflict checks but is not full PostgreSQL SSI. `REPEATABLE READ` support is partial: DDL, and reads and inserts on external Iceberg tables, run; reads and writes on managed tables and `UPDATE` or `DELETE` on external tables report an error. |
| Partitioned tables | PostgreSQL `PARTITION BY RANGE`, `LIST`, and `HASH` syntax, including multi-field keys that define one composite Iceberg partition spec | The table is stored as one partitioned Iceberg table, and PostgreSQL sees an ordinary table. |
| Schema changes | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `DROP NOT NULL` | Other `ALTER TABLE` changes, such as `ALTER COLUMN TYPE`, are rejected. |
| Storage | Local filesystem and S3-compatible object storage through storage volumes | GCS and Azure providers are experimental. |
| Foreign Iceberg tables | REST and SQLite catalogs; `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `COPY FROM`; `IMPORT FOREIGN SCHEMA`; optional creation of missing remote tables | Each table remains owned by its external catalog. |
| Spark interoperability | Spark reads managed tables through the JDBC catalog, and both engines read and modify the same REST-catalog tables, for format versions 1, 2, and 3 | The JDBC catalog is read-only. Spark cannot create or modify managed tables, and tables on local storage are not published. |
| Query execution | DataFusion offload for filters, joins, aggregations, and `DISTINCT`; Iceberg file and Parquet row-group pruning | PostgreSQL rechecks predicates whose semantics could differ, and runs anything DataFusion does not support. |
| Raw data files | Foreign tables and `COPY TO` / `COPY FROM` for Parquet, CSV, JSON, Avro, and text on local disk, S3, GCS, and Azure | Single-file foreign tables are read-only; directory and prefix foreign tables also accept `INSERT`. Tests cover local files and S3-compatible storage. |
| Maintenance | `VACUUM`, `VACUUM FULL`, and scheduled background maintenance | — |

## Quick start

LagoDB is currently built from source with [pgrx](https://github.com/pgcentralfoundation/pgrx).
Choose PostgreSQL 16, 17 or 18 and point `PG_CONFIG` at that installation:

```bash
PG_MAJOR=17 # or 16, 18
PG_CONFIG=/path/to/pg_config

cargo pgrx init "--pg${PG_MAJOR}=${PG_CONFIG}"

cargo pgrx install --package lagodb-base --pg-config "${PG_CONFIG}"
cargo pgrx install --package lagodb-iceberg --pg-config "${PG_CONFIG}"
cargo pgrx install --package lagodb-connectors --pg-config "${PG_CONFIG}"
```

See [Build from source](docs/build-from-source.md) for prerequisites and version
selection, and [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow.

Add LagoDB to `postgresql.conf`, then restart PostgreSQL:

```conf
shared_preload_libraries = 'lagodb_base'
lagodb.provider_libraries = 'lagodb_iceberg,lagodb_connectors'
```

These libraries load at server start, so changing either setting requires a
restart.

Enable the extensions in your database:

```sql
CREATE EXTENSION lagodb_base;
CREATE EXTENSION lagodb_iceberg;
CREATE EXTENSION lagodb_connectors;
```

## Examples

The examples below show what LagoDB does. They leave out setup details such as
credentials and privileges.

### Iceberg tables with PostgreSQL transactions

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
UPDATE events SET temperature = 22.1 WHERE device_id = 101;
COMMIT;

SELECT device_id, temperature FROM events ORDER BY device_id;
-- 101 | 22.1
-- 102 | 22

BEGIN;
DELETE FROM events WHERE device_id = 102;
ROLLBACK;
SELECT count(*) FROM events; -- 2
```

PostgreSQL partitioning syntax creates a partitioned Iceberg table. You do not
create individual partitions, and PostgreSQL treats the result as one ordinary
table:

```sql
CREATE TABLE daily_events (
    event_date  date   NOT NULL,
    device_id   bigint NOT NULL,
    temperature double precision
) PARTITION BY RANGE (event_date) USING iceberg TABLESPACE lake_local;

INSERT INTO daily_events VALUES
    ('2026-10-01', 101, 21.5),
    ('2026-10-02', 102, 22.0);
```

### Tables in object storage

A storage volume binds an empty tablespace to an object storage location.
Tables created in that tablespace store their metadata and data files there,
and you use the same SQL:

```sql
CREATE TABLESPACE lake_s3 LOCATION '/path/to/lake_s3';

SELECT lagodb.create_storage_volume(
    'events-lake',
    'lake_s3',
    's3://my-lake-bucket/lagodb',
    '{"type":"default_chain"}'::jsonb,
    '{"region":"us-east-1"}'::jsonb
);

CREATE TABLE cloud_events (LIKE events) USING iceberg TABLESPACE lake_s3;
INSERT INTO cloud_events SELECT * FROM events;

SELECT device_id, avg(temperature)
FROM cloud_events
GROUP BY device_id;
```

### Reading LagoDB tables from Spark

Spark uses the PostgreSQL database as an Iceberg JDBC catalog. The catalog name
is the database name. See [External managed-table access](lagodb-iceberg/docs/jdbc-catalog.md)
for the full configuration:

```properties
spark.sql.catalog.analytics_db=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.analytics_db.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog
spark.sql.catalog.analytics_db.uri=jdbc:postgresql://host:5432/analytics_db
spark.sql.catalog.analytics_db.jdbc.schema-version=V1
spark.sql.catalog.analytics_db.warehouse=s3://my-lake-bucket
spark.sql.catalog.analytics_db.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

```sql
-- In Spark SQL
SELECT device_id, avg(temperature)
FROM analytics_db.public.cloud_events
GROUP BY device_id;
```

### Existing Iceberg tables in an external catalog

An empty column list makes LagoDB read the columns from the remote Iceberg
schema:

```sql
CREATE SERVER iceberg_catalog
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri 'https://catalog.example.com');

CREATE FOREIGN TABLE ext_events ()
SERVER iceberg_catalog
OPTIONS (table 'analytics.events');

SELECT * FROM ext_events WHERE device_id = 101;
```

### Raw files in object storage

The file extension selects the format, and an empty column list makes LagoDB
read the columns from the file. Local file paths are supported as well:

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

- **Now: performance.** Benchmark the Iceberg, connector, and query offload
  paths, and use the results to guide optimization.
- **Next: more table formats.** Add Delta Lake and Apache Hudi with the same
  PostgreSQL experience.
- **Later: time-series and vector search.** Build time-series ingestion and
  analytics, plus vector similarity search, on lake tables in object storage.

## PostgreSQL support

LagoDB supports PostgreSQL 16, 17 and 18. PostgreSQL 17 remains the default for
ordinary Cargo commands; selecting PG16 or PG18 explicitly is documented in
[Build from source](docs/build-from-source.md). Other major versions, including
PostgreSQL 15 and 19, are not supported.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the repository layout, build setup,
and testing guide.

## License

LagoDB is licensed under the [Apache License 2.0](LICENSE).
