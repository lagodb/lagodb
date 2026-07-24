# pg-lakebase

[![Build Status](https://github.com/robertmu/pg-lakebase/workflows/CI/badge.svg)](https://github.com/robertmu/pg-lakebase/actions)
[![Rust](https://img.shields.io/badge/rust-1.96.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Native Apache Iceberg tables for PostgreSQL

`pg-lakebase` is building an Iceberg-first storage engine for PostgreSQL.
Today, [`pg-iceberg-am`](./pg-iceberg-am) exposes Apache Iceberg tables through
PostgreSQL's **Table Access Method (TAM)** interface. Applications use normal
PostgreSQL SQL and transaction semantics while table metadata and data files
are managed using Iceberg and Parquet.

The current product focus is to make this Iceberg path reliable and useful
before expanding into additional lake-table formats or higher-level workloads.

> [!WARNING]
> This project is under active development and is not recommended for
> production workloads. The primary runnable extension today is
> `pg-iceberg-am`. `pg-delta-am` is an experimental skeleton that delegates
> storage callbacks to PostgreSQL heap; it is not a Delta Lake implementation.

## PostgreSQL version direction

PostgreSQL 17 is the current product target. It is the only version covered by
the current product build, installation, and full-workspace test instructions.

The framework crates expose a PG16 feature, but the workspace's PostgreSQL C
forks and product validation are currently PG17-only. PostgreSQL 16 remains a
planned product target. PostgreSQL 18 and 19 are also planned targets; this
repository does not currently claim build or runtime support for them.

| PostgreSQL | Status | Boundary |
|---|---|---|
| 16 | Planned | Framework feature paths exist; the product access method still needs its compatibility port and full validation |
| 17 | Current | Current `pg-iceberg-am` product path and full documented source/test path |
| 18 | Planned | Compatibility scaffolding exists, but there is no complete workspace feature and test path |
| 19 | Planned | No current implementation or test target |

## Why pg-lakebase?

- **PostgreSQL-native tables.** Create an Iceberg table with `USING iceberg`
  and access it through ordinary SQL, including transactional DML.
- **Lake-native files.** Iceberg metadata files and Parquet data files live
  outside the PostgreSQL heap, on the local filesystem or through a configured
  object storage volume. PostgreSQL still retains the relation and catalog
  state needed to locate and manage the table.
- **Storage-aware scans.** The CustomScan path can push supported predicates
  into the Iceberg scan for file and row-group pruning, while PostgreSQL keeps
  residual predicate checks where the semantics require them.
- **Transaction-aware publication.** Writes and schema changes are staged in
  transaction-local state and published at the PostgreSQL transaction
  boundary.

## Quick start

The current quick start builds the extensions from source. See
[Build from source](docs/build-from-source.md) for pgrx setup details and the
full installation variants.

Initialize PostgreSQL 17 with pgrx using an existing `pg_config`:

```bash
cargo pgrx init --pg17=/path/to/pg_config
```

Install the shared runtime and the Iceberg access method into that PostgreSQL
installation:

```bash
cargo pgrx install --package pg-lakebase-runtime --pg-config /path/to/pg_config
cargo pgrx install --package pg-iceberg-am --pg-config /path/to/pg_config
```

`pg-iceberg-am` depends on `pg-lakebase-runtime`. `cargo pgrx install`
installs the package named by `--package`, so both commands are required;
installing the access method does not install the runtime artifacts.

Add both extensions to `postgresql.conf` and restart PostgreSQL:

```conf
shared_preload_libraries = 'pg_lakebase_runtime,pg_iceberg_am'
```

Then connect to a database and run:

```sql
CREATE EXTENSION IF NOT EXISTS pg_lakebase_runtime;
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

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

This is an Iceberg table, not a PostgreSQL heap table. PostgreSQL provides the
SQL and transaction boundary; `pg-iceberg-am` manages Iceberg metadata and
data files. Use `EXPLAIN (VERBOSE)` to inspect whether PostgreSQL selected the
Iceberg CustomScan path for a supported predicate.

## Current capabilities

The status below reflects code and regression coverage in this repository. It
does not claim cross-engine interoperability until that is tested explicitly.

| Capability | Status | Evidence or boundary |
|---|---|---|
| Create and query tables with `USING iceberg` | Available | `pg-iceberg-am` SQL regression suite |
| `SELECT` with CustomScan predicate pushdown | Available | Pushdown, projection, parameter, join, and residual-qual regression tests |
| `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY` | Available | DML and partitioned-write regression tests |
| PostgreSQL transaction-local visibility, rollback, and savepoint cleanup | Available | Transactional-write and transaction-resource regression tests |
| Local filesystem storage with optional PostgreSQL WAL integration | Available | Local storage and WAL implementation/tests |
| Partitioned Iceberg relations and partition-routed writes | Available | Partitioned-write regression tests |
| `ALTER TABLE` schema evolution | Available, limited | Tested `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `DROP NOT NULL`; unsupported forms are rejected |
| `VACUUM`, `VACUUM FULL`, and automatic maintenance | Available, limited | Maintenance and vacuum regression tests; operational limits still apply |
| S3-compatible object storage through storage volumes | Available, tested with the repository's object-storage fixture | Object-storage regression and storage-service E2E tests |
| GCS and Azure storage providers | Experimental | Provider implementations exist; this repository does not provide the same end-to-end coverage as its S3 fixture |
| Iceberg format versions 1, 2, and 3 | Available, limited | Version-specific option and maintenance coverage; feature-level compatibility is not a blanket claim |
| External Iceberg catalog integration | Planned | The current SQL-facing path uses the PostgreSQL-backed Iceberg metadata catalog |
| Spark/Flink/Trino interoperability | Planned validation | No external-reader interoperability suite is part of this repository yet |
| DataFusion query offload | Design exploration | [Existing design roadmap](pg-lakebase-core/docs/datafusion-offload-roadmap.md) explicitly has no implementation |
| Lake-native time-series ingestion policies | Vision | Background batching, file sizing, compaction, retention, and time partitioning are future product work |
| Delta Lake access method | Experimental skeleton | `pg-delta-am` is loadable for framework coverage but does not store Delta tables |

## Lake-table format direction

Supporting **Apache Iceberg, Delta Lake, and Apache Hudi** through
PostgreSQL-native lake-table access methods is a long-term `pg-lakebase` goal.
The formats do not have the same implementation status today:

| Format | Status today | Boundary |
|---|---|---|
| Apache Iceberg | Current primary implementation | `pg-iceberg-am` is the runnable SQL-facing access method |
| Delta Lake | Experimental skeleton | `pg-delta-am` delegates storage callbacks to PostgreSQL heap and does not implement Delta storage |
| Apache Hudi | Planned | No Hudi access method is implemented in this workspace |

The three-format target describes the product direction, not three currently
interchangeable implementations. Iceberg remains the product validation path;
each additional format requires its own metadata, transaction, storage, and
interoperability validation.

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
WITH (lakebase_storage_volume = 'events-lake');

CREATE TABLE object_events (
    event_time timestamptz NOT NULL,
    device_id bigint NOT NULL,
    payload text
) USING iceberg TABLESPACE lake_s3;
```

The same storage-volume API accepts `gs://` locations for Google Cloud
Storage and `az://` locations for Azure Blob Storage. Credentials and provider
options are validated by the runtime and persisted in the PostgreSQL data
directory's protected storage-volume configuration. They are not encrypted by
PostgreSQL; use the deployment's credential and filesystem security controls.

## Why this architecture?

```text
                 PostgreSQL SQL and transactions
                               |
                               v
             +--------------------------------------+
             | pg-iceberg-am                        |
             | Iceberg TAM + CustomScan             |
             +------------------+-------------------+
                                |
              +-----------------+------------------+
              |                                    |
              v                                    v
     Local filesystem                         Object storage
   VFD + optional WAL                    storage volume + runtime
                                                   |
                                                   v
                                          pg-lakebase-storage
                                                   |
                                  S3 / GCS / Azure backend
```

The architecture follows three PostgreSQL and Iceberg boundaries:

- **Table Access Method integration** makes an Iceberg table a PostgreSQL
  relation rather than a separate query API.
- **Transaction-local staging** lets statements in one PostgreSQL transaction
  see the transaction's staged schema and file changes. Commit materializes
  the changes and publishes the Iceberg metadata update.
- **Separate storage paths** keep local tables on PostgreSQL's file path with
  relation-dependent WAL integration and route object-backed tables through
  the runtime's storage service and cache.

## Roadmap

The roadmap has three stages.

### 1. Make Iceberg a trustworthy PostgreSQL storage engine

- broaden Iceberg specification and schema/partition coverage;
- validate snapshot, manifest, delete-file, and metadata correctness;
- add object-store reliability and external-reader interoperability tests;
- publish reproducible Docker/package workflows and benchmarks before making
  production performance claims.

### 2. Build toward lake-native time-series storage

The long-term goal is to make PostgreSQL a high-ingest frontend for
lake-native time-series data. This requires new capabilities such as
WAL-backed write buffering, background flush, target file sizing, batched
Iceberg commits, time partitioning, compaction, retention, and late/out-of-order
update policies. These are design goals, not current capabilities.

### 3. Prove the reusable lake-table framework

The long-term format target is Apache Iceberg, Delta Lake, and Apache Hudi.
Reuse across additional formats remains to be validated. Iceberg is the current
implementation, Delta is an experimental skeleton, and Hudi remains planned
until its access method exists and passes format-specific compatibility tests.

## Extensible lake-table framework

`pg-iceberg-am` is built on reusable PostgreSQL extension components:

- [`pg-lakebase-core`](pg-lakebase-core) owns the Rust-facing TAM and
  CustomScan framework, PostgreSQL lifecycle adapters, and transaction/cleanup
  boundaries.
- [`pg-lakebase-runtime`](pg-lakebase-runtime) owns shared workers, runtime
  coordination, and storage-volume administration.
- [`pg-arrow-conv`](pg-arrow-conv) provides Arrow/PostgreSQL value conversion.
- [`iceberg-lite`](iceberg-lite) is the synchronous, PostgreSQL-oriented
  Iceberg library derived from [`iceberg-rust`](https://github.com/apache/iceberg-rust).
- [`pg-lakebase-storage`](pg-lakebase-storage) provides the local cache and
  object-storage service used by object-backed tables.

`iceberg-lite` is intentionally adapted for PostgreSQL's synchronous model and
custom IO path. Changes to it must preserve a manageable merge path from the
upstream `iceberg-rust` project.

## Documentation and development

- [Build from source](docs/build-from-source.md)
- [Contributing and test commands](CONTRIBUTING.md)
- [`pg-iceberg-am` details](pg-iceberg-am/README.md)
- [`pg-iceberg-am` testing design](pg-iceberg-am/docs/testing.md)
- [`pg-lakebase-core` framework](pg-lakebase-core/README.md)
- [`pg-lakebase-storage` service](pg-lakebase-storage/README.md)
- [`iceberg-lite` adaptation](iceberg-lite/README.md)

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE)
for details.
