# pg-lakebase

[![Build Status](https://github.com/robertmu/pg-lakebase/workflows/CI/badge.svg)](https://github.com/robertmu/pg-lakebase/actions)
[![Rust](https://img.shields.io/badge/rust-1.95.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**The Unified Lakebase Extension Suite for PostgreSQL.**

`pg-lakebase` makes PostgreSQL a first-class citizen in the modern Lakebase ecosystem. By implementing high-performance **Table Access Methods (TAM)** and **Foreign Data Wrappers (FDW)** in Rust — backed by a dedicated local caching storage service — it allows PostgreSQL to query and manage open table formats with native-like performance and semantics.

The current runnable extension is **pg-iceberg-am**, a PostgreSQL Table Access
Method (TAM) for Apache Iceberg tables. It uses `pg-lakebase-core` for the TAM
framework, `iceberg-lite` for Iceberg metadata and file format logic, and pgrx
for PostgreSQL integration.

## Current State

- `pg-iceberg-am` is the primary SQL-facing extension. Its local Iceberg table
  storage path is the default and most exercised path, using PostgreSQL's local
  file APIs and a custom WAL resource manager for crash recovery.
- Predicate pushdown is supported through a CustomScan provider: SQL `WHERE`
  predicates are pushed into the Iceberg scan for file/row-group pruning and
  row-level filtering, instead of scanning everything and filtering in the
  executor.
- Object storage is available through distributed tablespaces backed by
  `pg-lakebase-storage`, a Unix-socket cache service. The storage layer supports
  AWS S3, S3-compatible endpoints, Google Cloud Storage, and Azure Blob Storage.
- `pg-lakebase-core` currently exposes a TAM framework plus a generic CustomScan
  filter-pushdown framework, and `pg-arrow-conv` provides the format-neutral
  Arrow⇆PostgreSQL value conversion both the scan and DML paths rely on. FDW
  support is still a project direction, not a completed public API.

## Architecture Overview

```
                    PostgreSQL backend
                          |
                          |  pgrx hooks (TAM / FDW)
                          v
                   +------------------+      +---------------------+
                   | pg-iceberg-am    | ---> | pg-lakebase-core    |
                   | (Iceberg TAM)    |      | (framework traits)  |
                   +------------------+      +---------------------+
                     /              \
        local storage                object storage
        (VFD + WAL)                  (Unix domain socket)
               /                            \
              v                              v
    local filesystem          +-------------------------------+
                              |     pg-lakebase-storage       |
                              |  transport | protocol | conn  |
                              |  service   | backend  | cache |
                              +-------------------------------+
                                    |                |
                                    v                v
                              local disk cache   S3 / S3-compatible / GCS / Azure
                              (redb + files)     (object_store)
```

`pg-iceberg-am` supports two storage paths depending on the tablespace:

- **Local storage**: reads and writes go directly through PostgreSQL's Virtual
  File Descriptor (VFD) system with optional WAL logging for crash consistency.
- **Object storage**: the database process communicates with
  `pg-lakebase-storage` over **Unix domain sockets**. Reads of cached files use
  a local `pread` fast path that bypasses the socket entirely; control
  operations (open, head, miss fetch, upload) go over the socket. Cache misses
  are transparently fetched from AWS S3, S3-compatible endpoints, Google Cloud
  Storage, or Azure Blob Storage. Writes go through an explicit **stage →
  commit** flow tied to database transaction boundaries.

Object-storage tablespaces intentionally use the PostgreSQL tablespace name as
the storage-service `store_id`, so cache and staging paths remain readable on
disk. Because that name is part of the storage identity, renaming a distributed
tablespace is unsupported.

Tablespace options currently expose `protocol=s3`, `protocol=gcs`, and
`protocol=azure`; use `protocol=s3` with a custom `endpoint` for S3-compatible
services.

Distributed tablespace credentials are currently stored in
`pg_tablespace.spcoptions`. They are redacted from Rust `Debug` output, but the
catalog value itself is not encrypted; production deployments should prefer
credential references, IAM-style ambient credentials, or another secret manager
once that integration exists.

## Workspace

| Crate | Purpose |
|-------|---------|
| [pg-iceberg-am](./pg-iceberg-am) | PostgreSQL extension implementing the Iceberg table access method. |
| [pg-lakebase-core](./pg-lakebase-core) | Framework crate for PostgreSQL TAM implementations and CustomScan predicate pushdown. |
| [pg-arrow-conv](./pg-arrow-conv) | Format-neutral Arrow⇆PostgreSQL value conversion layer shared by Arrow-backed access methods. |
| [pg-backend-tests](./pg-backend-tests) | Single test-only extension hosting the backend (`#[pg_test]`) tests for the framework library crates. |
| [pg-lakebase-macros](./pg-lakebase-macros) | Procedural macro support, including `#[pg_table_am]`. |
| [iceberg-lite](./iceberg-lite) | Synchronous, PostgreSQL-friendly Iceberg library used by the TAM. |
| [pg-lakebase-storage](./pg-lakebase-storage) | Local object-storage caching service library. |
| [xtask](./xtask) | Workspace maintenance commands: `test-all`, `isolation`. |

## Requirements

- Rust 1.95.0 or later
- PostgreSQL 17, including server development files, or a pgrx-managed
  PostgreSQL 17 downloaded during setup
- `cargo-pgrx` 0.18.0

## Setup

Register PostgreSQL 17 with pgrx. Use either an existing `pg_config` or let
pgrx download PostgreSQL:

```bash
cargo pgrx init --pg17=/path/to/pg_config
# or
cargo pgrx init --pg17=download
```

## Build

Build the Iceberg extension crate:

```bash
cargo build --package pg-iceberg-am
```

## Install and Run

Install the extension into the PostgreSQL instance you want to use. Pass the
target PostgreSQL 17 `pg_config`, whether it comes from pgrx-managed PostgreSQL
or an existing PostgreSQL installation:

```bash
cargo pgrx install --package pg-iceberg-am --pg-config /path/to/pg_config
```

Then start or restart PostgreSQL with `shared_preload_libraries='pg_iceberg_am'`.
For a pgrx-managed PostgreSQL 17:

```bash
cargo pgrx start pg17 \
  --package pg-iceberg-am \
  --postgresql-conf "shared_preload_libraries='pg_iceberg_am'"

cargo pgrx connect pg17 --package pg-iceberg-am
```

If the pgrx-managed PostgreSQL instance is already running, stop it before
starting it again so `shared_preload_libraries` is applied.

For an existing PostgreSQL 17, update `postgresql.conf`:

```conf
shared_preload_libraries = 'pg_iceberg_am'
```

Then restart PostgreSQL and connect to the target database.

## Testing

After modifying code, run the standard test suite:

```bash
cargo xtask test-all pg17
```

This runs unit tests, pgrx tests, SQL regression, and isolation tests.

Regression SQL lives in [pg-iceberg-am/tests/pg_regress/sql](./pg-iceberg-am/tests/pg_regress/sql),
isolation specs in [pg-iceberg-am/tests/isolation/specs](./pg-iceberg-am/tests/isolation/specs),
and isolation results are written to `target/isolation/pg17/output_iso/`.

## Package

Build a distributable directory of extension artifacts:

```bash
cargo pgrx package --package pg-iceberg-am --pg-config "$(cargo pgrx info pg-config pg17)"
```

Use `package` when you want to copy the extension artifacts into an image, VM,
or distro package instead of installing directly into a local PostgreSQL
installation.

## Usage

Create the extension once in each database that uses Iceberg tables:

```sql
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;
```

Create a local Iceberg table in PostgreSQL's default tablespace:

```sql
CREATE TABLE events (
    id int,
    payload text,
    created_at timestamp
) USING iceberg;

INSERT INTO events VALUES
    (1, 'hello', now()),
    (2, 'lakebase', now());

SELECT * FROM events ORDER BY id;
```

To use a regular PostgreSQL local tablespace, create the tablespace first and
then place the Iceberg table in it:

```sql
CREATE TABLESPACE lake_local LOCATION '/path/to/local/tablespace';

CREATE TABLE local_events (
    id int,
    payload text
) USING iceberg TABLESPACE lake_local;
```

To use object storage, create a distributed tablespace and then place the
Iceberg table in it. PostgreSQL still requires a local `LOCATION` directory for
the tablespace metadata.

```sql
CREATE TABLESPACE lake_s3 LOCATION '/path/to/local/tablespace' WITH (
    protocol = 's3',
    bucket = 'my-lake-bucket',
    region = 'us-east-1'
);

CREATE TABLE object_events (
    id int,
    payload text
) USING iceberg TABLESPACE lake_s3;

INSERT INTO object_events VALUES
    (1, 'hello'),
    (2, 'lakebase');

SELECT * FROM object_events ORDER BY id;
```

For S3-compatible services, keep `protocol = 's3'` and set `endpoint`.

## Roadmap

The items below are project directions, not committed releases. Each links to
the design notes that describe the problem and the intended approach in more
detail.

| Area | Goal | Status | Design notes |
|------|------|--------|--------------|
| DuckDB query offload | Push fully-pushable SELECT fragments (join / aggregate / sort / limit) to a standalone DuckDB server process, with PostgreSQL receiving only the final result rows. Layers on top of the existing single-relation filter pushdown and falls back to it when a fragment cannot be offloaded. | Design draft, no implementation | [duckdb-offload-roadmap.md](./pg-lakebase-core/docs/duckdb-offload-roadmap.md) |
| Iceberg metadata tracker | Remove statement-time intermediate Iceberg metadata writes. The leading candidate is an auxiliary per-table PostgreSQL heap catalog that tracks data/delete file metadata, so scans consume MVCC-visible file rows directly and Iceberg metadata is synchronized only at commit. | Design notes / TODO in code | [catalog/README.md](./pg-iceberg-am/src/catalog/README.md) |
| Expanded DML | Support `UPDATE` and `DELETE` in addition to `INSERT`, using Iceberg position/equality delete files. | Planned | — |
| Expanded DDL | Support schema evolution through `ALTER TABLE` (add / drop / rename column, type changes where Iceberg allows). | Planned | — |
| Partitioned tables | Support creating and querying Iceberg partitioned tables, including partition-aware pruning on the scan path. | Planned | — |

## Documentation

- [Core framework](./pg-lakebase-core/README.md)
- [Arrow⇆PostgreSQL conversion](./pg-arrow-conv/README.md)
- [Backend integration tests](./pg-backend-tests/README.md)
- [Iceberg access method](./pg-iceberg-am/README.md)
- [Storage service](./pg-lakebase-storage/README.md)
- [Storage design](./pg-lakebase-storage/doc/design.md)

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE)
for details.
