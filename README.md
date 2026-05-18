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

- `pg-iceberg-am` is the primary SQL-facing extension.
- Local Iceberg table storage is the currently exercised path. It uses
  PostgreSQL's local file APIs and a custom WAL resource manager for crash
  recovery.
- `pg-lakebase-storage` is a separate Unix-socket object-storage cache service
  library. The object-storage path exists as lower-level infrastructure, but it
  is not yet the default SQL path for `pg-iceberg-am`.
- `pg-lakebase-core` currently exposes TAM framework primitives. FDW support is
  still a project direction, not a completed public API.

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
                              local disk cache   S3 / GCS / Azure
                              (redb + files)     (object_store)
```

`pg-iceberg-am` supports two storage paths depending on the tablespace:

- **Local storage**: reads and writes go directly through PostgreSQL's Virtual File Descriptor (VFD) system with optional WAL logging for crash consistency.
- **Object storage**: the database process communicates with `pg-lakebase-storage` over **Unix domain sockets**. Reads are served from a local disk cache when possible; misses are transparently fetched from the remote object store. Writes go through an explicit **stage → commit** flow tied to database transaction boundaries.

## Workspace

| Crate | Purpose |
|-------|---------|
| [pg-iceberg-am](./pg-iceberg-am) | PostgreSQL extension implementing the Iceberg table access method. |
| [pg-lakebase-core](./pg-lakebase-core) | Framework crate for PostgreSQL TAM implementations. |
| [pg-lakebase-macros](./pg-lakebase-macros) | Procedural macro support, including `#[pg_table_am]`. |
| [iceberg-lite](./iceberg-lite) | Synchronous, PostgreSQL-friendly Iceberg library used by the TAM. |
| [pg-lakebase-storage](./pg-lakebase-storage) | Local object-storage caching service library. |

## Requirements

- Rust 1.95.0 or later
- PostgreSQL 17, including server development files
- `cargo-pgrx` 0.17.0

Install `cargo-pgrx`:

```bash
cargo install --locked cargo-pgrx --version 0.17.0
```

Initialize pgrx for PostgreSQL 17. Use either an existing `pg_config` or let
pgrx download PostgreSQL:

```bash
cargo pgrx init --pg17=/path/to/pg_config
# or
cargo pgrx init --pg17=download
```

## Build

Check the whole workspace:

```bash
cargo check --workspace
```

Build the Iceberg extension crate:

```bash
cargo build --package pg-iceberg-am
```

Generate the extension SQL/schema for inspection:

```bash
cargo pgrx schema pg17 --package pg-iceberg-am
```

## Run With pgrx

For this extension, prefer `install + start` over plain `cargo pgrx run`, so
the pgrx-managed PostgreSQL instance starts with `shared_preload_libraries`.

```bash
PG_CONFIG="$(cargo pgrx info pg-config pg17)"

cargo pgrx install --package pg-iceberg-am --pg-config "$PG_CONFIG"
cargo pgrx stop pg17 || true
cargo pgrx start pg17 \
  --package pg-iceberg-am \
  --postgresql-conf "shared_preload_libraries='pg_iceberg_am'"

cargo pgrx connect pg17 --package pg-iceberg-am
```

Inside `psql`:

```sql
CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;

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

## SQL Regression Tests

Run the `pg_regress` suite for `pg-iceberg-am`:

```bash
cargo pgrx regress pg17 \
  --package pg-iceberg-am \
  --resetdb \
  --postgresql-conf "shared_preload_libraries='pg_iceberg_am'"
```

The regression SQL lives in [pg-iceberg-am/tests/pg_regress/sql](./pg-iceberg-am/tests/pg_regress/sql),
with expected output in [pg-iceberg-am/tests/pg_regress/expected](./pg-iceberg-am/tests/pg_regress/expected).

## Isolation Tests

The isolation specs cover concurrent visibility, commit retry, and savepoint
behavior. They live in [pg-iceberg-am/tests/isolation/specs](./pg-iceberg-am/tests/isolation/specs).

```bash
cargo test --package pg-iceberg-am --test isolation_test -- --nocapture
```

## Install Into PostgreSQL

Install the extension artifacts into a PostgreSQL installation:

```bash
cargo pgrx install \
  --package pg-iceberg-am \
  --release \
  --pg-config /path/to/pg_config
```

Then configure PostgreSQL:

```conf
shared_preload_libraries = 'pg_iceberg_am'
```

Restart PostgreSQL, then create the extension in each database that should use
the Iceberg table access method:

```bash
psql -d your_database -c "CREATE EXTENSION IF NOT EXISTS pg_iceberg_am;"
```

After that, create Iceberg tables with:

```sql
CREATE TABLE events (
    id int,
    payload text
) USING iceberg;
```

## Other Useful Test Commands

Rust-only crates can be tested without starting PostgreSQL:

```bash
cargo test --package iceberg-lite
cargo test --package pg-lakebase-storage
cargo test --package pg-lakebase-core
```

For `pg-iceberg-am`, prefer `cargo pgrx regress` for SQL behavior because it
starts PostgreSQL with the required preload configuration.

## Documentation

- [Core framework](./pg-lakebase-core/README.md)
- [Iceberg access method](./pg-iceberg-am/README.md)
- [Storage service](./pg-lakebase-storage/README.md)
- [Storage design](./pg-lakebase-storage/doc/design.md)

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE)
for details.
