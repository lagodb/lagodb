# lagodb-iceberg

[![Rust](https://img.shields.io/badge/rust-1.97.1%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-17-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../LICENSE)

## PostgreSQL-native Apache Iceberg Engine

`lagodb-iceberg` is the Apache Iceberg extension for [LagoDB](../README.md). It provides two native PostgreSQL integration modes backed by a single shared Iceberg execution engine:
1. **Managed Tables (`USING iceberg`)**: PostgreSQL relations with native Table Access Method (TableAM) integration, ACID transactions, and metadata lifecycle management.
2. **Foreign Tables (`lagodb_iceberg`)**: Foreign Data Wrapper (FDW) integration connecting PostgreSQL to existing tables in an external Iceberg REST Catalog without moving data.

Applications query and mutate Iceberg tables using ordinary PostgreSQL SQL and transaction semantics, while data and metadata remain in open Iceberg and Parquet formats.

> [!WARNING]
> This extension is under active development and is not recommended for production workloads. Iceberg is the primary runnable format in this extension; Delta Lake and Apache Hudi are planned for separate extensions.

## Architecture and Components

`lagodb-iceberg` combines PostgreSQL TableAM, CustomScan, and FDW interfaces with a synchronous Iceberg planning, execution, and storage engine.

```text
                  PostgreSQL Planner / Executor
                                |
      +-------------------------+-------------------------+
      |                                                   |
      v (TableAM & CustomScan)                            v (FDW)
+------------------------------------+    +------------------------------------+
| Managed Table Adapter              |    | Foreign Table Adapter              |
| - TableAM scan/insert/update/delete|    | - FDW scan, modify, schema import  |
| - CustomScan predicate pushdown    |    | - Foreign scan predicate pushdown  |
| - PostgreSQL catalog lifecycle     |    | - REST catalog metadata binding    |
+-----------------+------------------+    +-----------------+------------------+
                  |                                         |
                  +-------------------+---------------------+
                                      |
                                      v
+------------------------------------------------------------------------------+
| Shared Iceberg Engine                                                        |
| - Expression planning & predicate classification (Exact / Conservative)      |
| - Schema mapping (PostgreSQL Datums <-> Arrow <-> Iceberg Types)             |
| - Snapshot lifecycle & transaction-local mutation staging                     |
| - Columnar Arrow batch conversion & Parquet file writing                     |
+-------------------------------------+----------------------------------------+
                                      |
                                      v
+------------------------------------------------------------------------------+
| Storage & I/O Subsystem (iceberg-lite / lagodb-base)                 |
| - Local filesystem (with optional WAL integration)                           |
| - Object storage volumes (S3 / MinIO, GCS, Azure Blob Storage)               |
+------------------------------------------------------------------------------+
```

### Core Components

- **Managed Table Adapter (TableAM & CustomScan)**: Connects PostgreSQL TableAM callbacks (scans, tuple mutations, relation lifecycle, VACUUM) to Iceberg sessions, and registers CustomScan paths for predicate pushdown.
- **Foreign Table Adapter (FDW)**: Connects PostgreSQL FDW callbacks (scan, modify, analyze, schema import) to remote Iceberg REST Catalogs.
- **Shared Iceberg Engine**: Unified engine for expression translation, schema mapping, scan iteration, mutation planning, and file formatting across both managed and foreign paths.
- **Transaction-Local Mutation Staging**: Collects data files, delete files, and schema actions within a PostgreSQL transaction and atomically publishes them to the Iceberg catalog at the commit boundary.
- **Synchronous Lake I/O (`iceberg-lite`)**: Synchronous, PostgreSQL-oriented Iceberg metadata and data file reader/writer derived from `iceberg-rust`.

## Key Interaction Flows

### 1. Query Flow (Scan and Predicate Pushdown)

```text
SQL Query (WHERE ...)
  │
  ▼
Planner Path Selection
  ├─ Core / Provider evaluates SQL expressions
  ├─ Classifies predicates (Exact Row Filter vs Conservative Pruning)
  └─ Emits CustomScan (Managed) or ForeignScan (FDW) path
  │
  ▼
Iceberg Pruning & Scan Execution
  ├─ Manifest & Data File Pruning (via partition & column metrics)
  ├─ Parquet Row-Group Pruning
  └─ Synchronous RecordBatch scanning & Datum slot projection
  │
  ▼
PostgreSQL Executor
  └─ Evaluates any residual predicates and returns final tuple slots
```

### 2. Mutation & Write Flow (Transactional DML)

```text
SQL DML (INSERT / UPDATE / DELETE / MERGE)
  │
  ▼
Executor Slot Dispatch
  ├─ Core converts tuple slots into columnar Arrow batches
  └─ Engine writes new Parquet data files or position/equality delete files
  │
  ▼
Transaction-Local Staging
  ├─ Stages new data files and delete files in transaction memory
  └─ Read-your-own-writes visibility within the same transaction
  │
  ▼
Transaction Boundary (COMMIT / ROLLBACK)
  ├─ COMMIT: Atomically commits new Iceberg Snapshot to Catalog (local or REST)
  └─ ROLLBACK / ERROR: Discards staged files and rolls back transaction state
```

### 3. Catalog Synchronization Flow

- **Managed Tables**: The PostgreSQL system catalog manages table definitions and links directly to PostgreSQL-backed Iceberg metadata snapshots.
- **Foreign Tables**: Reads table schemas, snapshots, and partition metadata dynamically from external Iceberg REST Catalogs, supporting `IMPORT FOREIGN SCHEMA`.

## Current Scope and Boundaries

| Capability | Status | Description / Boundary |
|---|---|---|
| Managed Tables (`USING iceberg`) | Available | Full TableAM integration, local/object storage, PostgreSQL-backed catalog |
| Foreign Tables (`lagodb_iceberg`) | Available | REST catalog integration, read-only and read-write modes, `IMPORT FOREIGN SCHEMA` |
| SQL DML Operations | Available | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`, and `COPY` |
| Transactions & Isolation | Available | `READ COMMITTED` statement-level metadata visibility, savepoints, read-your-own-writes |
| Predicate Pushdown | Available | File and row-group pruning with automatic residual qual evaluation in PostgreSQL |
| Storage Backends | Available | Local disk (with WAL replay) and S3-compatible object storage via storage volumes |
| Schema Evolution | Available | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `DROP NOT NULL` |
| Maintenance Operations | Available | `VACUUM`, `VACUUM FULL`, and scheduled automated Iceberg table maintenance |

## Quick Start

### 1. Configure `postgresql.conf`

`lagodb-iceberg` requires `lagodb-base` to coordinate background workers and storage services. Preload both in `postgresql.conf`:

```conf
shared_preload_libraries = 'lagodb_base'
lagodb.provider_libraries = 'lagodb_iceberg'
```

### 2. SQL Usage

```sql
CREATE EXTENSION IF NOT EXISTS lagodb_base;
CREATE EXTENSION IF NOT EXISTS lagodb_iceberg;

-- 1. Managed Iceberg Table (ACID DML)
CREATE TABLE events (
    event_time  timestamptz NOT NULL,
    device_id   bigint      NOT NULL,
    temperature double precision
) USING iceberg;

BEGIN;
INSERT INTO events VALUES (now(), 101, 21.5), (now(), 102, 22.0);
UPDATE events SET temperature = 22.1 WHERE device_id = 101;
COMMIT;

SELECT * FROM events WHERE device_id = 101;

-- 2. Iceberg Foreign Table (External REST Catalog)
CREATE SERVER iceberg_catalog
TYPE 'rest'
FOREIGN DATA WRAPPER lagodb_iceberg
OPTIONS (uri 'https://catalog.example.com');

CREATE USER MAPPING FOR CURRENT_USER
SERVER iceberg_catalog
OPTIONS (credential 'client_id:client_secret');

CREATE FOREIGN TABLE ext_events (
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

SELECT * FROM ext_events WHERE device_id = 101;
```

## Testing

For testing methodology, test matrix guidelines, and test execution commands, see the [Testing Design Document](docs/testing.md).

## Further Reading

- [Root Project README](../README.md)
- [Core Framework README](../lagodb-core/README.md)
- [Build from Source Guide](../docs/build-from-source.md)
- [Testing Architecture](docs/testing.md)

## License

This project is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) for details.
