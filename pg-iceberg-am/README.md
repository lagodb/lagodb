# pg-iceberg-am

[![Rust](https://img.shields.io/badge/rust-1.90.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Apache Iceberg Table Access Method (TAM) for PostgreSQL**

`pg-iceberg-am` is a PostgreSQL extension implemented in Rust that provides native support for the [Apache Iceberg](https://iceberg.apache.org/) table format. It is built using the [pg-lakebase-core](https://github.com/robertmu/pg-lakebase) framework and leverages [pgrx](https://github.com/tcdi/pgrx) for deep integration with PostgreSQL's internal engine.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Testing](#testing)
- [Usage](#usage)

## Overview

`pg-iceberg-am` allows PostgreSQL to treat Apache Iceberg tables as native tables. Unlike Foreign Data Wrappers (FDW), which operate at the query planning level, `pg-iceberg-am` implements the **Table Access Method (TAM)** interface, providing tighter integration with the storage engine, transaction management, and recovery systems.

This implementation allows for:
- Native SQL support (INSERT, SELECT, UPDATE, DELETE)
- Predicate pushdown into the Iceberg scan via a CustomScan provider
- Recovery through custom WAL (Write-Ahead Log) resource managers

## Key Features

- **Native TAM Integration**: Implements the `TableAmRoutine` to hook directly into PostgreSQL's scan and modification paths.
- **Predicate Pushdown (CustomScan)**: Implements the `pg-lakebase-core` CustomScan provider so that SQL `WHERE` predicates are pushed into the Iceberg scan, enabling file/row-group pruning and row-level filtering instead of scanning everything and filtering in the executor.
- **Iceberg Support**: Uses `iceberg-lite` (a synchronous, PostgreSQL-friendly fork of `iceberg-rust`) to manage Iceberg metadata and data files.
- **Storage Flexibility**: Supports local storage and S3-compatible object storage via the `object-store` crate.
- **Custom WAL RMGR**: Includes a custom WAL resource manager to ensure data consistency and recovery for Iceberg metadata during PostgreSQL crashes.
- **Schema Mapping**: Automatic bidirectional mapping between PostgreSQL types and Iceberg/Arrow types.
- **Parquet Integration**: High-performance data access using the Parquet file format.

## Architecture

`pg-iceberg-am` is split into several logical components:

- **Scan & DML**: Optimized table scanning and data modification logic.
- **CustomScan Provider**: Implements the `pg-lakebase-core` CustomScan provider trait. It classifies which `WHERE` predicates can be pushed down, builds parameterized/plain scan paths with pruning-aware cost estimates, and at execution time translates the pushed PostgreSQL `Expr` nodes into native `iceberg-lite` predicates that drive the same scan core used by the seqscan path.
- **Catalog Management**: Handles Iceberg snapshot creation, metadata updates, and expiration.
- **Storage Layer**: Abstracted I/O for reading and writing data/metadata files to various backends.
- **WAL RMGR**: Ensures that Iceberg metadata changes are atomic and recoverable in sync with PostgreSQL transactions.

### Predicate pushdown

Without pushdown, a normal TableAM scan returns every row and PostgreSQL
evaluates the `WHERE` clause afterwards. The CustomScan provider lets the
planner choose an Iceberg scan that prunes data files and row groups before
they are read, then re-checks correctness in the executor as needed. Two
contracts govern what may be pushed:

- *Exact row filter* — the predicate is applied as a true row-level filter, so
  it does not need to be re-evaluated by PostgreSQL.
- *Conservative pruning* — the predicate may only skip candidates with no false
  negatives; PostgreSQL keeps the original predicate as a residual qual to
  guarantee correct results.

Pushdown of `float4` / `float8` and `numeric` *comparisons* is currently
disabled because the available row-level Arrow filter would diverge from
PostgreSQL semantics (IEEE 754 `NaN` ordering and `decimal` scale downcast,
respectively); `IS NULL` / `IS NOT NULL` on those columns is unaffected. The
`pg_lakebase.customscan_mode` GUC (`off` / `auto` / `force`) controls whether
the framework emits CustomScan paths, and
`pg_iceberg_am.customscan_min_scan_fraction` floors the estimated scanned
fraction so an implausibly small selectivity cannot make the scan look almost
free to the planner.

## Getting Started

### Prerequisites

- Rust 1.90.0 or later
- PostgreSQL 16 or 17
- `cargo-pgrx` installed (`cargo install --locked cargo-pgrx`)

### Building

1. Initialize `pgrx` (if not done already):
   ```bash
   cargo pgrx init --pg17=/path/to/pg_config
   ```

2. Build and install the extension into the target PostgreSQL instance:
   ```bash
   cargo pgrx install --pg-config /path/to/pg_config --release
   ```

### Required server configuration

`pg-iceberg-am` registers a custom WAL resource manager and a static
`pg-lakebase-storage` background worker during `_PG_init`. Both must be loaded
at postmaster start, so the extension **must be listed in
`shared_preload_libraries`** in `postgresql.conf`:

```
shared_preload_libraries = 'pg_iceberg_am'
```

`LOAD 'pg_iceberg_am'` and bare `CREATE EXTENSION pg_iceberg_am` after server
start are not sufficient: the storage background worker is a static bgworker
and the WAL resource manager registration only takes effect at postmaster
startup.

After updating `postgresql.conf`, restart PostgreSQL and then run
`CREATE EXTENSION pg_iceberg_am;` once per database that needs the access
method.

> Note: `cargo pgrx run pg17` is convenient for ad-hoc development of pgrx
> functions, but it does not configure `shared_preload_libraries`, so the
> background worker and WAL resource manager will not be active under that
> command. Use `cargo pgrx install` plus an explicit
> `shared_preload_libraries` entry for any realistic test of `pg-iceberg-am`.

## Testing

See [docs/testing.md](docs/testing.md) for the pgrx testing model used by
this crate, including when to use ordinary `#[test]` versus
`#[pgrx::pg_test]`.

## Usage

Once the extension is installed, you can create a table using the Iceberg access method:

```sql
CREATE EXTENSION pg_iceberg_am;

-- Create a table using the 'iceberg' access method
CREATE TABLE my_iceberg_table (
    id int,
    data text,
    ts timestamp
) USING iceberg;

-- Insert data
INSERT INTO my_iceberg_table VALUES (1, 'hello', now());

-- Query data
SELECT * FROM my_iceberg_table;
```

Predicate pushdown happens automatically when the planner picks the Iceberg
CustomScan. Use `EXPLAIN` to confirm a `Custom Scan` node and to see which
predicates were pushed versus left as residual filters:

```sql
EXPLAIN (VERBOSE)
SELECT * FROM my_iceberg_table WHERE id = 42;
```

To force or disable the behavior during testing, set
`pg_lakebase.customscan_mode` (`off` / `auto` / `force`) at the session level.

### Distributed tablespace limitations

Distributed tablespaces use the tablespace name as the object-storage
`store_id`. This keeps `pg-lakebase-storage` cache and staging directories
human-readable, but it also makes the tablespace name part of the storage
identity. Do not rename distributed tablespaces after creation.

Object-store credentials supplied through tablespace options are persisted in
`pg_tablespace.spcoptions`. The Rust config types redact secrets from logs and
debug output, but PostgreSQL still stores the catalog option value as plain
text. Treat this as a current limitation until secret references or external
credential providers are supported.

### Type mapping limitations

PostgreSQL types are mapped to Iceberg types when a column is added (CREATE
TABLE / ALTER TABLE ... ADD COLUMN). The mapping is not always lossless;
the cases worth knowing about:

- **`numeric` without `(p, s)`**: Iceberg `decimal` requires a fixed
  precision and scale, but PostgreSQL `numeric` without a modifier is
  arbitrary-precision. pg-iceberg-am falls back to `decimal(38, 18)` and
  emits a `WARNING` per column at CREATE TABLE time so the choice is
  visible. Values whose unscaled integer part exceeds 20 digits, or whose
  fractional part exceeds 18 digits after rounding, will fail at INSERT
  time. Declare `numeric(p, s)` explicitly to avoid this.
- **`numeric(p, -k)` (negative scale)**: maps to `decimal(p + |k|, 0)`.
  The mapping is round-trip-safe — PostgreSQL only stores values that are
  multiples of `10^k`, which the widened decimal can represent exactly.
  CREATE TABLE is rejected when `p + |k| > 38`.
- **`numeric(p, s)` with `p > 38`**: rejected at CREATE TABLE. Iceberg's
  `decimal` is capped at 38-digit precision.
- **`json`** is stored as Iceberg `string`. The encoding is the textual
  output of PostgreSQL's `json_out`, which is portable JSON.
- **`jsonb`** is stored as Iceberg `binary` using PostgreSQL's internal
  `jsonb` varlena format. **This is a pg-iceberg-am private codec**, not a
  portable Iceberg JSON encoding — other Iceberg readers cannot decode
  these bytes. Will be revisited when Iceberg variant types land.
- **Unsupported types**: types without an explicit PostgreSQL-to-Iceberg
  mapping are rejected at CREATE TABLE. Examples currently include `pg_lsn`,
  `tsvector`, range types, geometric types, custom enum types, and composite
  types.

The full mapping is defined in
[`pg-iceberg-am/src/catalog/schema_mapping.rs`](pg-iceberg-am/src/catalog/schema_mapping.rs).

Refer to the project documentation for advanced configuration options and storage backend setup.
