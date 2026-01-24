# pg-iceberg-am

[![Rust](https://img.shields.io/badge/rust-1.90.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**Apache Iceberg Table Access Method (TAM) for PostgreSQL**

`pg-iceberg-am` is a PostgreSQL extension implemented in Rust that provides native support for the [Apache Iceberg](https://iceberg.apache.org/) table format. It is built using the [pg-lakebase-core](https://github.com/robertmu/pg-lakehouse) framework and leverages [pgrx](https://github.com/tcdi/pgrx) for deep integration with PostgreSQL's internal engine.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Usage](#usage)

## Overview

`pg-iceberg-am` allows PostgreSQL to treat Apache Iceberg tables as native tables. Unlike Foreign Data Wrappers (FDW), which operate at the query planning level, `pg-iceberg-am` implements the **Table Access Method (TAM)** interface, providing tighter integration with the storage engine, transaction management, and recovery systems.

This implementation allows for:
- Native SQL support (INSERT, SELECT, UPDATE, DELETE)
- Recovery through custom WAL (Write-Ahead Log) resource managers

## Key Features

- **Native TAM Integration**: Implements the `TableAmRoutine` to hook directly into PostgreSQL's scan and modification paths.
- **Iceberg Support**: Uses `iceberg-lite` (a synchronous, PostgreSQL-friendly fork of `iceberg-rust`) to manage Iceberg metadata and data files.
- **Storage Flexibility**: Supports local storage and S3-compatible object storage via the `object-store` crate.
- **Custom WAL RMGR**: Includes a custom WAL resource manager to ensure data consistency and recovery for Iceberg metadata during PostgreSQL crashes.
- **Schema Mapping**: Automatic bidirectional mapping between PostgreSQL types and Iceberg/Arrow types.
- **Parquet Integration**: High-performance data access using the Parquet file format.

## Architecture

`pg-iceberg-am` is split into several logical components:

- **Scan & DML**: Optimized table scanning and data modification logic.
- **Catalog Management**: Handles Iceberg snapshot creation, metadata updates, and expiration.
- **Storage Layer**: Abstracted I/O for reading and writing data/metadata files to various backends.
- **WAL RMGR**: Ensures that Iceberg metadata changes are atomic and recoverable in sync with PostgreSQL transactions.

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

2. Compile and run the extension:
   ```bash
   cargo pgrx run pg17
   ```

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

Refer to the project documentation for advanced configuration options and storage backend setup.
