# pg-lakebase-core

[![Rust](https://img.shields.io/badge/rust-1.90.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

**A high-performance framework for building PostgreSQL storage extensions in Rust.**

`pg-lakebase-core` is the foundational library for the `pg-lakehouse` ecosystem. It provides a set of type-safe abstractions and procedural macros that simplify the development of custom **Table Access Methods (TAM)** and **Foreign Data Wrappers (FDW)** within PostgreSQL.

By leveraging [pgrx](https://github.com/tcdi/pgrx), the framework bridges the gap between PostgreSQL's C-based storage engine internals and Rust's safety and performance characteristics.

The [pg-iceberg-am](../pg-iceberg-am) project is a prime example of an extension built using this framework.

## Core Vision
 
The goal of `pg-lakebase-core` is to provide a unified development framework for building "Lakehouse" architectures on PostgreSQL. We aim to support a wide range of open data standards and storage backends:
 
-**Multi-Format Support**: A single framework for developing access methods for **Apache Iceberg**, **Apache Hudi**, and **Delta Lake**.
-**Ecosystem Integration**: Built-in abstractions for connecting to the **Hadoop ecosystem** (HDFS, Hive Catalog) and cloud object storage.
-**Deep Engine Integration**: High-performance implementations via **Table Access Method (TAM)** and **Foreign Data Wrapper (FDW)** traits.
-**Shared Infrastructure**: Common tools for schema evolution, metadata tracking, and data conversion between PostgreSQL and Arrow/Parquet.

## Key Features

- **Safe TAM Abstractions**: Implements trait-based interfaces for PostgreSQL's `TableAmRoutine`, covering Scans, DML (Insert/Update/Delete), and DDL operations.
- **Custom WAL Resource Manager**: Built-in support for registering custom WAL resource managers, ensuring that custom storage metadata remains consistent across crashes and recovery.
- **Handle System**: Safe wrappers (`RelationHandle`, `ScanKeyHandle`, etc.) for managing raw PostgreSQL pointers safely.
- **Row-Oriented Data API**: Efficient `Row` and `Cell` abstractions for moving data between PostgreSQL's `TupleTableSlot` and custom storage formats.
- **Procedural Macros**: The `#[pg_table_am]` macro (provided via `pg-lakebase-macros`) reduces hundreds of lines of boilerplate into a simple, declarative attribute.

## Quick Start (TAM Example)

```rust
use pg_lakebase_core::prelude::*;

#[pg_table_am(
    version = "0.1.0",
    author = "Robert Mu",
    website = "https://github.com/robertmu/pg-lakehouse"
)]
pub struct MyCustomAm;

impl TableAccessMethod<MyError> for MyCustomAm {
    type ScanState = MyScan;
    type RelationState = MyRelation;
    type IndexState = MyIndex;
    type DdlState = MyDdl;
    type ModifyState = MyModify;
}
```

## Roadmap

- [ ] **Unified FDW Support**: Extension of the trait system to support Foreign Data Wrappers, mirroring the ergonomic TAM interface.
- [ ] **Plug-and-play Catalog API**: Simplified metadata tracking for tables across both TAM and FDW.
- [ ] **Parallel Scan Optimization**: Enhanced support for PostgreSQL's parallel query execution.
- [ ] **Advanced Data Conversion**: Further optimization for Arrow-based data processing within the access method path.

## Requirements

- **Rust**: 1.90.0 or later
- **PostgreSQL**: 16 or 17
- **pgrx**: 0.16.x

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
