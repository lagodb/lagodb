# pg-lakehouse

[![Build Status](https://github.com/robertmu/pg-lakehouse/workflows/CI/badge.svg)](https://github.com/robertmu/pg-lakehouse/actions)
[![Rust](https://img.shields.io/badge/rust-1.90.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/pg-lakehouse-core.svg)](https://crates.io/crates/pg-lakehouse-core)

**The Unified Data Lakehouse Extension Suite for PostgreSQL.**

`pg-lakehouse` is an ambitious project aimed at making PostgreSQL a first-class citizen in the modern Data Lakehouse ecosystem. By implementing high-performance **Table Access Methods (TAM)** and **Foreign Data Wrappers (FDW)** in Rust, it allows PostgreSQL to query and manage open table formats with native-like performance and semantics.

## The Vision

The goal of `pg-lakehouse` is to be the **one-stop Data Lakehouse solution** for PostgreSQL, enabling it to seamlessly interact with the broader big data ecosystem. Our roadmap includes:

- **Universal Table Format Support**: In addition to Apache Iceberg, we plan to provide native support for **Apache Hudi** and **Delta Lake**.
- **Hadoop Ecosystem Integration**: Support for accessing and managing data within the Hadoop ecosystem, including HDFS and various Hadoop-based data warehouses.
- **Cloud-Native Storage**: Transparent access to data stored in S3, GCS, Azure Blob Storage, and other object stores.
- **Native-Like Experience**: Providing a "PostgreSQL-native" experience for big data, supporting standard SQL (DML/DDL) and maintaining transactional integrity.

## Project Structure

The project consists of the following components:

- **[pg-lakehouse-core](./pg-lakehouse-core)**: Core framework library providing abstractions for TAM and FDW development.
- **[pg-lakehouse-macros](./pg-lakehouse-macros)**: Procedural macros to reduce boilerplate when implementing access methods.
- **[pg-am-iceberg](./pg-am-iceberg)**: A reference implementation of an Apache Iceberg Table Access Method.
- **[pg-lakehouse-storage](./pg-lakehouse-storage)**: Centralized storage caching server and client, featuring a high-performance single-file image cache synchronized via PostgreSQL latches and shared memory.
- **iceberg-lite**: A synchronous, PostgreSQL-friendly fork of [iceberg-rust](https://github.com/apache/iceberg-rust).

## Key Features

- **Unified Development Model**: A consistent, trait-based API for implementing both native Table Access Methods and Foreign Data Wrappers.
- **Deep PostgreSQL Integration**: Direct hooks into PostgreSQL's scan, DML, and DDL paths for maximum performance.
- **Crash Recovery**: Support for custom WAL resource managers to ensure metadata consistency across PostgreSQL restarts.
- **Safe Abstractions**: High-level Rust handles and types that wrap complex PostgreSQL C structures, preventing memory safety issues.
- **Reference Implementation**: Includes `pg-am-iceberg` as a production-grade example of a Table Access Method.

## Getting Started

### Prerequisites

- Rust 1.90.0 or later
- PostgreSQL 16 or 17
- `cargo-pgrx` (`cargo install --locked cargo-pgrx`)

### Building

1. Initialize `pgrx`:
   ```bash
   cargo pgrx init --pg17=/path/to/pg_config
   ```

2. Compile all workspace members:
   ```bash
   cargo build
   ```

## Documentation

For detailed information on each component, please refer to their respective README files:
- [Core Framework Guide](./pg-lakehouse-core/README.md)
- [Iceberg Access Method Guide](./pg-am-iceberg/README.md)
- [Storage Caching Guide](./pg-lakehouse-storage/README.md)

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
