# pg-lakebase

[![Build Status](https://github.com/robertmu/pg-lakebase/workflows/CI/badge.svg)](https://github.com/robertmu/pg-lakebase/actions)
[![Rust](https://img.shields.io/badge/rust-1.95.0%2B-blue.svg)](https://www.rust-lang.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16%20%7C%2017-blue.svg)](https://www.postgresql.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Crates.io](https://img.shields.io/crates/v/pg-lakebase-core.svg)](https://crates.io/crates/pg-lakebase-core)

**The Unified Data Lakebase Extension Suite for PostgreSQL.**

`pg-lakebase` makes PostgreSQL a first-class citizen in the modern Data Lakebase ecosystem. By implementing high-performance **Table Access Methods (TAM)** and **Foreign Data Wrappers (FDW)** in Rust — backed by a dedicated local caching storage service — it allows PostgreSQL to query and manage open table formats with native-like performance and semantics.

## The Vision

The goal of `pg-lakebase` is to be the **one-stop Data Lakebase solution** for PostgreSQL, enabling it to seamlessly interact with the broader big data ecosystem:

- **Universal Table Format Support**: Apache Iceberg today, with **Apache Hudi** and **Delta Lake** on the roadmap.
- **Cloud-Native Storage**: Transparent access to S3, GCS, Azure Blob Storage, and other object stores through a co-located caching service.
- **Native-Like Experience**: Standard SQL (DML/DDL) with transactional integrity — no external query engines required.

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

## Project Structure

| Crate | Description |
|-------|-------------|
| **[pg-lakebase-core](./pg-lakebase-core)** | Framework library — trait-based abstractions for TAM and FDW development. |
| **[pg-lakebase-macros](./pg-lakebase-macros)** | `#[pg_table_am]` procedural macro to reduce registration boilerplate. |
| **[pg-iceberg-am](./pg-iceberg-am)** | Apache Iceberg Table Access Method — the reference TAM implementation. |
| **[pg-lakebase-storage](./pg-lakebase-storage)** | Local object-storage caching service with a custom binary protocol over UDS. |
| **[iceberg-lite](./iceberg-lite)** | Synchronous, PostgreSQL-friendly fork of [iceberg-rust](https://github.com/apache/iceberg-rust). |

## Key Features

- **Unified Development Model**: A consistent, trait-based API for implementing both native Table Access Methods and Foreign Data Wrappers.
- **Deep PostgreSQL Integration**: Direct hooks into PostgreSQL's scan, DML, and DDL paths for maximum performance.
- **Crash Recovery**: Custom WAL resource managers ensure metadata consistency across PostgreSQL restarts; the cache treats all resident data as derived and recovers safely on startup.
- **High-Performance Caching**: Multi-tier local cache — small objects in an embedded KV store (redb), large objects as files on disk — with LRU eviction, watermark-based cleanup, and FD passing via `SCM_RIGHTS` so clients can `pread` cache files directly, bypassing the wire protocol.
- **Staging Write Path**: Database transactions write locally via filesystem calls; the storage service uploads to the backend on commit — no streaming state held between stage and finalize.
- **Safe Abstractions**: High-level Rust handles and types that wrap complex PostgreSQL C structures, preventing memory safety issues.

## pg-lakebase-storage

The storage service is the I/O backbone of the system. After major refactoring it is now a standalone, layered caching server:

- **Transport** — framed byte I/O and FD side-channel over Unix domain sockets.
- **Protocol** — compact binary wire format (`STG1`, big-endian, length-prefixed frames).
- **Connection** — per-socket pipeline: concurrent request tasks, single writer, backpressure via bounded response queue.
- **Service** — command routing for `open / read / seek / close`, staging (`stage / commit / abort`), and store management (`register / unregister / purge / invalidate`).
- **Backend** — `object_store` trait wrapper (S3, GCS, Azure, in-memory for tests).
- **Cache** — on-disk layout with persistent redb index, small-object KV, large-object chunked fill sessions, and LRU eviction with high/low watermarks.

See [pg-lakebase-storage/README.md](./pg-lakebase-storage/README.md) and [pg-lakebase-storage/doc/design.md](./pg-lakebase-storage/doc/design.md) for the full design rationale and API reference.

## Getting Started

### Prerequisites

- Rust 1.95.0 or later
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

3. Run the Iceberg extension inside PostgreSQL:
   ```bash
   cargo pgrx run pg17
   ```

### Quick SQL Example

```sql
CREATE EXTENSION pg_iceberg_am;

CREATE TABLE events (
    id   int,
    data text,
    ts   timestamp
) USING iceberg;

INSERT INTO events VALUES (1, 'hello', now());
SELECT * FROM events;
```

## Documentation

For detailed information on each component, please refer to their respective documentation:

- [Core Framework Guide](./pg-lakebase-core/README.md)
- [Iceberg Access Method Guide](./pg-iceberg-am/README.md)
- [Storage Service Guide](./pg-lakebase-storage/README.md)
- [Storage Design Rationale](./pg-lakebase-storage/doc/design.md)

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.
