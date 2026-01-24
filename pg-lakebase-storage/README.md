# pg-lakebase-storage

Storage caching server for `pg-lakebase`.

## Overview

`pg-lakebase-storage` provides a unified interface for object storage I/O with an integrated block-level cache. It is designed to be used by the `pg-iceberg-am` extension to improve performance and decouple storage logic from PostgreSQL worker processes.

## Architecture

- **Communication Mechanism**: Uses a high-performance **Shared Memory (shm)** message passing interface. Metadata (file handles, block offsets, cache status) is exchanged via a compact binary protocol, synchronized using PostgreSQL **Latches** for efficient process wake-ups.
- **Direct Client I/O with LWLock**: PostgreSQL Backend processes (clients) perform direct read/write operations on the `shared image file`. Access to specific block slots is coordinated via **LWLocks (Lightweight Locks)** to ensure data consistency across multiple backends.
- **Server-Side Orchestration**: The storage server (implemented as a Background Worker) manages the global cache state using a **Clock Sweep** algorithm (tracking `usage_count` per block). It handles asynchronous remote object storage I/O using `tokio` and `object_store`.
- **Zero-Copy & Alignment**: 
  - Supports **O_DIRECT** to bypass the OS Page Cache. Since the **image file** is stored on local high-speed storage (e.g., NVMe SSD), this prevents "double-buffering" where data resides in both the OS cache and the application-managed cache, saving memory and reducing CPU overhead.
  - I/O operations are **aligned** to block boundaries (1MB/4MB) to maximize throughput.
- **Single Image File Cache**: 
  - All cached blocks reside in a single pre-allocated "image file" mapped or accessed directly by clients.
  - Reduces file system metadata overhead and fragmentation.

## Design Rationales

1. **Performance**: Offloading remote I/O to a dedicated async server prevents PG backends from stalling on high-latency object store requests. Shared memory communication minimizes serialization overhead.
2. **PostgreSQL Native Integration**: Utilizing `LWLock` and `Latch` ensures the storage system behaves predictably within the PostgreSQL resource management framework.
3. **Cache Efficiency**: The `Clock Sweep` algorithm is ideally suited for the large block sizes (1MB-4MB) typical of OLAP workloads, providing LRU-like behavior with lower synchronization overhead.

## Quick Start

### Building
```bash
cargo build -p pg-lakebase-storage
```

### Running the Server
(Documentation for running the server will be added as implementation progresses)

## Integration
The client will be used in `pg-iceberg-am/src/storage/object.rs` to replace direct object storage interaction.
