pg-lakebase-storage
===================

A local object-storage caching service for database workloads. The server
accepts connections over Unix domain sockets, caches remote object-store
objects on local disk, and serves reads from cache when possible. A thin
staging subsystem lets database transactions write new objects through the
local filesystem and upload them explicitly before the database publishes
metadata that references those objects.

Architecture
============

```
  +-----------+    +-----------+
  |  client   |    |  server   |   process boundary (Unix socket)
  +-----------+    +-----------+
                        |
  +---------------------v-----------------------+
  |               transport                     |   frame I/O, FD channel
  +---------------------------------------------+
  |               protocol                      |   codec, opcodes, limits
  +---------------------------------------------+
  |               connection                    |   reader, writer, dispatch
  +---------------------------------------------+
  |               service                       |   command routing, open/read
  +-----+-----------------------------------+---+
        |                                   |
  +-----v---------------+             +-----v-----+
  | backend/context     |             |   cache   |
  +---------------------+             +-----------+
  | managed volume slot |             | index     |
  | configured pool     |             | chunks    |
  | object_store        |             | store     |
  +---------------------+             | eviction  |
                                      | staging   |
                                      +-----------+
```

The server is a thin network shell around a cache-aware storage service.
One process accepts many Unix socket connections. Each connection decodes
requests concurrently but sends responses through a single writer, keeping
frame and file-descriptor ordering predictable.

Every socket performs one mandatory attach handshake before request
multiplexing starts. A connection attaches either to a runtime-managed volume
or to an inline `StoreConfig`; after attach, both sources become the same
connection-local backend context. Object requests therefore carry only
`(bucket, key)`, never a registry name or database catalog identity.

Read requests are served from an on-disk cache (backed by redb for metadata
and small objects) or fetched from a remote object store on miss. A separate
staging tree lets clients write files locally and upload them to the backend
with a single `upload(bucket, key)` request on a connection attached to the
same physical backend. Upload is not a
database transaction commit; it only copies the closed staging file into the
object backend and returns object facts such as size and etag.

API
===

**Reads:**
attach once / `open(bucket, key)` / `read(len)` / `seek(offset)` / `close()`

**Staging writes:**
`StagingFile::create(backend_identity, bucket, key)` / local writes / close /
`upload(bucket, key)` / caller-side unlink

**Connection setup and cache control:**
`connect_managed(volume_id)` or `connect_configured(config)` /
`invalidate_object_cache(bucket, key)`

Backend operations use `ObjectPath(bucket, key)`. Cache and staging identity is
the credential-free tuple `(BackendDataIdentity, bucket, key)`. Consequently,
two credentials that address the same physical service share cached bytes;
cache hits do not re-run remote authorization or HEAD.

Quick Start
===========

```rust
use std::sync::Arc;

use pg_lakebase_storage::{
    BackendDataIdentity, CacheCleanupConfig, CacheRuntimeConfig,
    ManagedStoreRegistry, MemoryObjectBackend, StorageRuntime,
    StorageRuntimeConfig, StorageServerBuilder, StorageServerConfig,
    StorageServiceConfig,
};

async fn run() -> pg_lakebase_storage::StorageResult<()> {
    // 1. Publish runtime-owned managed volumes before accepting clients.
    let managed = ManagedStoreRegistry::new();
    managed.register_backend(
        42,
        BackendDataIdentity::memory("demo"),
        Arc::new(MemoryObjectBackend::new()),
    )?;

    // 2. Build and bind the server.
    let runtime = StorageRuntime::new(StorageRuntimeConfig {
        cache: CacheRuntimeConfig {
            cleanup: CacheCleanupConfig::default()
                .with_max_cache_bytes(100 * 1024 * 1024 * 1024)    // 100 GiB cache budget
                .with_thresholds(80, 70),  // start at 80 GiB, evict to 70 GiB
            ..CacheRuntimeConfig::default()
        },
    })?;
    let server = StorageServerBuilder::new("/tmp/storage.sock", "/tmp/storage-cache")
        .with_server_config(
            StorageServerConfig::default()
                .with_max_connections(1024)
                .with_max_in_flight_requests(256),
        )
        .with_service_config(
            StorageServiceConfig::default()
                .with_max_read_size(1024 * 1024),
        )
        .with_managed_store_registry(managed)
        .with_runtime(runtime)
        .bind()
        .await?;

    // 3. Clients attach volume 42 with StorageClient::connect_managed.
    //    Foreign/component callers instead use connect_configured.
    server.serve_forever().await
}
```

The example above configures a 100 GiB local cache with write-path triggered
cleanup. Each time a new object is admitted to the cache, the server checks
whether resident bytes have crossed the start watermark (80%, i.e. 80 GiB).
If so, LRU eviction runs immediately and removes least-recently-accessed
objects until usage drops to the target watermark (70%, i.e. 70 GiB).
Objects with active read handles are never evicted.

Cache cleanup supports two online trigger modes (both can be enabled at the
same time):

- **Write-path trigger** (enabled automatically when `max_cache_bytes` is
  set): eviction fires right after a cache admission that crosses the start
  watermark. This is event-driven and only runs when necessary — no wasted
  CPU when the cache is below capacity.
- **Periodic trigger** (opt-in via `cleanup_interval` in `CacheCleanupConfig`):
  a background janitor task runs at a fixed interval. The task only schedules
  periodic runs when **both** `cleanup_interval` and `max_cache_bytes` are set;
  otherwise it parks idle. The `pg-lakebase-core` bgworker GUC layer sets both
  together; standalone callers using `StorageRuntimeConfig::default()` get
  periodic cleanup disabled until they explicitly configure a cache budget.

All cleanup triggers feed one `CleanupScheduler`. Write-path nudges are
allocation-free and coalesced by the actor; periodic, reload, and manual
passes share one async gate so janitor traversals never overlap. A trigger
arriving during manual cleanup waits behind the gate instead of being lost.

Configuration
=============

**Server (per-connection limits)**

| Knob                        | Default | Description                        |
|-----------------------------|---------|------------------------------------|
| max_connections             | 1024    | Server-wide accepted connections   |
| max_in_flight_requests      | 256     | Concurrent requests per connection |
| max_pending_responses       | 64      | Queued response frames per connection |
| max_pending_response_bytes  | 32 MiB  | Byte budget for queued READ payloads |
| response_write_timeout      | 30 s    | Disconnect slow/stuck consumers    |
| connection_drain_timeout    | 2 s     | Grace period on client half-close  |

**Service (read and cache geometry)**

| Knob                | Default  | Description                        |
|---------------------|----------|------------------------------------|
| max_read_size       | 1 MiB   | Server clamps each READ to this    |
| small_object_limit  | 4 KiB   | Objects at or below this go to KV  |
| chunk_size          | 32 MiB  | Large-object fetch granularity     |

**Runtime (hot-reloadable via `StorageRuntime::apply`)**

| Knob                    | Default   | Description                           |
|-------------------------|-----------|---------------------------------------|
| touch_granularity       | 60 s      | LRU access-time refresh interval      |
| max_cache_bytes         | unlimited | Total cache budget                    |
| cleanup_start_percent   | 90%       | Begin eviction above this watermark   |
| cleanup_target_percent  | 80%       | Evict down to this watermark          |
| max_cleanup_batch_items | 256       | Objects examined per cleanup pass     |
| max_cleanup_batch_bytes | 64 MiB    | Bytes evicted per cleanup pass        |
| cleanup_interval        | disabled  | Optional periodic janitor (safety net)|

Testing
=======

```
cargo test --all-targets
cargo test --features strict
```

Current Limitations
===================

- Unix domain sockets only.
- No kernel mount integration.
- Cache cleanup uses logical payload bytes, not filesystem block accounting.
- No minimum-free-disk pressure policy.

Documentation
=============

- `doc/design.md` — design rationale, invariants, and architecture overview.
- `src/cache/README.md` — cache model, consistency invariants, and eviction.
- `src/protocol/README.md` — wire protocol and codec design.
- `src/connection/README.md` — connection pipeline and concurrency model.
- `src/staging/README.md` — staging write lifecycle for database transactions.

License
=======

BSD-3-Clause
