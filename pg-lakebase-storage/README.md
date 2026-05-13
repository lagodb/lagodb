pg-lakebase-storage
===================

A local object-storage caching service for database workloads. The server
accepts connections over Unix domain sockets, caches remote object-store
objects on local disk, and serves reads from cache when possible. A thin
staging subsystem lets database transactions write new objects through the
local filesystem and upload them on commit.

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
  +-----v-----+                       +-----v-----+
  |  backend  |                       |   cache   |
  +-----------+                       +-----------+
  | registry  |                       | index     |
  | obj store |                       | chunks    |
  | memory    |                       | store     |
  +-----------+                       | eviction  |
                                      | staging   |
                                      +-----------+
```

The server is a thin network shell around a cache-aware storage service.
One process accepts many Unix socket connections. Each connection decodes
requests concurrently but sends responses through a single writer, keeping
frame and file-descriptor ordering predictable.

Read requests are served from an on-disk cache (backed by redb for metadata
and small objects) or fetched from a remote object store on miss. A separate
staging tree lets clients write files locally and upload them to the backend
in a single commit step.

API
===

**Reads:**
`open(store_id, bucket, key)` / `read(len)` / `seek(offset)` / `close()`

**Staging writes:**
`stage(store_id, bucket, key)` / local writes / `commit(store_id, bucket, key)` / `abort(store_id, bucket, key)`

**Store management:**
`register_store(store_id, config)` / `unregister_store(store_id)` /
`purge_store_cache(store_id)` / `invalidate_object_cache(store_id, bucket, key)`

Object identity is the explicit tuple `(store_id, bucket, key)`.

Quick Start
===========

```rust
use std::sync::Arc;

use pg_lakebase_storage::{
    CacheCleanupConfig, StorageServerBuilder, StorageServerConfig, StorageServiceConfig,
};

async fn run() -> pg_lakebase_storage::StorageResult<()> {
    // 1. Build and bind the server (no backend at this point).
    let server = StorageServerBuilder::new("/tmp/storage.sock", "/tmp/storage-cache")
        .with_server_config(
            StorageServerConfig::default()
                .with_max_connections(1024)
                .with_max_in_flight_requests(256),
        )
        .with_service_config(
            StorageServiceConfig::default()
                .with_max_read_size(1024 * 1024)
                .with_max_cache_bytes(100 * 1024 * 1024 * 1024)  // 100 GiB cache budget
                .with_cache_cleanup_config(
                    CacheCleanupConfig::default()
                        .with_max_cache_bytes(100 * 1024 * 1024 * 1024)
                        .with_thresholds(80, 70),  // start at 80 GiB, evict to 70 GiB
                ),
        )
        .bind()
        .await?;

    // 2. Register backends dynamically after bind.
    //    Clients can also register stores at runtime via the RegisterStore wire message.
    let store = Arc::new(object_store::memory::InMemory::new());
    server.store_registry().register_object_store_bucket("default", store, "my-bucket")?;

    // 3. Serve.
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
- **Periodic trigger** (opt-in via `with_cleanup_interval`): a background
  janitor task runs at a fixed interval. Useful as a safety net for orphan
  file cleanup, but the write-path trigger alone is usually sufficient for
  capacity management.

Both triggers use `try_lock` on an internal gate so concurrent cleanup
traversals from different triggers never pile up.

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
| small_object_limit  | 64 KiB  | Objects at or below this go to KV  |
| chunk_size          | 4 MiB   | Large-object fetch granularity     |
| touch_granularity   | 60 s    | LRU access-time refresh interval   |

**Cache cleanup**

| Knob                    | Default   | Description                           |
|-------------------------|-----------|---------------------------------------|
| max_cache_bytes         | unlimited | Total cache budget                    |
| cleanup_start_percent   | 80%       | Begin eviction above this watermark   |
| cleanup_target_percent  | 70%       | Evict down to this watermark          |
| max_cleanup_batch_items | 1024      | Objects examined per cleanup pass     |
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
- Staging cleanup is startup-wipe only; no online orphan reaper for the
  staging tree.

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
