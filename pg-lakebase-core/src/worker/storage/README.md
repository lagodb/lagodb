# pg-lakebase storage background worker

This module owns the PostgreSQL-facing lifecycle for running
`pg-lakebase-storage` as a PostgreSQL background worker.

The storage server itself stays a pure Rust/Tokio library. It must not depend on
`pgrx` and must not call PostgreSQL FFI. PostgreSQL integration belongs here, in
`pg-lakebase-core`, so multiple AM extensions can eventually reuse the same
capability.

## Goals

- Register a static PostgreSQL background worker from an AM extension's
  `_PG_init`.
- Start `pg-lakebase-storage` inside a multi-thread Tokio runtime after the
  postmaster forks the worker process.
- Keep PostgreSQL FFI on the bgworker main thread.
- Snapshot GUC values once into plain Rust config before handing work to Tokio.
- Bridge `tracing` logs from Tokio threads into PostgreSQL's standard log.
- Preserve PostgreSQL log severity, including ERROR severity, without using
  `pgrx::error!` or PostgreSQL `errfinish()`.
- Handle SIGHUP and SIGTERM through the normal bgworker latch loop.
- Avoid automatic bgworker restart in the same postmaster lifetime.
- Keep `pg_lakebase_storage::StoreRegistry` in sync with the distributed
  tablespaces declared in `pg_tablespace`, using a controller-style
  reconciler driven by syscache invalidation plus periodic full resync.

## Non-goals for the first version

- Backend-side `StorageEndpoint` integration.
- Automatic reconnect of existing `StorageClient` handles.
- Dynamic reload of storage worker GUCs.
- Multiple AM extensions registering the same storage worker at the same time.
- Generation-aware handle invalidation (existing `RegisteredStore` handles
  remain usable after their store is unregistered or replaced; new requests
  pick up the new state).
- Per-store cache and staging cleanup on `DROP TABLESPACE`.
- Encrypted catalog storage of object-store credentials.

## Ownership boundary

`pg-lakebase-core/src/worker/storage` contains code that can run in a PostgreSQL
background worker process:

- `mod.rs`: public initialization API and bgworker entry point.
- `gucs.rs`: Postmaster-scope GUC definitions and accessors.
- `config.rs`: GUC snapshot into `StorageWorkerConfig`.
- `supervisor.rs`: bgworker main-thread lifecycle, signals, Tokio runtime, and
  shutdown.
- `logging.rs`: bounded `tracing` to PostgreSQL log bridge.
- `catalog.rs`: PostgreSQL `pg_tablespace` scanner plus the syscache dirty
  flag. The only file in this module that calls PostgreSQL FFI for the store
  reconciler.
- `reconciler.rs`: pure Rust diff/apply driving `StoreRegistry`. Must not
  depend on `pgrx` or `pg_sys`.

`pg-lakebase-storage` contains the storage server implementation. It exposes
`serve_until(CancellationToken)` for cooperative shutdown, but remains independent
from PostgreSQL and `pgrx`.

`pg-iceberg-am` only opts into this capability:

```rust
#[pg_guard]
extern "C-unwind" fn _PG_init() {
    setup_rustls_default_crypto_provider();
    gucs::init();
    pg_lakebase_core::worker::storage::init_for_extension("pg_iceberg_am");
    hooks::init_hooks();
    wal::init_wal_rmgr();
}
```

The `library_name` passed to `init_for_extension` must match the final shared
object loaded by PostgreSQL, without path or extension. For `pg-iceberg-am` that
is `pg_iceberg_am`.

## Registration model

`init_for_extension(library_name)` performs three operations:

1. Register storage-worker GUCs.
2. Return immediately if `pg_lakebase.storage_server.enabled = false`.
3. Register a static bgworker:

```rust
BackgroundWorkerBuilder::new("pg-lakebase-storage")
    .set_type("pg-lakebase-storage")
    .set_library(library_name)
    .set_function("pg_lakebase_storage_bgworker_main")
    .enable_spi_access()
    .set_restart_time(None)
    .load();
```

`enable_spi_access()` sets `BGWORKER_BACKEND_DATABASE_CONNECTION |
BGWORKER_SHMEM_ACCESS` and forces start time to `RecoveryFinished`. The
worker therefore does not start until PostgreSQL leaves recovery; in standby
mode the storage server does not bind its socket.

`set_restart_time(None)` is intentional. `StorageServerBuilder::bind()` performs
cache recovery and wipes the staging area. That is correct after PostgreSQL
restart or postmaster reset, because database transactions are gone. It is not
safe to repeat that initialization while unrelated backend processes from the
same postmaster lifetime are still alive.

If the worker crashes, storage becomes unavailable until PostgreSQL restarts.
Current queries should error. New queries should also error if they cannot
connect to the storage socket. The first version does not hide this failure with
automatic reconnect or worker restart.

## Worker lifecycle

The exported entry point is:

```rust
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_lakebase_storage_bgworker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );

    supervisor::StorageWorkerSupervisor::from_gucs().run();
}
```

The supervisor runs on the bgworker main thread:

1. Snapshot GUCs into `StorageWorkerConfig`.
2. Set this worker process's `log_min_messages` to `INFO`.
3. Install the `tracing` subscriber backed by the PostgreSQL log bridge.
4. Build a multi-thread Tokio runtime.
5. Connect the worker to PostgreSQL with
   `BackgroundWorker::connect_worker_to_spi(None, None)`. We do not bind a
   user database; `pg_tablespace` is a shared catalog and reachable without
   one.
6. Construct a shared `StoreRegistry`, install the `pg_tablespace` syscache
   invalidation callback, and run the **initial reconcile** inside
   `BackgroundWorker::transaction`. A failure here logs at `PGERROR`
   severity and calls `proc_exit(1)`; we do not bind the storage socket
   when the desired tablespace state cannot be loaded.
7. Spawn the async storage server task with the same `StoreRegistry`:

```rust
let server = pg_lakebase_storage::StorageServerBuilder::new(
    &config.socket_path,
    &config.cache_dir,
)
.with_server_config(config.server_config)
.with_service_config(config.service_config)
.with_store_registry(server_registry)
.with_tracing_request_observer()
.bind()
.await?;

server.serve_until(shutdown).await
```

8. Enter the main loop:

```text
drain PG log bridge
if server task finished:
    log result
    proc_exit(1)
AcceptInvalidationMessages
if take_dirty() or periodic timer due:
    BackgroundWorker::transaction { reconciler.apply() }
    refresh next periodic deadline
wait_latch(<= 100ms, clamped to next periodic deadline)
if SIGHUP:
    log "restart required"
if SIGTERM:
    cancel shutdown token
    wait for server task until shutdown timeout
    shutdown runtime
```

The loop drains logs before waiting so pending storage logs are not delayed
behind latch sleeps. The loop uses `wait_latch` for signal handling; after
`wait_latch` returns false, SIGTERM has already been consumed by pgrx, so the
supervisor uses the boolean return value instead of calling
`sigterm_received()`.

## GUC snapshot

All storage-worker GUCs are `GucContext::Postmaster`. Changes require a
PostgreSQL restart. SIGHUP only logs that restart is required.

Current GUCs:

- `pg_lakebase.storage_server.enabled`
- `pg_lakebase.storage_server.socket_path`
- `pg_lakebase.storage_server.cache_dir`
- `pg_lakebase.storage_server.worker_threads`
- `pg_lakebase.storage_server.shutdown_timeout_ms`
- `pg_lakebase.storage_server.log_channel_capacity`
- `pg_lakebase.storage_server.max_connections`
- `pg_lakebase.storage_server.max_read_size`
- `pg_lakebase.storage_server.tablespace_reconcile_interval_ms` &mdash; how
  often the worker rescans `pg_tablespace` as a safety net behind syscache
  invalidation. Default `30000`. `0` disables the periodic resync; the
  reconciler then runs only on syscache wake-up.

`StorageWorkerConfig::from_gucs()` must run on the bgworker main thread. It
reads PostgreSQL state such as `DataDir`, resolves default paths, and returns a
plain Rust struct that can be moved into Tokio tasks.

Default paths are derived from `DataDir`:

- socket: `$DataDir/pg_lakebase/storage.sock`
- cache: `$DataDir/pg_lakebase/storage-cache`

## Distributed tablespace reconciliation

The storage worker keeps `pg_lakebase_storage::StoreRegistry` in sync with
PostgreSQL `pg_tablespace.spcoptions` using a controller pattern.

- **Desired state:** distributed tablespaces in `pg_tablespace`. Native
  PostgreSQL tablespaces (no Lakebase storage options) are ignored.
- **Actual state:** entries currently registered in the storage server's
  `StoreRegistry`.
- **Driver:** the bgworker main thread loops on `wait_latch`, takes the
  `pg_tablespace` syscache dirty flag, optionally lets the periodic timer
  expire, runs `BackgroundWorker::transaction { reconciler.apply() }`, and
  applies the diff to the registry.

`reconciler.rs` is pure Rust and unit-testable without a PostgreSQL
backend; it must not depend on `pgrx` or `pg_sys`. `catalog.rs` is the only
file in this module that performs PostgreSQL FFI for the reconciler.

The syscache callback is intentionally minimal: it only sets a
`thread_local!` `Cell<bool>`. It does not allocate, does not parse options,
and does not touch the registry. Catalog reads happen on the next loop
iteration.

DDL behavior:

- `CREATE TABLESPACE ... WITH (...)` &mdash; validated by the existing
  `pg-iceberg-am` utility hook; `spcoptions` are persisted, the syscache
  callback fires, and the next reconcile registers the new store.
- `DROP TABLESPACE` &mdash; not intercepted; the next reconcile observes
  the disappearance and unregisters the store.
- `ALTER TABLESPACE name RENAME TO new_name` &mdash; rejected for
  distributed tablespaces by `pg-iceberg-am`'s rename guard hook. The
  store id is the tablespace name and renames would orphan
  cache/staging directories.
- `ALTER TABLESPACE name SET/RESET (...)` &mdash; rejected for distributed
  tablespaces. Distributed tablespaces are immutable in this release;
  changes go through `DROP` + `CREATE`.

Failure modes:

- **Initial reconcile fails on startup:** `proc_exit(1)`. The storage
  socket is not bound; new connections fail with the standard
  "connection refused" path.
- **Runtime reconcile fails:** logged at `WARNING` and retried on the next
  syscache wake-up or periodic timer. The previously applied registry
  state is preserved; transient catalog errors do not turn into a partial
  unregister.

## Logging bridge

Tokio/storage threads must not call PostgreSQL FFI. They only format tracing
events and send `LogEvent` values into a bounded `sync_channel`.

The bgworker main thread owns `PgLogBridge` and emits drained events to the
PostgreSQL log. The channel is bounded; when it is full, log events are dropped
and an atomic counter is incremented. The next drain reports the number of
dropped messages as a PostgreSQL WARNING.

Severity mapping:

| tracing level | PostgreSQL severity |
| --- | --- |
| ERROR | `PGERROR` |
| WARN | `WARNING` |
| INFO | `INFO` |
| DEBUG | `DEBUG1` |
| TRACE | `DEBUG5` |

The subscriber installed by `install_tracing_subscriber` currently fixes the
filter at `LevelFilter::INFO`, so DEBUG and TRACE events are dropped before they
reach the bridge. The DEBUG/TRACE rows in the table above describe the mapping
that will apply when a configurable verbosity GUC is added; the rows are kept so
the mapping function stays exhaustive over `tracing::Level`.

The worker process sets `log_min_messages = INFO` with `SetConfigOption`, using
`PGC_S_OVERRIDE`. This affects only the bgworker process and mirrors the Neon
communicator pattern. It makes startup and storage INFO logs visible by default.

### ERROR severity without errfinish

`emit_pg_log` intentionally uses PostgreSQL's low-level error-reporting API:

```text
message_level_is_interesting
errstart_cold
errmsg_internal
EmitErrorReport
FlushErrorState
```

It does not call `errfinish()`.

That distinction is the core safety property. `errfinish(ERROR)` would perform
PostgreSQL ERROR control flow, which can longjmp or exit the worker. Here ERROR
is used as a log severity only. The bridge holds interrupts by incrementing
`InterruptHoldoffCount` while the ErrorData stack is open, equivalent to
PostgreSQL's `HOLD_INTERRUPTS()`.

If a PostgreSQL log destination itself raises a nested ERROR during
`EmitErrorReport`, PostgreSQL may exit the bgworker process. That is acceptable:
the logging subsystem itself is failing.

## Shutdown semantics

`pg-lakebase-storage` exposes:

```rust
pub async fn serve_until(&self, shutdown: CancellationToken) -> StorageResult<()>
```

The accept loop stops when the cancellation token is cancelled. Existing
connections are not explicitly awaited by `serve_until`; they continue until the
peer closes, their own drain timeout fires, or the Tokio runtime is shut down.

On SIGTERM the supervisor:

1. Cancels the token.
2. Treats `pg_lakebase.storage_server.shutdown_timeout_ms` as a single total
   stop budget. The supervisor waits for the server task to finish within that
   deadline, draining logs while it waits.
3. Whatever budget remains is then passed to `runtime.shutdown_timeout(...)` as
   the final hard cap covering already-spawned connection tasks. A small
   minimum is enforced so the runtime always gets a non-zero budget.

## Safety rules

- `pg-lakebase-storage` must not depend on `pgrx`.
- Tokio worker threads must not call PostgreSQL FFI.
- PostgreSQL FFI in this module must stay on the bgworker main thread.
- GUC reads must be converted into plain Rust values before moving data into
  Tokio tasks.
- `pgrx::error!` must not be used in the logging bridge.
- `errfinish()` must not be called by the logging bridge.
- `set_restart_time(None)` must stay unless staging initialization semantics are
  redesigned.

## Known limitations

The first version assumes exactly one AM extension owns the storage worker. If
`pg-iceberg-am` and a future `pg-hudi-am` are both preloaded and both call
`init_for_extension`, they may try to define the same GUCs, register the same
worker, and bind the same socket.

The long-term design should move ownership to a dedicated PostgreSQL extension,
for example `pg-lakebase`, while AM extensions only consume the storage service.
