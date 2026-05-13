Session Subsystem
=================

The session module holds per-connection state so that multiple connections
can share one `Arc<StorageService>` without sharing open-file handles.
It lives at the crate root (not under `connection` or `service`) because
both subsystems depend on it.


1  StorageContext
=================

`StorageContext<I: CacheIndex>` bundles everything a connection needs:

```
  client_addr     Arc<str>                 peer identity for logging
  service         Arc<StorageService<I>>   shared across all connections
  handles         Arc<HandleTable>         per-connection handle registry
  request_hooks   RequestHooks             per-connection hook callbacks
```

One `StorageContext` is created per accepted socket. It is cheaply
cloneable (all fields are `Arc` or `Clone`) so spawned request tasks can
hold their own copy with shared underlying state.


2  Handle Table
===============

`HandleTable` is a per-connection numeric handle registry. Each open object
gets a monotonic `u64` handle ID (starting at 1).

```
  HandleTable
    +--- HashMap<u64, Arc<HandleEntry>>
    |
    +--- next_handle_id: AtomicU64
    |
    +--- open_slots: Arc<Semaphore>   (max_open_handles capacity)
```

Each `HandleEntry` stores:

- `OpenFileState` — snapshot of the object's identity, metadata, flags,
  and optional `Arc<Residency>` for cache-backed reads.
- `HandleResources` — owns the semaphore permit; dropping it releases
  the open slot.
- `HandleLifecycle` — `closing: bool` and `active_reads: usize`.
- `Notify` — wakes the close waiter when the last read guard drops.


3  Handle Lifecycle
===================

```
  reserve_open()
    try_acquire semaphore permit → OpenHandleSlot
    (fail → resource_exhausted error)
        |
        v
  open_reserved(ReservedOpen)
    assign handle ID, insert HandleEntry
    return OpenFileState
        |
        v
  begin_read(handle)
    clone Arc<HandleEntry>, check not closing
    active_reads += 1 → ReadHandleGuard
        |
        v                           close(handle)
  ReadHandleGuard::drop()             set closing = true
    active_reads -= 1                 await active_reads == 0
    if closing && 0: notify             (Notify::notified)
                                      remove from map
                                      return ClosedHandle
                                        |
                                        v
                                      Drop ClosedHandle
                                        releases semaphore permit
```


4  READ / CLOSE Synchronization
===============================

The handle table ensures that a CLOSE never removes a handle while a
READ is in progress on it:

1. `begin_read` fails if `closing` is already set, so no new reads can
   start after close begins.
2. `close` sets `closing = true`, then loops on `Notify` until
   `active_reads` drops to zero.
3. `ReadHandleGuard::drop` decrements `active_reads` and wakes the
   close waiter when the count hits zero.

This is an async barrier pattern — `close` does not hold the map mutex
while waiting. It clones the `Arc<HandleEntry>` and releases the map
lock so other handles remain accessible.


5  Resource Cleanup
===================

When a handle is closed, dropping `ClosedHandle` releases the semaphore
permit (freeing an open slot) and drops the `OpenFileState`. When the
last clone of `Arc<Residency>` on `OpenFileState` is dropped, cache
leases and large-fill sessions clean up automatically without an explicit
finalize step.

`close_all` snapshots all handle IDs under the lock and closes each one
sequentially. This is used during connection shutdown.


6  Design Decisions
===================

- **`std::sync::Mutex`** for the handle map and per-entry lifecycle.
  Critical sections are short and synchronous (no async work under the
  lock). Poisoned mutex is treated as fatal — the connection state is
  no longer trustworthy.

- **`tokio::sync::Semaphore`** for open-handle capacity.
  `try_acquire_owned` fails fast with `resource_exhausted` instead of
  blocking, so a slow client cannot stall OPEN for other requests on
  the same connection.

- **Double-Arc pattern.** The map holds `Arc<HandleEntry>` and guards
  hold clones. This lets read guards and close operations proceed after
  releasing the map lock, avoiding lock contention between independent
  handles.

- **Default limit.** 1024 max open handles per connection
  (`DEFAULT_MAX_OPEN_HANDLES_PER_CONNECTION` in server config).
