Backend Subsystem
=================

The backend subsystem provides the object-storage abstraction layer. It
defines how the server talks to remote (or in-memory) object stores without
implementing caching — that responsibility belongs to the cache subsystem.


1  ObjectBackend Trait
=====================

All storage operations go through a single async trait:

```
  ObjectBackend
    head(key)              -> ObjectInfo (size, etag)
    get_range(key, range)  -> bytes::Bytes
    put_from_file(key, path, len) -> ObjectInfo
    list(store_id, bucket, prefix) -> Stream<ListEntry>
    delete(key)            -> ()
    delete_stream(store_id, bucket, keys) -> Stream<String>
```

Implementations must be `Send + Sync`. The trait documents several
semantic contracts:

- **No payload caching.** Implementations must not cache object data.
  Caching is the exclusive responsibility of `CacheManager`.
- **Idempotent delete.** Deleting an object that does not exist returns
  success. Backends disagree on whether "existed" is observable, so the
  trait does not expose that signal.
- **delete_stream tolerates NotFound.** Individual key deletions that
  return NotFound are mapped to success, making bulk delete composable
  with racing list operations.
- **list takes strings, not ObjectLocation.** Listing spans a namespace
  (`store_id` + `bucket` + optional prefix), so a full `ObjectLocation`
  is not required.


2  Implementation Layers
========================

```
  StoreRegistry
       |
       v
  RegisteredStore  (store_id + generation + Arc<dyn ObjectBackend>)
       |
       +--- MemoryObjectBackend         (in-memory HashMap, tests)
       |
       +--- ObjectStoreBackend          (wraps Arc<dyn ObjectStore>)
       |
       +--- ConfiguredObjectBackend     (lazy per-bucket ObjectStore
                                         built from StoreConfig)
```

**MemoryObjectBackend.** Thread-safe `HashMap<ObjectLocation, Vec<u8>>` (stored as owned bytes internally).
Provides head, ranged get, put, list, delete, and delete_stream backed
entirely by in-memory state. Used for tests and local embedding.

**ObjectStoreBackend.** Adapter from the `object_store` crate to
`ObjectBackend`. Supports optional single-bucket pinning: when pinned,
keyed operations on the wrong bucket return NotFound, and list on the
wrong bucket returns an empty stream.

**ConfiguredObjectBackend.** Takes a `StoreConfig` (S3, S3-compatible,
GCS, or Azure) and lazily builds one `ObjectStore` client per bucket on
first access. The per-bucket client is cached under a double-checked
lock.


3  Store Registry
=================

`StoreRegistry` is a concurrent map from `StoreId` to
`Arc<RegisteredStore>`. It provides several registration methods:

- `register_backend` — concrete `ObjectBackend` implementation.
- `register_shared_backend` — pre-built `Arc<dyn ObjectBackend>`.
- `register_config` — validates a `StoreConfig`, then wraps it in
  `ConfiguredObjectBackend`.
- `register_object_store` — wraps a raw `ObjectStore`.
- `register_object_store_bucket` — wraps a pinned single-bucket store.

Resolution: `resolve(&StoreId)` returns `Arc<RegisteredStore>` or
NotFound. Unregistering a store removes it from the map but does not
invalidate already-resolved `Arc<RegisteredStore>` references — callers
that hold a resolved store can continue using it.

Each `RegisteredStore` carries a monotonic generation counter so callers
can detect when a store has been replaced.


4  Store Configuration
======================

`StoreConfig` is a tagged enum with one variant per cloud provider:

```
  StoreConfig
    S3          { region, endpoint, access_key, secret_key }
    S3Compatible { region, endpoint, access_key, secret_key }
    Gcs         { service_account_key | application_default }
    Azure       { account, access_key | client_secret triple }
```

`StoreConfig::validate()` runs before registration and rejects:

- Empty endpoints (S3-compatible).
- Ambiguous credential combinations (GCS with both key types).
- Partial client-secret triples (Azure).
- Empty secret strings.

Credentials are stored as `SecretString`, which redacts `Debug` output
and only exposes the underlying value through `expose_secret()`.


5  Upload Path
==============

`put_from_file` on `ObjectStoreBackend` uses sequential multipart upload
with 8 MiB parts. If an I/O or network error occurs mid-upload, the
multipart upload is aborted. Parallel part uploads are noted as a future
optimization.

`ConfiguredObjectBackend` resolves the per-bucket store first, then
delegates to `ObjectStoreBackend::for_bucket`.


6  Error Handling
=================

All backend APIs return `StorageResult<T>`. Error mappings:

- `StorageError::configuration` — config validation and client build
  failures.
- `StorageError::backend_source` — wraps `object_store::Error` with
  context (head, get, list, delete, multipart).
- `StorageError::not_found` — missing objects, wrong pinned bucket,
  unknown store id.
- `StorageError::io` — local file I/O during staging uploads.

Poisoned mutex/RwLock is treated as fatal (`expect` with an explicit
message) because it indicates corrupted connection state.
