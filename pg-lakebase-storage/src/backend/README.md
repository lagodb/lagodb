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
    head(ObjectPath)                 -> ObjectInfo (size, etag)
    get_range(ObjectPath, range)     -> bytes::Bytes
    put_from_file(ObjectPath, path, len) -> ObjectInfo
    list(bucket, prefix)             -> Stream<ListEntry>
    delete(ObjectPath)               -> ()
    delete_stream(bucket, keys)      -> Stream<String>
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
- **Backends do not see cache identity.** Keyed operations receive
  `ObjectPath(bucket, key)`. Listing receives a bucket and optional prefix.
  `BackendDataIdentity` belongs to cache/staging addressing and is not passed
  back into an already configured backend.


2  Implementation Layers
========================

```
  connection attach
       |
       +--- ManagedStoreSlot -------- runtime-owned volume reference
       |
       +--- BackendPool ------------ weak interning by StoreConfig
                    |
                    v
             Arc<dyn ObjectBackend>
                    |
                    +--- MemoryObjectBackend
                    +--- ObjectStoreBackend
                    +--- ConfiguredObjectBackend
```

**MemoryObjectBackend.** Thread-safe `HashMap<ObjectPath, Vec<u8>>` (stored as owned bytes internally).
Provides head, ranged get, put, list, delete, and delete_stream backed
entirely by in-memory state. Used for tests and local embedding.

**ObjectStoreBackend.** Adapter from the `object_store` crate to
`ObjectBackend`. Supports optional single-bucket pinning: when pinned,
keyed operations on the wrong bucket return NotFound, and list on the
wrong bucket returns an empty stream.

**ConfiguredObjectBackend.** Takes a `StoreConfig` (S3, S3-compatible,
GCS, or Azure) and lazily builds one `ObjectStore` client set per bucket on
first access. It keeps a default client for reads and administrative
operations and a zero-retry client for caller-controlled staging uploads.
This prevents an Upload protocol request from being replayed invisibly by
the object-store SDK; the ordinary upload result is returned to the database.
The per-bucket clients are cached under a double-checked lock.


3  Context Resolution and Sharing
=================================

There is no wire-visible backend registry and no dynamic registration
lifecycle. One mandatory handshake resolves one backend per connection:

- `AttachManaged(volume_id)` resolves a `ManagedStoreSlot` published by the
  runtime reconciler.
- `AttachConfigured(Arc<StoreConfig>)` validates the supplied config and
  interns a `ConfiguredObjectBackend` through `BackendPool`.

`BackendPool` stores only `Weak<ConfiguredObjectBackend>` references. Equal
live configurations share the same backend and its lazily built per-bucket
clients; when no connection or managed slot owns the backend, the backend is
released naturally. The pool is not a lifecycle registry and does not need
register/unregister commands.

`ManagedStoreRegistry` maps numeric runtime volume IDs to stable
`ManagedStoreSlot`s. A slot contains an immutable `BackendDataIdentity` and a
replaceable backend `Arc`. Credential refresh can replace the backend while
preserving the identity. Changing a slot's provider endpoint or other
physical identity is rejected, because existing cache and staging paths must
not silently acquire a different meaning. Already attached connections keep
their resolved backend; later connections observe the refreshed backend.

`BackendDataIdentity` contains only physical addressing fields. It excludes
credentials by design, so different credentials for the same endpoint share
cache residency. Endpoint validation rejects URL userinfo, query strings, and
fragments to keep credentials and tokens out of persistent keys and logs.


4  Store Configuration
======================

`StoreConfig` is a tagged enum with one variant per cloud provider:

```
  StoreConfig
    S3           { region, endpoint, credentials/default chain, transport }
    S3Compatible { endpoint, region, credentials, transport }
    Gcs          { base_url, one credential source, skip_signature }
    Azure        { account/endpoint, one auth source, transport/emulator }
```

`StoreConfig::validate()` runs before backend materialization and rejects:

- Empty endpoints (S3-compatible).
- Ambiguous credential combinations (GCS with both key types).
- Partial client-secret triples (Azure).
- Empty secret strings.
- Endpoint URLs with unsupported schemes, forbidden HTTP, missing hosts,
  embedded user credentials, query strings, or fragments.

Credentials are stored as `SecretString`, which redacts `Debug` output
and only exposes the underlying value through `expose_secret()`.


5  Upload Path
==============

`put_from_file` on `ObjectStoreBackend` uses sequential multipart upload
with 8 MiB parts. If an I/O or network error occurs mid-upload, the
multipart upload is aborted. Parallel part uploads are noted as a future
optimization.

`ConfiguredObjectBackend` resolves the per-bucket clients first, then
delegates to `ObjectStoreBackend::for_bucket`; only `put_from_file` selects
the zero-retry upload client.


6  Error Handling
=================

All backend APIs return `StorageResult<T>`. Error mappings:

- `StorageError::configuration` — config validation and client build
  failures.
- `StorageError::backend_source` — wraps `object_store::Error` with context
  (head, get, list, delete, multipart setup/parts/completion).
- `StorageError::not_found` — missing objects, wrong pinned bucket, or an
  unknown managed volume during attach.
- `StorageError::io` — local file I/O during staging uploads.

Poisoned mutex/RwLock is treated as fatal (`expect` with an explicit
message) because it indicates corrupted connection state.
