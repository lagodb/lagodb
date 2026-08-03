Object Model
============

The object module defines the crate-wide domain model for object identity,
metadata, and chunk arithmetic. These types are shared across `backend`,
`cache`, `protocol`, and `service` so that all layers agree on the same
representations.


1  Core Types
=============

```
  BackendDataIdentity  credential-free identity of a physical storage service
  ObjectPath           (bucket, key) — path understood by a configured backend
  ObjectLocation       (backend identity, ObjectPath) — cache/staging identity
  ObjectInfo       { size: u64, etag: Option<String> }
  ListEntry        { key, size, etag, last_modified_ms } — bucket-relative
```

**BackendDataIdentity** is derived from physical provider/addressing fields:
S3 region/endpoint, an S3-compatible endpoint, a GCS base URL, or Azure
account/endpoint/emulator mode. Credentials, tokens, and database logical
identities are deliberately excluded so two credentials addressing the same
physical object share cache residency. It has a length-prefixed stable
serialization used by persistent cache keys and filesystem paths.

**ObjectPath** requires a non-empty bucket and key and forbids `/` in the
bucket name. It is the only object identity passed to an already configured
`ObjectBackend`; the backend does not need to know how cache namespaces are
formed.

**ObjectLocation** adds `BackendDataIdentity` to an `ObjectPath`. It is used
by cache and staging, where two physical services with the same bucket/key
must not collide. `parse_path` reconstructs a location from the encoded
`/backend_identity/bucket/key` representation used by persistent paths.

The `Hash` implementation uses explicit sentinel bytes (`0xfe`, `0xff`)
between backend identity, bucket, and key so adjacent variable-length fields
cannot produce ambiguous hashes.

**ListEntry** keys are bucket-relative (no backend identity or bucket prefix),
matching the semantics of `object_store` list APIs.


2  Chunk Arithmetic
===================

Large objects are fetched and cached in fixed-size chunks. The module
provides three pure functions for chunk math:

```
  chunk_count(size, chunk_size)          number of chunks (0 for empty objects)
  chunk_index(offset, chunk_size)        which chunk contains an offset
  chunk_range(size, chunk_size, index)   byte range [start, end) for a chunk
```

All functions apply `normalize_chunk_size` internally, which maps zero to
one. This makes chunk helpers infallible even when callers pass a zero
chunk size — the result is byte-granular chunks.

Default constants:

- `DEFAULT_CHUNK_SIZE` — 32 MiB.
- `DEFAULT_SMALL_OBJECT_LIMIT` — 4 KiB (threshold for small-KV vs
  large-file cache paths).


3  Path Encoding
================

`path_encoding` provides deterministic, reversible filesystem-safe
encoding for object path segments and the serialized backend identity. It is
used by both `cache/path.rs` and `staging/path.rs` to build on-disk paths from
physical object identities.

Encoding rules:

- Empty segments are encoded as the literal `%empty` so they survive
  filesystem roundtrips (prevents silent collapse of `a//b`).
- ASCII letters, digits, `-`, and `_` pass through unencoded.
- `.` is allowed within segments but the whole-segment values `.` and
  `..` are percent-encoded (`%2e`, `%2e%2e`) to prevent directory
  traversal.
- All other bytes are percent-encoded as `%xx` (lowercase hex).

Decoding (`decode_segment`) is the strict inverse. Malformed percent
escapes or invalid UTF-8 return `None`.

**Portable path validation** (`validate_portable_path`) rejects paths
whose total OS length exceeds 4095 bytes or whose individual components
exceed 255 bytes. This catches overlong keys before they reach the
filesystem.

The module deliberately does not embed cache or staging prefixes or
suffixes — it provides only the segment encoding rules and length
validation. Directory layout details live in `cache/path.rs` and
`staging/path.rs`.


4  Design Decisions
===================

- **Layer-neutral model.** Object identity and chunk math live here so
  HTTP/service code, backends, and cache agree without circular
  dependencies.
- **Backend/cache boundary is explicit.** Backend methods accept
  `ObjectPath`; cache and staging methods accept `ObjectLocation`.
- **Bucket is a single segment.** `ObjectPath` forbids `/` in
  bucket names. Keys can contain `/` (multi-segment logical paths).
- **Defensive chunk math.** Zero chunk size is normalized internally so
  public functions never panic or divide by zero.
- **Encoding is reversible.** Every encoded path can be decoded back to
  the original segments. The `%empty` sentinel ensures empty segments
  are preserved.
- **Length limits are enforced at resolve time.** Path encoding itself
  does not truncate or hash — overlong paths are rejected with a clear
  error.
