//! Object-storage backends that populate the cache with HEAD metadata and ranged GET payloads.
//!
//! The module is organised around a single trait, [`ObjectBackend`], with three implementations
//! layered behind it:
//!
//! * [`MemoryObjectBackend`] — in-memory backend for tests and local embedding.
//! * [`ObjectStoreBackend`] — adapter over an [`object_store::ObjectStore`] client, optionally
//!   pinned to a single bucket.
//! * [`ConfiguredObjectBackend`] — lazily instantiates per-bucket [`ObjectStoreBackend`] clients
//!   from a [`StoreConfig`].
//!
//! Named backends are looked up through [`StoreRegistry`], which is the binding between
//! [`crate::object::StoreId`] and the backend that services its reads.
//!
//! Implementations must **not** cache payloads themselves — that belongs to
//! [`crate::cache::CacheManager`] plus [`crate::cache::index::CacheIndex`].

use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::error::StorageResult;
use crate::object::{ListEntry, ObjectInfo, ObjectLocation};

mod config;
mod memory;
mod object_store;
mod registry;
mod secret;

pub use config::{
    AzureStoreConfig, ConfiguredObjectBackend, GcsStoreConfig,
    S3CompatibleStoreConfig, S3StoreConfig, StoreConfig,
};
pub use memory::MemoryObjectBackend;
pub use object_store::ObjectStoreBackend;
pub use registry::{RegisteredStore, StoreRegistry};
pub use secret::SecretString;

/// Minimal object-storage abstraction backing reads used to populate cache (HEAD metadata +
/// ranged GET bodies).
///
/// Implementations map [`ObjectLocation`] to backend-specific paths or buckets; they must **not**
/// implement caching — that belongs to [`crate::cache::CacheManager`] plus
/// [`crate::cache::index::CacheIndex`].
#[async_trait]
pub trait ObjectBackend: Send + Sync {
    async fn head(&self, key: &ObjectLocation) -> StorageResult<ObjectInfo>;
    async fn get_range(
        &self,
        key: &ObjectLocation,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes>;

    /// Uploads the contents of a local `path` to `key` and returns the resulting backend identity.
    ///
    /// `len` is the authoritative number of bytes the caller promises to upload; implementations
    /// may use it to size multipart parts, pre-allocate buffers, or set HTTP content-length and
    /// must stream exactly `len` bytes from the front of the file.
    ///
    /// Used today only by the staging commit path in [`crate::staging::StagingArea::commit`],
    /// which always writes the full file; other callers that need partial or in-memory uploads
    /// should grow a new method rather than reusing this one.
    async fn put_from_file(
        &self,
        key: &ObjectLocation,
        path: &Path,
        len: u64,
    ) -> StorageResult<ObjectInfo>;

    /// Lists objects under `(store_id, bucket)` whose key starts with `prefix` (or the whole
    /// bucket when `prefix` is `None`). Listing is recursive: an object at `foo/bar/baz`
    /// matches `prefix = Some("foo/")`.
    ///
    /// The returned stream surfaces backend pagination as a single logical sequence; callers
    /// see one entry per object and do not deal with page tokens. Order is **not** guaranteed.
    ///
    /// `bucket` is taken as a `&str` instead of an [`ObjectLocation`] because list is the one
    /// backend operation that has no single key — it spans the whole `(store_id, bucket)`. The
    /// store id is forwarded so multi-store backends (e.g. routing wrappers) can dispatch.
    fn list(
        &self,
        store_id: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> BoxStream<'static, StorageResult<ListEntry>>;

    /// Deletes a single object. Idempotent: deleting a missing object is `Ok(())`.
    ///
    /// `existed`-style semantics are intentionally **not** exposed because backends disagree:
    /// AWS S3 and `object_store::memory::InMemory` return success on delete-of-missing, while
    /// LocalFS / GCP / Azure return `NotFound`. Returning a synthetic `existed: bool` would be
    /// silently wrong on half the backends. Callers that need to know whether a key existed
    /// should `head` first.
    async fn delete(&self, key: &ObjectLocation) -> StorageResult<()>;

    /// Deletes a stream of bucket-relative object keys, returning a stream of per-key outcomes.
    ///
    /// Implementations should prefer backend-native bulk delete where available (S3
    /// `DeleteObjects` 1000/batch, Azure `Blob Batch` 256/batch) by forwarding to
    /// [`object_store::ObjectStore::delete_stream`]. Per-key `NotFound` is suppressed (mapped to
    /// success) so the stream is composable with a `list()` source even if the listing races
    /// against external deletes.
    ///
    /// The output stream yields the **deleted key** for each successful removal (and a
    /// [`crate::error::StorageError`] for any per-key failure). Order matches the input stream
    /// only when the underlying backend preserves it.
    ///
    /// Inputs and outputs are `'static` to match `object_store`'s contract; callers should
    /// produce the input stream by `.boxed()`-ing a list result or an owned iterator.
    fn delete_stream(
        &self,
        store_id: &str,
        bucket: &str,
        keys: BoxStream<'static, StorageResult<String>>,
    ) -> BoxStream<'static, StorageResult<String>>;
}
