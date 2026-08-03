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
//! Each connection is attached to one configured backend before object requests are accepted.
//!
//! Implementations must **not** cache payloads themselves — that belongs to
//! [`crate::cache::CacheManager`] plus [`crate::cache::index::CacheIndex`].

use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::error::{StorageError, StorageResult};
use crate::object::{ListEntry, ObjectInfo, ObjectPath};

mod config;
mod configured;
mod identity;
mod managed;
mod memory;
mod object_store;
mod pool;
mod probe;
mod secret;

pub use config::{
    AzureStoreConfig, GcsStoreConfig, S3CompatibleStoreConfig, S3StoreConfig,
    StoreConfig,
};
pub use configured::ConfiguredObjectBackend;
pub use identity::BackendDataIdentity;
pub use managed::{ManagedStoreRegistry, ManagedStoreSlot};
#[cfg(test)]
pub(crate) type StoreRegistry = ManagedStoreRegistry;
pub use memory::MemoryObjectBackend;
pub use object_store::ObjectStoreBackend;
pub use pool::BackendPool;
pub use probe::StorageProbeResult;
pub use secret::SecretString;

/// Minimal object-storage abstraction backing reads used to populate cache (HEAD metadata +
/// ranged GET bodies).
///
/// Implementations map [`ObjectPath`] to backend-specific paths or buckets; they must **not**
/// implement caching — that belongs to [`crate::cache::CacheManager`] plus
/// [`crate::cache::index::CacheIndex`].
#[async_trait]
pub trait ObjectBackend: Send + Sync {
    async fn head(&self, key: &ObjectPath) -> StorageResult<ObjectInfo>;
    async fn get_range(
        &self,
        key: &ObjectPath,
        range: Range<u64>,
    ) -> StorageResult<bytes::Bytes>;

    /// Uploads the contents of a local `path` to `key` and returns the resulting backend identity.
    ///
    /// `len` is the authoritative number of bytes the caller promises to upload; implementations
    /// may use it to size multipart parts, pre-allocate buffers, or set HTTP content-length and
    /// must stream exactly `len` bytes from the front of the file.
    ///
    /// Used today only by the service's staging upload path, which always writes the full file;
    /// other callers that need partial or in-memory uploads should grow a new method rather than
    /// reusing this one.
    async fn put_from_file(
        &self,
        key: &ObjectPath,
        path: &Path,
        len: u64,
    ) -> StorageResult<ObjectInfo>;

    /// Creates a small in-memory object without overwriting an existing key.
    ///
    /// This is the write primitive used by the explicit connectivity probe. Create-only
    /// semantics ensure that a probe-key collision cannot overwrite user data.
    async fn put_if_absent(
        &self,
        _key: &ObjectPath,
        _data: bytes::Bytes,
    ) -> StorageResult<ObjectInfo> {
        Err(StorageError::unsupported(
            "create-only in-memory upload is not supported by this backend",
        ))
    }

    /// Exercises the attached backend without involving the cache or staging
    /// layers.  The probe is composed from the primitive backend operations so
    /// every implementation uses the same connectivity and credential path.
    async fn probe(
        &self,
        bucket: &str,
        root_prefix: &str,
    ) -> StorageResult<StorageProbeResult> {
        Ok(probe::BackendProbe::new(self, bucket, root_prefix)?
            .run()
            .await)
    }

    /// Lists objects under `bucket` whose key starts with `prefix` (or the whole
    /// bucket when `prefix` is `None`). Listing is recursive: an object at `foo/bar/baz`
    /// matches `prefix = Some("foo/")`.
    ///
    /// The returned stream surfaces backend pagination as a single logical sequence; callers
    /// see one entry per object and do not deal with page tokens. Order is **not** guaranteed.
    ///
    /// `bucket` is taken as a `&str` instead of an [`ObjectPath`] because list is the one
    /// backend operation that has no single key.
    fn list(
        &self,
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
    async fn delete(&self, key: &ObjectPath) -> StorageResult<()>;

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
        bucket: &str,
        keys: BoxStream<'static, StorageResult<String>>,
    ) -> BoxStream<'static, StorageResult<String>>;
}
