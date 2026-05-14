use std::sync::Arc;

use async_trait::async_trait;

mod memory;
#[cfg(test)]
pub(crate) mod persistent;
#[cfg(not(test))]
mod persistent;

pub use memory::InMemoryCacheIndex;
pub use persistent::RedbCacheIndex;

use crate::cache::LogicalCacheUsage;
use crate::cache::meta::CachedObjectMeta;
use crate::error::StorageResult;
use crate::object::ObjectLocation;

/// One small-object row returned by paged scans of embedded payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SmallCacheEntry {
    pub key: ObjectLocation,
    pub bytes: u64,
}

/// Opaque cursor for stable key-ordered small-object traversal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SmallScanCursor {
    pub key: ObjectLocation,
}

/// Fixed-size page of small-object keys plus optional [`SmallScanCursor`] continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SmallScanPage {
    pub entries: Vec<SmallCacheEntry>,
    pub next_cursor: Option<SmallScanCursor>,
}

/// Cursor combining `(last_access_ns, key)` tie-break so LRU scans remain deterministic when timestamps collide.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LruScanCursor {
    pub last_access_ns: u64,
    pub key: ObjectLocation,
}

/// Oldest-access-first page of resident [`crate::cache::CachedObjectMeta`] rows used by capacity cleanup LRU eviction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LruScanPage {
    pub metas: Vec<CachedObjectMeta>,
    pub next_cursor: Option<LruScanCursor>,
}

/// Cursor for stable object-key order over durable metadata (`recover`, reconciliation tooling).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaScanCursor {
    pub key: ObjectLocation,
}

/// Page of metadata rows plus optional [`MetaScanCursor`] continuation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaScanPage {
    pub metas: Vec<CachedObjectMeta>,
    pub next_cursor: Option<MetaScanCursor>,
}

/// Value returned by [`CacheIndex::open_hit`] when the key is resident.
///
/// `meta` is the post-touch metadata snapshot (if a touch fired) or the original one otherwise.
/// For `SmallKv` residency, `payload` carries the small-object bytes read in the **same**
/// transaction as the metadata — callers never need to re-fetch them. For `CompleteFile`
/// residency, `payload` is `None` because the bytes live on disk, not in the KV.
#[derive(Clone, Debug)]
pub struct OpenHit {
    pub meta: CachedObjectMeta,
    pub payload: Option<Arc<[u8]>>,
}

/// Value returned by [`CacheIndex::admit_small_if_absent`].
///
/// Two variants make the race outcome explicit: `Admitted` means this caller installed the new
/// row; `AlreadyPresent` means a concurrent caller beat us and we are returning their already-
/// committed row (backed by the same immutable `(size, etag)` under the cache invariants).
#[derive(Clone, Debug)]
pub enum AdmitSmallOutcome {
    /// This caller's `(meta, payload)` was committed to the index.
    Admitted {
        meta: CachedObjectMeta,
        payload: Arc<[u8]>,
    },
    /// A concurrent caller already published a small-KV row for this key; we return theirs.
    AlreadyPresent {
        meta: CachedObjectMeta,
        payload: Arc<[u8]>,
    },
}

/// Complete cache-index contract required by `CacheManager`.
///
/// # Transaction-count contract for OPEN
///
/// [`Self::open_hit`] is the only lookup entry point for OPEN. KV-backed implementations must
/// honor these transaction and access-count bounds:
///
/// * **Inside the touch window** (no refresh needed): exactly one read transaction; exactly one
///   `get(Meta)`; plus one `get(Small)` for `SmallKv` residency. No writes.
/// * **Across the touch window** (refresh fires): one read transaction (to observe the current
///   meta and, for `SmallKv`, to read the payload) followed by one write transaction that
///   reuses the observed meta via `touch_observed` and updates `(object_meta, lru_by_access)`.
///   Implementations must not re-read the meta in the write transaction.
///
/// Either way, the caller never needs a second `get(Meta)` Rust-level call. Implementations
/// without a transaction boundary (for example the in-memory index used by unit tests) perform
/// the equivalent work under a single internal lock; the transaction counts above apply only to
/// backends that expose real transactions.
///
/// # Single-transaction contract for small admission
///
/// [`Self::admit_small_if_absent`] is the only way to publish a new `SmallKv` row. It runs a
/// single write transaction that checks for an existing row, returns it on race-loss, or writes
/// `(small_object, object_meta, lru_by_access)` atomically on race-win. Admission is the only
/// place where a race between two OPENs for the same key can happen (under the invariants in
/// `CacheManager`'s docs); collapsing the check-then-act into one transaction is what gives
/// `CacheManager` a race-free admit path with a single KV round-trip.
#[async_trait]
pub trait CacheIndex: Send + Sync {
    /// Returns metadata for `key`, or `None` when the object is unknown to the cache index.
    async fn get_meta(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>>;

    /// Scans metadata records in stable key order.
    ///
    /// Startup recovery uses this so large indexes do not need to materialize every object record
    /// at once.
    async fn scan_meta_page(
        &self,
        cursor: Option<MetaScanCursor>,
        limit: usize,
    ) -> StorageResult<MetaScanPage>;

    /// Writes complete-file metadata for a cache row whose slot `CacheManager` has proven absent.
    ///
    /// Precondition: the caller must perform the publish under the object's cache lock and must prove
    /// through `CacheManager`'s admission/fill state that no current metadata can exist for
    /// `meta.key()`. This method does not perform an insert-if-absent check; the no-rehome rule lives
    /// in `CacheManager`'s state machine.
    async fn put_new_complete(
        &self,
        meta: CachedObjectMeta,
    ) -> StorageResult<CachedObjectMeta>;

    /// Removes metadata and its resident tracking entry, but does not remove any small-object payload.
    async fn delete_meta(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>>;

    /// Reads embedded small-object bytes.
    async fn get_small(&self, key: &ObjectLocation)
    -> StorageResult<Option<Vec<u8>>>;

    async fn stat_small(&self, key: &ObjectLocation) -> StorageResult<Option<u64>> {
        Ok(self.get_small(key).await?.map(|data| data.len() as u64))
    }

    /// Scans physical small-object payloads, including payloads whose metadata may be missing.
    async fn scan_small_entries_page(
        &self,
        cursor: Option<SmallScanCursor>,
        limit: usize,
    ) -> StorageResult<SmallScanPage>;

    /// Removes only an unclaimed small-object payload.
    ///
    /// This does not update metadata or resident-byte tracking. Callers must prove the current
    /// metadata does not claim `key` before calling this. Normal deletion of a cached small object
    /// must use `delete_meta_and_small` instead.
    async fn remove_unclaimed_small_payload(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<()>;

    /// Removes metadata and the small-object payload as one logical operation.
    ///
    /// Persistent implementations must apply this atomically together with any resident-tracking indexes
    /// maintained alongside metadata (for example LRU-by-access rows), so completed operations never leave
    /// [`crate::cache::CacheState::SmallKv`] metadata without a matching small-object payload.
    async fn delete_meta_and_small(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>>;

    /// Replaces in-memory resident-byte tracking from an authoritative startup reconciliation pass.
    ///
    /// This must not rebuild durable LRU state; `lru_by_access` is maintained transactionally by
    /// metadata writes and is treated as persistent state during normal startup.
    ///
    /// Callers must only install this value while there are no concurrent index writes. The normal
    /// server startup path satisfies that by running recovery before publishing the cache manager to
    /// request handling or periodic cleanup tasks.
    async fn replace_runtime_cache_usage(
        &self,
        usage: LogicalCacheUsage,
    ) -> StorageResult<()>;

    /// Returns logical resident cache usage maintained by the index.
    ///
    /// Implementations may return an eventually consistent value while concurrent writes are
    /// committing; capacity cleanup treats this as a fast trigger and re-checks as it evicts.
    async fn logical_cache_usage(&self) -> StorageResult<LogicalCacheUsage>;

    /// Scans resident metadata in oldest-access order.
    async fn oldest_cached_metas_page(
        &self,
        cursor: Option<LruScanCursor>,
        limit: usize,
    ) -> StorageResult<LruScanPage>;

    /// Reads meta (and the small payload when residency is `SmallKv`) in one logical query,
    /// applying the LRU touch policy at the same time.
    ///
    /// Transaction contract for KV-backed implementations:
    ///
    /// * inside the touch window: one read transaction (meta + optional small payload), no
    ///   writes;
    /// * across the touch window: one read transaction (meta + optional small payload) followed
    ///   by one write transaction that uses `touch_observed` to refresh `last_access_ns` without
    ///   re-reading meta.
    ///
    /// Non-transactional backends (in-memory) perform the equivalent work under a single
    /// internal lock and have no observable "txn" boundary.
    async fn open_hit(
        &self,
        key: &ObjectLocation,
        now_ns: u64,
        touch_granularity_ns: u64,
    ) -> StorageResult<Option<OpenHit>>;

    /// Writes a small-object row for `meta.key()` unless a row already exists.
    ///
    /// Runs as a single write transaction that performs insert-if-absent: on race-loss, the
    /// existing row is read out of the same transaction and returned; on race-win, the new row is
    /// committed alongside the small payload and the LRU tracking entry. Implementations must not
    /// fall back to a separate pre-read; the whole check-then-act lives inside one write txn.
    async fn admit_small_if_absent(
        &self,
        meta: CachedObjectMeta,
        payload: Vec<u8>,
        now_ns: u64,
    ) -> StorageResult<AdmitSmallOutcome>;
}
