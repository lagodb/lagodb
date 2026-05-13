//! Test doubles used by the service integration tests.
//!
//! [`BlockingRangeBackend`] lets a test pause the first `get_range` so multiple readers can
//! converge on the same large fill, while [`CountingCompleteIndex`] counts `put_new_complete`
//! invocations to verify the "commit-once" contract for concurrent fills.

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio::sync::Notify;

use crate::backend::{MemoryObjectBackend, ObjectBackend};
use crate::cache::{
    AdmitSmallOutcome, CacheIndex, CachedObjectMeta, LogicalCacheUsage, LruScanCursor, LruScanPage, MetaScanCursor,
    MetaScanPage, OpenHit, SmallScanCursor, SmallScanPage,
};
use crate::error::StorageResult;
use crate::object::{ObjectInfo, ObjectLocation};

/// [`ObjectBackend`] wrapper that blocks the first `get_range` until the test releases it.
pub(crate) struct BlockingRangeBackend {
    inner: MemoryObjectBackend,
    block_next_range_get: AtomicBool,
    range_gets: AtomicUsize,
    first_range_get_started: Notify,
    release_first_range_get: Notify,
}

impl BlockingRangeBackend {
    pub(crate) fn new(inner: MemoryObjectBackend) -> Self {
        Self {
            inner,
            block_next_range_get: AtomicBool::new(true),
            range_gets: AtomicUsize::new(0),
            first_range_get_started: Notify::new(),
            release_first_range_get: Notify::new(),
        }
    }

    pub(crate) async fn wait_until_first_range_get_starts(&self) {
        self.first_range_get_started.notified().await;
    }

    pub(crate) fn release_first_range_get(&self) {
        self.release_first_range_get.notify_waiters();
    }

    pub(crate) fn range_gets(&self) -> usize {
        self.range_gets.load(Ordering::Acquire)
    }
}

#[async_trait]
impl ObjectBackend for BlockingRangeBackend {
    async fn head(&self, key: &ObjectLocation) -> StorageResult<ObjectInfo> {
        self.inner.head(key).await
    }

    async fn get_range(&self, key: &ObjectLocation, range: Range<u64>) -> StorageResult<bytes::Bytes> {
        self.range_gets.fetch_add(1, Ordering::AcqRel);
        if self.block_next_range_get.swap(false, Ordering::AcqRel) {
            self.first_range_get_started.notify_waiters();
            self.release_first_range_get.notified().await;
        }
        self.inner.get_range(key, range).await
    }

    async fn put_from_file(
        &self,
        key: &ObjectLocation,
        path: &std::path::Path,
        len: u64,
    ) -> StorageResult<ObjectInfo> {
        self.inner.put_from_file(key, path, len).await
    }

    fn list(
        &self,
        store_id: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> futures::stream::BoxStream<'static, StorageResult<crate::object::ListEntry>> {
        self.inner.list(store_id, bucket, prefix)
    }

    async fn delete(&self, key: &ObjectLocation) -> StorageResult<()> {
        self.inner.delete(key).await
    }

    fn delete_stream(
        &self,
        store_id: &str,
        bucket: &str,
        keys: futures::stream::BoxStream<'static, StorageResult<String>>,
    ) -> futures::stream::BoxStream<'static, StorageResult<String>> {
        self.inner.delete_stream(store_id, bucket, keys)
    }
}

/// [`ObjectBackend`] wrapper that blocks the first `head` until the test releases it.
///
/// Dual of [`BlockingRangeBackend`] but gated on HEAD: lets tests converge multiple concurrent
/// OPENs onto the establishment single-flight leader's in-flight HEAD so follower behavior
/// can be observed while the leader is still running.
pub(crate) struct BlockingHeadBackend {
    inner: MemoryObjectBackend,
    block_next_head: AtomicBool,
    head_calls: AtomicUsize,
    first_head_started: Notify,
    release_first_head: Notify,
    /// Optional error the first HEAD should return after being released.
    fail_first_head_with_not_found: AtomicBool,
}

impl BlockingHeadBackend {
    pub(crate) fn new(inner: MemoryObjectBackend) -> Self {
        Self {
            inner,
            block_next_head: AtomicBool::new(true),
            head_calls: AtomicUsize::new(0),
            first_head_started: Notify::new(),
            release_first_head: Notify::new(),
            fail_first_head_with_not_found: AtomicBool::new(false),
        }
    }

    pub(crate) async fn wait_until_first_head_starts(&self) {
        self.first_head_started.notified().await;
    }

    pub(crate) fn release_first_head(&self) {
        self.release_first_head.notify_waiters();
    }

    pub(crate) fn head_calls(&self) -> usize {
        self.head_calls.load(Ordering::Acquire)
    }

    /// Configure the first (blocked) HEAD to return `NotFound` once released. Subsequent HEADs
    /// fall through to the inner backend unchanged.
    pub(crate) fn fail_first_head_with_not_found(&self) {
        self.fail_first_head_with_not_found.store(true, Ordering::Release);
    }
}

#[async_trait]
impl ObjectBackend for BlockingHeadBackend {
    async fn head(&self, key: &ObjectLocation) -> StorageResult<ObjectInfo> {
        self.head_calls.fetch_add(1, Ordering::AcqRel);
        let is_first = self.block_next_head.swap(false, Ordering::AcqRel);
        if is_first {
            self.first_head_started.notify_waiters();
            self.release_first_head.notified().await;
            if self.fail_first_head_with_not_found.load(Ordering::Acquire) {
                return Err(crate::error::StorageError::not_found(key.to_string()));
            }
        }
        self.inner.head(key).await
    }

    async fn get_range(&self, key: &ObjectLocation, range: Range<u64>) -> StorageResult<bytes::Bytes> {
        self.inner.get_range(key, range).await
    }

    async fn put_from_file(
        &self,
        key: &ObjectLocation,
        path: &std::path::Path,
        len: u64,
    ) -> StorageResult<ObjectInfo> {
        self.inner.put_from_file(key, path, len).await
    }

    fn list(
        &self,
        store_id: &str,
        bucket: &str,
        prefix: Option<&str>,
    ) -> futures::stream::BoxStream<'static, StorageResult<crate::object::ListEntry>> {
        self.inner.list(store_id, bucket, prefix)
    }

    async fn delete(&self, key: &ObjectLocation) -> StorageResult<()> {
        self.inner.delete(key).await
    }

    fn delete_stream(
        &self,
        store_id: &str,
        bucket: &str,
        keys: futures::stream::BoxStream<'static, StorageResult<String>>,
    ) -> futures::stream::BoxStream<'static, StorageResult<String>> {
        self.inner.delete_stream(store_id, bucket, keys)
    }
}

pub(crate) struct CountingCompleteIndex<I> {
    inner: I,
    complete_puts: AtomicUsize,
}

impl<I> CountingCompleteIndex<I> {
    pub(crate) fn new(inner: I) -> Self {
        Self {
            inner,
            complete_puts: AtomicUsize::new(0),
        }
    }

    pub(crate) fn complete_puts(&self) -> usize {
        self.complete_puts.load(Ordering::Acquire)
    }
}

#[async_trait]
impl<I: CacheIndex> CacheIndex for CountingCompleteIndex<I> {
    async fn get_meta(&self, key: &ObjectLocation) -> StorageResult<Option<CachedObjectMeta>> {
        self.inner.get_meta(key).await
    }

    async fn scan_meta_page(&self, cursor: Option<MetaScanCursor>, limit: usize) -> StorageResult<MetaScanPage> {
        self.inner.scan_meta_page(cursor, limit).await
    }

    async fn put_new_complete(&self, meta: CachedObjectMeta) -> StorageResult<CachedObjectMeta> {
        self.complete_puts.fetch_add(1, Ordering::AcqRel);
        self.inner.put_new_complete(meta).await
    }

    async fn delete_meta(&self, key: &ObjectLocation) -> StorageResult<Option<CachedObjectMeta>> {
        self.inner.delete_meta(key).await
    }

    async fn get_small(&self, key: &ObjectLocation) -> StorageResult<Option<Vec<u8>>> {
        self.inner.get_small(key).await
    }

    async fn stat_small(&self, key: &ObjectLocation) -> StorageResult<Option<u64>> {
        self.inner.stat_small(key).await
    }

    async fn scan_small_entries_page(
        &self,
        cursor: Option<SmallScanCursor>,
        limit: usize,
    ) -> StorageResult<SmallScanPage> {
        self.inner.scan_small_entries_page(cursor, limit).await
    }

    async fn remove_unclaimed_small_payload(&self, key: &ObjectLocation) -> StorageResult<()> {
        self.inner.remove_unclaimed_small_payload(key).await
    }

    async fn delete_meta_and_small(&self, key: &ObjectLocation) -> StorageResult<Option<CachedObjectMeta>> {
        self.inner.delete_meta_and_small(key).await
    }

    async fn replace_runtime_cache_usage(&self, usage: LogicalCacheUsage) -> StorageResult<()> {
        self.inner.replace_runtime_cache_usage(usage).await
    }

    async fn logical_cache_usage(&self) -> StorageResult<LogicalCacheUsage> {
        self.inner.logical_cache_usage().await
    }

    async fn oldest_cached_metas_page(
        &self,
        cursor: Option<LruScanCursor>,
        limit: usize,
    ) -> StorageResult<LruScanPage> {
        self.inner.oldest_cached_metas_page(cursor, limit).await
    }

    async fn open_hit(
        &self,
        key: &ObjectLocation,
        now_ns: u64,
        touch_granularity_ns: u64,
    ) -> StorageResult<Option<OpenHit>> {
        self.inner.open_hit(key, now_ns, touch_granularity_ns).await
    }

    async fn admit_small_if_absent(
        &self,
        meta: CachedObjectMeta,
        payload: Vec<u8>,
        now_ns: u64,
    ) -> StorageResult<AdmitSmallOutcome> {
        self.inner.admit_small_if_absent(meta, payload, now_ns).await
    }
}
