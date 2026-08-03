use async_trait::async_trait;

use super::client::PersistentCacheIndex;
use super::keys::db_key;
use super::kv::CacheKv;
use super::ops::{meta, open, small, usage};
use crate::cache::index::{
    AdmitSmallOutcome, CacheIndex, LogicalCacheUsage, LruScanCursor, LruScanPage,
    MetaScanCursor, MetaScanPage, OpenHit, SmallScanCursor, SmallScanPage,
};
use crate::cache::meta::CachedObjectMeta;
use crate::error::StorageResult;
use crate::object::ObjectLocation;

#[async_trait]
impl<K: CacheKv> CacheIndex for PersistentCacheIndex<K> {
    async fn get_meta(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let key = db_key(key);
        self.run_kv(move |kv| meta::get_meta(kv, key.as_str()))
            .await
    }

    async fn scan_meta_page(
        &self,
        cursor: Option<MetaScanCursor>,
        limit: usize,
    ) -> StorageResult<MetaScanPage> {
        self.run_kv(move |kv| meta::scan_meta_page(kv, cursor, limit))
            .await
    }

    async fn put_new_complete(
        &self,
        meta: CachedObjectMeta,
    ) -> StorageResult<CachedObjectMeta> {
        self.run_tracked(move |kv, tracking| {
            meta::put_new_complete(kv, tracking, meta)
        })
        .await
    }

    async fn delete_meta(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let key = db_key(key);
        self.run_tracked(move |kv, tracking| {
            meta::delete_meta(kv, tracking, key.as_str())
        })
        .await
    }

    async fn get_small(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<Vec<u8>>> {
        let key = db_key(key);
        self.run_kv(move |kv| small::get_small(kv, key.as_str()))
            .await
    }

    async fn stat_small(&self, key: &ObjectLocation) -> StorageResult<Option<u64>> {
        let key = db_key(key);
        self.run_kv(move |kv| small::stat_small(kv, key.as_str()))
            .await
    }

    async fn scan_small_entries_page(
        &self,
        cursor: Option<SmallScanCursor>,
        limit: usize,
    ) -> StorageResult<SmallScanPage> {
        self.run_kv(move |kv| small::scan_small_entries_page(kv, cursor, limit))
            .await
    }

    async fn remove_unclaimed_small_payload(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<()> {
        let key = db_key(key);
        self.run_kv(move |kv| small::remove_unclaimed_small_payload(kv, key.as_str()))
            .await
    }

    async fn delete_meta_and_small(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let key = db_key(key);
        self.run_tracked(move |kv, tracking| {
            small::delete_meta_and_small(kv, tracking, key.as_str())
        })
        .await
    }

    async fn replace_runtime_cache_usage(
        &self,
        usage: LogicalCacheUsage,
    ) -> StorageResult<()> {
        self.tracking().replace_total(usage.resident_bytes);
        Ok(())
    }

    async fn logical_cache_usage(&self) -> StorageResult<LogicalCacheUsage> {
        Ok(self.tracking().logical_usage())
    }

    async fn oldest_cached_metas_page(
        &self,
        cursor: Option<LruScanCursor>,
        limit: usize,
    ) -> StorageResult<LruScanPage> {
        self.run_kv(move |kv| usage::oldest_cached_metas_page(kv, cursor, limit))
            .await
    }

    async fn open_hit(
        &self,
        key: &ObjectLocation,
        now_ns: u64,
        touch_granularity_ns: u64,
    ) -> StorageResult<Option<OpenHit>> {
        let key = db_key(key);
        self.run_tracked(move |kv, tracking| {
            open::open_hit(kv, tracking, key.as_str(), now_ns, touch_granularity_ns)
        })
        .await
    }

    async fn admit_small_if_absent(
        &self,
        meta: CachedObjectMeta,
        payload: Vec<u8>,
        now_ns: u64,
    ) -> StorageResult<AdmitSmallOutcome> {
        self.run_tracked(move |kv, tracking| {
            small::admit_small_if_absent(kv, tracking, meta, payload, now_ns)
        })
        .await
    }
}
