use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;

use super::{
    AdmitSmallOutcome, CacheIndex, LogicalCacheUsage, LruScanCursor, LruScanPage,
    MetaScanCursor, MetaScanPage, OpenHit, SmallCacheEntry, SmallScanCursor,
    SmallScanPage,
};
use crate::cache::meta::{CacheState, CachedObjectMeta};
use crate::cache::should_touch;
use crate::error::{StorageError, StorageResult};
use crate::object::ObjectLocation;

/// Process-local [`crate::cache::CacheIndex`] for tests: BTrees mirror meta payloads, LRU keys, resident-byte totals.
#[derive(Default)]
pub struct InMemoryCacheIndex {
    inner: Mutex<InMemoryCacheState>,
}

#[derive(Default)]
struct InMemoryCacheState {
    meta: BTreeMap<ObjectLocation, CachedObjectMeta>,
    small: BTreeMap<ObjectLocation, Vec<u8>>,
    lru: BTreeSet<(u64, ObjectLocation)>,
    cached_bytes: u64,
}

impl InMemoryCacheIndex {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_inner(&self) -> MutexGuard<'_, InMemoryCacheState> {
        // The in-memory index has no durable recovery path, and its metadata,
        // payload, LRU, and resident-byte mirrors must stay consistent. Poisoning
        // means those invariants may be broken, so this implementation fails fast.
        self.inner
            .lock()
            .expect("in-memory cache index mutex poisoned; in-memory cache state is no longer trustworthy")
    }
}

fn store_meta_in_memory(inner: &mut InMemoryCacheState, meta: CachedObjectMeta) {
    let meta = meta.normalized();
    if let Some(old) = inner.meta.insert(meta.key().clone(), meta.clone()) {
        remove_from_in_memory_tracking(inner, &old);
    }
    add_to_in_memory_tracking(inner, &meta);
}

fn add_to_in_memory_tracking(
    inner: &mut InMemoryCacheState,
    meta: &CachedObjectMeta,
) {
    if meta.is_cache_resident() {
        inner.cached_bytes = inner.cached_bytes.saturating_add(meta.cached_bytes());
        inner.lru.insert((meta.last_access_ns, meta.key().clone()));
    }
}

fn remove_from_in_memory_tracking(
    inner: &mut InMemoryCacheState,
    meta: &CachedObjectMeta,
) {
    inner.lru.remove(&(meta.last_access_ns, meta.key().clone()));
    if meta.is_cache_resident() {
        inner.cached_bytes = inner.cached_bytes.saturating_sub(meta.cached_bytes());
    }
}

#[async_trait]
impl CacheIndex for InMemoryCacheIndex {
    async fn get_meta(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        Ok(self.lock_inner().meta.get(key).cloned())
    }

    async fn scan_meta_page(
        &self,
        cursor: Option<MetaScanCursor>,
        limit: usize,
    ) -> StorageResult<MetaScanPage> {
        use std::ops::Bound::{Excluded, Unbounded};

        let inner = self.lock_inner();
        let limit = limit.max(1);
        let start = cursor.map(|cursor| cursor.key);
        let iter: Box<
            dyn Iterator<Item = (&ObjectLocation, &CachedObjectMeta)> + '_,
        > = match start {
            Some(start) => Box::new(inner.meta.range((Excluded(start), Unbounded))),
            None => Box::new(inner.meta.iter()),
        };
        let mut metas = Vec::new();
        let mut next_cursor = None;
        for (key, meta) in iter {
            next_cursor = Some(MetaScanCursor { key: key.clone() });
            metas.push(meta.clone());
            if metas.len() >= limit {
                break;
            }
        }
        if metas.len() < limit {
            next_cursor = None;
        }
        Ok(MetaScanPage { metas, next_cursor })
    }

    async fn put_new_complete(
        &self,
        meta: CachedObjectMeta,
    ) -> StorageResult<CachedObjectMeta> {
        let meta = meta.normalized();
        if meta.cache_state() != CacheState::CompleteFile {
            return Err(StorageError::cache(format!(
                "metadata for {} is not complete-file residency",
                meta.key()
            )));
        }
        let mut inner = self.lock_inner();
        store_meta_in_memory(&mut inner, meta.clone());
        Ok(meta)
    }

    async fn delete_meta(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let mut inner = self.lock_inner();
        let old = inner.meta.remove(key);
        if let Some(old) = &old {
            remove_from_in_memory_tracking(&mut inner, old);
        }
        Ok(old)
    }

    async fn get_small(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<Vec<u8>>> {
        Ok(self.lock_inner().small.get(key).cloned())
    }

    async fn stat_small(&self, key: &ObjectLocation) -> StorageResult<Option<u64>> {
        Ok(self
            .lock_inner()
            .small
            .get(key)
            .map(|data| data.len() as u64))
    }

    async fn scan_small_entries_page(
        &self,
        cursor: Option<SmallScanCursor>,
        limit: usize,
    ) -> StorageResult<SmallScanPage> {
        use std::ops::Bound::{Excluded, Unbounded};

        let inner = self.lock_inner();
        let limit = limit.max(1);
        let start = cursor.map(|cursor| cursor.key);
        let iter: Box<dyn Iterator<Item = (&ObjectLocation, &Vec<u8>)> + '_> =
            match start {
                Some(start) => {
                    Box::new(inner.small.range((Excluded(start), Unbounded)))
                }
                None => Box::new(inner.small.iter()),
            };
        let mut entries = Vec::new();
        let mut next_cursor = None;
        for (key, data) in iter {
            next_cursor = Some(SmallScanCursor { key: key.clone() });
            entries.push(SmallCacheEntry {
                key: key.clone(),
                bytes: data.len() as u64,
            });
            if entries.len() >= limit {
                break;
            }
        }
        if entries.len() < limit {
            next_cursor = None;
        }
        Ok(SmallScanPage {
            entries,
            next_cursor,
        })
    }

    async fn remove_unclaimed_small_payload(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<()> {
        self.lock_inner().small.remove(key);
        Ok(())
    }

    async fn delete_meta_and_small(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<Option<CachedObjectMeta>> {
        let mut inner = self.lock_inner();
        inner.small.remove(key);
        let old = inner.meta.remove(key);
        if let Some(old) = &old {
            remove_from_in_memory_tracking(&mut inner, old);
        }
        Ok(old)
    }

    async fn replace_runtime_cache_usage(
        &self,
        usage: LogicalCacheUsage,
    ) -> StorageResult<()> {
        self.lock_inner().cached_bytes = usage.resident_bytes;
        Ok(())
    }

    async fn logical_cache_usage(&self) -> StorageResult<LogicalCacheUsage> {
        Ok(LogicalCacheUsage::resident(self.lock_inner().cached_bytes))
    }

    async fn oldest_cached_metas_page(
        &self,
        cursor: Option<LruScanCursor>,
        limit: usize,
    ) -> StorageResult<LruScanPage> {
        use std::ops::Bound::{Excluded, Unbounded};

        let inner = self.lock_inner();
        let limit = limit.max(1);
        let start = cursor.map(|cursor| (cursor.last_access_ns, cursor.key));
        let iter: Box<dyn Iterator<Item = &(u64, ObjectLocation)> + '_> = match start
        {
            Some(start) => Box::new(inner.lru.range((Excluded(start), Unbounded))),
            None => Box::new(inner.lru.iter()),
        };
        let mut metas = Vec::new();
        for (last_access_ns, key) in iter {
            let next_cursor = Some(LruScanCursor {
                last_access_ns: *last_access_ns,
                key: key.clone(),
            });
            if let Some(meta) = inner.meta.get(key).cloned() {
                metas.push(meta);
                if metas.len() >= limit {
                    return Ok(LruScanPage { metas, next_cursor });
                }
            }
        }
        Ok(LruScanPage {
            metas,
            next_cursor: None,
        })
    }

    async fn open_hit(
        &self,
        key: &ObjectLocation,
        now_ns: u64,
        touch_granularity_ns: u64,
    ) -> StorageResult<Option<OpenHit>> {
        let mut inner = self.lock_inner();
        let Some(mut meta) = inner.meta.get(key).cloned() else {
            return Ok(None);
        };
        if should_touch(meta.last_access_ns, now_ns, touch_granularity_ns) {
            meta.last_access_ns = now_ns;
            store_meta_in_memory(&mut inner, meta.clone());
        }
        let payload = match meta.cache_state() {
            CacheState::SmallKv => Some(Arc::<[u8]>::from(
                inner.small.get(key).cloned().ok_or_else(|| {
                    StorageError::cache(format!(
                        "small object missing from cache: {key}"
                    ))
                })?,
            )),
            CacheState::CompleteFile => None,
        };
        Ok(Some(OpenHit { meta, payload }))
    }

    async fn admit_small_if_absent(
        &self,
        mut meta: CachedObjectMeta,
        payload: Vec<u8>,
        now_ns: u64,
    ) -> StorageResult<AdmitSmallOutcome> {
        let mut inner = self.lock_inner();
        if let Some(existing) = inner.meta.get(key_of(&meta)).cloned() {
            let bytes = inner.small.get(key_of(&meta)).cloned().ok_or_else(|| {
                StorageError::cache(format!(
                    "small object missing from cache: {}",
                    meta.key()
                ))
            })?;
            return Ok(AdmitSmallOutcome::AlreadyPresent {
                meta: existing,
                payload: Arc::<[u8]>::from(bytes),
            });
        }
        meta.set_small(payload.len() as u64);
        meta.last_access_ns = now_ns;
        meta = meta.normalized();
        inner.small.insert(meta.key().clone(), payload.clone());
        store_meta_in_memory(&mut inner, meta.clone());
        Ok(AdmitSmallOutcome::Admitted {
            meta,
            payload: Arc::<[u8]>::from(payload),
        })
    }
}

fn key_of(meta: &CachedObjectMeta) -> &ObjectLocation {
    meta.key()
}
