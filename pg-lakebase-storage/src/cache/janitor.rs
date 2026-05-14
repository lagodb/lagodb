use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use tracing::info;

use crate::cache::eviction::OrphanFileDeleted;
use crate::cache::{
    CacheCleanupPolicy, CacheCleanupReport, CacheEvictionOutcome, CacheIndex,
    CacheManager,
};
use crate::error::StorageResult;

const CLEANUP_LRU_PAGE_SIZE: usize = 32;

/// Serializes `CacheJanitor::cleanup` traversals that go through `CacheManager`'s `run_cleanup`.
/// `recover()` is intentionally **not** routed here; see `CacheManager::recover`.
#[derive(Default)]
pub(super) struct CleanupCoordinator {
    gate: AsyncMutex<()>,
}

impl CleanupCoordinator {
    pub(super) async fn lock(&self) -> AsyncMutexGuard<'_, ()> {
        self.gate.lock().await
    }

    pub(super) fn try_lock(&self) -> Option<AsyncMutexGuard<'_, ()>> {
        self.gate.try_lock().ok()
    }
}

/// Bounded cleanup pass invoked through [`crate::cache::CacheManager::run_cleanup`] (manual, write-triggered, or
/// periodic).
///
/// **Pipeline:** optional orphan deletes from [`crate::cache::inventory::RuntimeOrphanCandidates`], then LRU-driven
/// eviction until logical resident bytes fall near [`crate::cache::CacheCleanupPolicy::target_bytes`] or batch caps
/// trip.
///
/// Eviction walks oldest-first pages from [`crate::cache::index::CacheIndex::oldest_cached_metas_page`];
/// skipping [`crate::cache::object_state::PerObjectState`] activity may leave usage above target until the next
/// pass.
pub(crate) struct CacheJanitor<'a, I: CacheIndex> {
    cache: &'a CacheManager<I>,
}

impl<'a, I: CacheIndex> CacheJanitor<'a, I> {
    pub(crate) fn new(cache: &'a CacheManager<I>) -> Self {
        Self { cache }
    }

    pub(crate) async fn cleanup(
        &self,
        policy: CacheCleanupPolicy,
    ) -> StorageResult<CacheCleanupReport> {
        self.cache.prepare_dirs().await?;
        let mut report = CacheCleanupReport {
            bytes_before: self
                .cache
                .index
                .logical_cache_usage()
                .await?
                .resident_bytes,
            ..CacheCleanupReport::default()
        };

        // Orphan cleanup is always performed unconditionally.
        self.delete_orphans(&mut report).await?;

        let report = self.evict_by_lru(policy, report).await?;
        info!(
            bytes_before = report.bytes_before,
            bytes_after = report.bytes_after,
            evicted_objects = report.evicted_objects,
            bytes_evicted = report.bytes_evicted,
            orphan_complete_files_deleted = report.orphan_complete_files_deleted,
            orphan_partial_files_deleted = report.orphan_partial_files_deleted,
            "cache cleanup complete",
        );
        Ok(report)
    }

    pub(crate) async fn cleanup_capacity(
        &self,
        policy: CacheCleanupPolicy,
    ) -> StorageResult<CacheCleanupReport> {
        self.cache.prepare_dirs().await?;
        let report = CacheCleanupReport {
            bytes_before: self
                .cache
                .index
                .logical_cache_usage()
                .await?
                .resident_bytes,
            ..CacheCleanupReport::default()
        };
        self.evict_by_lru(policy, report).await
    }

    async fn evict_by_lru(
        &self,
        policy: CacheCleanupPolicy,
        mut report: CacheCleanupReport,
    ) -> StorageResult<CacheCleanupReport> {
        let mut usage = self.cache.index.logical_cache_usage().await?.resident_bytes;
        if usage <= policy.start_bytes() {
            report.bytes_after = usage;
            return Ok(report);
        }

        let target = policy.target_bytes();
        let mut cursor = None;
        while usage > target
            && report.evicted_objects < policy.max_cleanup_batch_items
            && report.bytes_evicted < policy.max_cleanup_batch_bytes
        {
            let page = self
                .cache
                .index
                .oldest_cached_metas_page(cursor, CLEANUP_LRU_PAGE_SIZE)
                .await?;
            cursor = page.next_cursor;
            if page.metas.is_empty() {
                break;
            }

            let mut reached_batch_bytes = false;
            for meta in page.metas {
                if usage <= target
                    || report.evicted_objects >= policy.max_cleanup_batch_items
                {
                    break;
                }
                if report.bytes_evicted >= policy.max_cleanup_batch_bytes {
                    reached_batch_bytes = true;
                    break;
                }
                if meta.cached_bytes() == 0 {
                    continue;
                }
                if self.cache.is_active(meta.key()) {
                    report.active_objects_skipped += 1;
                    continue;
                }
                let bytes = meta.cached_bytes();
                if report.bytes_evicted > 0
                    && report.bytes_evicted.saturating_add(bytes)
                        > policy.max_cleanup_batch_bytes
                {
                    reached_batch_bytes = true;
                    break;
                }
                match self.cache.evict_meta_if_current(meta).await? {
                    CacheEvictionOutcome::Evicted { bytes } => {
                        usage = usage.saturating_sub(bytes);
                        report.evicted_objects += 1;
                        report.bytes_evicted =
                            report.bytes_evicted.saturating_add(bytes);
                    }
                    CacheEvictionOutcome::Active => {
                        report.active_objects_skipped += 1;
                    }
                    CacheEvictionOutcome::Changed
                    | CacheEvictionOutcome::AlreadyGone
                    | CacheEvictionOutcome::NotResident => {
                        // Snapshot LRU iteration became stale; re-read authoritative sum instead of local subtraction
                        // drift.
                        usage = self
                            .cache
                            .index
                            .logical_cache_usage()
                            .await?
                            .resident_bytes;
                    }
                }
            }

            if cursor.is_none() || reached_batch_bytes {
                break;
            }
        }

        report.bytes_after =
            self.cache.index.logical_cache_usage().await?.resident_bytes;
        Ok(report)
    }

    async fn delete_orphans(
        &self,
        report: &mut CacheCleanupReport,
    ) -> StorageResult<()> {
        let candidates = self.cache.orphan_candidate_snapshot();

        for path in candidates.file_paths {
            match self.cache.delete_orphan_file_if_unclaimed(path).await? {
                Some(OrphanFileDeleted::Complete) => {
                    report.orphan_complete_files_deleted += 1
                }
                Some(OrphanFileDeleted::Partial) => {
                    report.orphan_partial_files_deleted += 1
                }
                None => {}
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::cache::util::create_parent_dir;
    use crate::cache::{
        CacheIndex, CacheManager, CachedObjectMeta, InMemoryCacheIndex,
    };
    use crate::object::{ObjectInfo, ObjectLocation};

    static TEST_CACHE_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn recovery_leaves_no_runtime_orphan_candidates() {
        let index = InMemoryCacheIndex::new();
        let complete_key =
            ObjectLocation::new("default", "bucket", "complete").unwrap();
        let deleted_partial_key =
            ObjectLocation::new("default", "bucket", "deleted-partial").unwrap();

        let cache = CacheManager::new(test_cache_dir(), index).with_limits(4, 4);
        cache.prepare_dirs().await.unwrap();

        let complete_meta = CachedObjectMeta::complete(
            complete_key.clone(),
            ObjectInfo {
                size: 8,
                etag: None,
            },
        );
        cache.index().put_new_complete(complete_meta).await.unwrap();
        write_cache_file(cache.complete_path(&complete_key).unwrap(), b"complete")
            .await;

        write_cache_file(cache.partial_path(&deleted_partial_key).unwrap(), b"")
            .await;

        cache.recover().await.unwrap();

        let candidates = cache.orphan_candidate_snapshot();

        assert!(candidates.file_paths.is_empty());
    }

    fn test_cache_dir() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let id = TEST_CACHE_ID.fetch_add(1, Ordering::Relaxed);
        PathBuf::from("/tmp").join(format!(
            "-cache-janitor-inventory-test-{}-{stamp}-{id}",
            std::process::id()
        ))
    }

    async fn write_cache_file(path: PathBuf, data: &[u8]) {
        create_parent_dir(&path).await.unwrap();
        tokio::fs::write(path, data).await.unwrap();
    }
}
