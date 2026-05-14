//! [`CacheManager`] struct definition and core methods.
//!
//! The heavy lifting (admission, eviction, large-fill orchestration, startup recovery) lives in
//! sibling modules that add `impl CacheManager<I>` blocks; this file owns the struct layout,
//! constructors, and the methods that don't belong to any single subsystem.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{StorageError, StorageResult};
use crate::object::{
    DEFAULT_CHUNK_SIZE, DEFAULT_SMALL_OBJECT_LIMIT, ObjectLocation, StoreId,
    normalize_chunk_size,
};

use super::chunks::ReaperInbox;
use super::index::CacheIndex;
use super::inventory::{RuntimeOrphanCandidateSnapshot, RuntimeOrphanCandidates};
use super::janitor::{CacheJanitor, CleanupCoordinator};
use super::object_state::{ObjectStateRegistry, PerObjectState};
use super::path::CachePathResolver;
use super::startup::StartupRecovery;
use super::store::{
    CacheStore, FileCacheStore, PhysicalCacheEntryVisitor, SmallObjectStore,
};
use super::types::{CacheCleanupPolicy, CacheCleanupReport, CachePurgeReport};
use super::usage::{
    CacheUsageSnapshot, LogicalCacheUsage, PhysicalCacheUsage, PhysicalUsageVisitor,
};

/// Triggers for `CacheManager`'s `run_cleanup` (the only path that takes `CleanupCoordinator`'s gate).
///
/// There is no `Startup` variant by design: production startup runs
/// `CacheManager::recover` then optional startup-only capacity cleanup in
/// `StorageServerBuilder::bind` before accepting traffic and before spawning the periodic cleanup
/// task. Runtime cleanup triggers below all operate after startup reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::cache) enum CleanupTrigger {
    Manual,
    WritePath,
    Periodic,
}

/// Orchestrates on-disk cache files, small-object payloads, and index mutations for one cache root.
///
/// Ordering contracts callers rely on:
///
/// - **Per-object serialization:** the internal [`ObjectStateRegistry`] hands out a single
///   [`PerObjectState`] per [`crate::object::ObjectLocation`]; its embedded async mutex ensures large-fill writes,
///   promotions, eviction, and metadata mutations for one key do not overlap another task's critical section on that
///   key.
/// - **Single-version identity:** [`crate::object::ObjectLocation`] is the cache/fill identity. `ObjectInfo`
///   (`size`/`etag`) is frozen for the current cache lifecycle once admitted to a cache row or large-fill session.
///   `CacheManager` does not reconcile backend changes, does not host multiple generations for one key, and relies on
///   explicit `invalidate_object_cache` calls as the only freshness/version boundary. Capacity eviction may remove the
///   current residency, but it is not backend reconciliation.
/// - **Derived cache:** Partial files may exist without metadata. Complete-file eviction deletes metadata before unlink
///   so logical resident bytes drop before orphaned bytes disappear from disk (see `cache/eviction.rs`).
/// - **Cleanup gate:** an internal async mutex serializes janitor traversals from periodic/manual/write-triggered
///   cleanup; startup [`Self::recover`] deliberately bypasses that gate.
///
/// Implement [`crate::cache::index::CacheIndex`] so metadata, small rows, and LRU tracking stay mutually consistent.
pub struct CacheManager<I: CacheIndex> {
    pub(crate) paths: CachePathResolver,
    pub(crate) index: I,
    pub(in crate::cache) small_object_limit: u64,
    pub(in crate::cache) chunk_size: u64,
    pub(in crate::cache) cleanup_policy: Option<CacheCleanupPolicy>,
    pub(crate) touch_granularity: Duration,
    pub(in crate::cache) object_states: ObjectStateRegistry,
    /// Receiver half of the large-fill reaper channel. `None` after
    /// [`Self::spawn_large_fill_reaper`] consumed it; double-spawn is a programmer error.
    pub(in crate::cache) reaper_inbox: std::sync::Mutex<Option<ReaperInbox>>,
    pub(in crate::cache) cleanup: CleanupCoordinator,
    pub(in crate::cache) orphan_candidates: RuntimeOrphanCandidates,
}

impl<I: CacheIndex> CacheManager<I> {
    pub fn new(root: PathBuf, index: I) -> Self {
        let (object_states, reaper_inbox) = ObjectStateRegistry::new();
        Self {
            paths: CachePathResolver::new(root),
            index,
            small_object_limit: DEFAULT_SMALL_OBJECT_LIMIT,
            chunk_size: DEFAULT_CHUNK_SIZE,
            cleanup_policy: None,
            touch_granularity: Duration::from_secs(60),
            object_states,
            reaper_inbox: std::sync::Mutex::new(Some(reaper_inbox)),
            cleanup: CleanupCoordinator::default(),
            orphan_candidates: RuntimeOrphanCandidates::default(),
        }
    }

    pub fn with_limits(mut self, small_object_limit: u64, chunk_size: u64) -> Self {
        self.small_object_limit = small_object_limit;
        self.chunk_size = normalize_chunk_size(chunk_size);
        self
    }

    pub fn with_cleanup_policy(mut self, cleanup_policy: CacheCleanupPolicy) -> Self {
        self.cleanup_policy = Some(cleanup_policy);
        self
    }

    pub fn with_touch_granularity(mut self, touch_granularity: Duration) -> Self {
        self.touch_granularity = touch_granularity;
        self
    }

    #[cfg(test)]
    pub(crate) fn index(&self) -> &I {
        &self.index
    }

    pub fn small_object_limit(&self) -> u64 {
        self.small_object_limit
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    /// Placeholder for future per-store cache eviction; today it validates the API shape and leaves cache data intact.
    pub async fn purge_store_cache(
        &self,
        _store_id: &StoreId,
    ) -> StorageResult<CachePurgeReport> {
        Ok(CachePurgeReport::default())
    }

    #[cfg(test)]
    pub(crate) fn has_live_large_fill(&self, key: &ObjectLocation) -> bool {
        self.object_states
            .get_existing(key)
            .and_then(|state| state.live_fill_session())
            .is_some()
    }

    #[cfg(test)]
    pub(crate) fn live_large_fill_partial_path(
        &self,
        key: &ObjectLocation,
    ) -> Option<PathBuf> {
        self.object_states
            .get_existing(key)
            .and_then(|state| state.live_fill_session())
            .map(|session| session.partial_path().to_path_buf())
    }

    /// **Test-only helper; bypasses the per-object lock.**
    ///
    /// All production [`Self::abort_large_fill`] call-sites (chunk write failure, promotion rename
    /// failure, [`Self::invalidate_object_cache`]) hold the object lock so abort and fill-chunk
    /// writes cannot interleave for the same key. The large-fill reaper (see
    /// [`Self::reap_large_fill`]) reaches `abort_large_fill` only by way of a session's `Drop`,
    /// and likewise takes the object lock before doing any work.
    ///
    /// This helper intentionally does **not** take that lock so tests can race an abort against a
    /// leader that is blocked mid-backend-I/O (for example, to exercise
    /// [`LargeFillSession::abort`] waking up chunk waiters). Do not use it in production code.
    #[cfg(test)]
    pub(crate) async fn abort_live_large_fill_for_test(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<()> {
        let session = self
            .object_states
            .get_existing(key)
            .and_then(|state| state.live_fill_session())
            .ok_or_else(|| {
                StorageError::cache(format!("no live large fill for {key}"))
            })?;
        self.abort_large_fill(&session).await
    }

    pub(crate) fn is_active(&self, key: &ObjectLocation) -> bool {
        self.object_states
            .get_existing(key)
            .is_some_and(|state| state.is_active())
    }

    /// Returns the per-object state for `key`, creating one if necessary. Every caller that
    /// needs the object lock, an activity guard, or the live fill session goes through this
    /// entry point.
    pub(crate) fn object_state(&self, key: &ObjectLocation) -> Arc<PerObjectState> {
        self.object_states.get_or_create(key)
    }

    pub async fn logical_cache_usage(&self) -> StorageResult<LogicalCacheUsage> {
        self.index.logical_cache_usage().await
    }

    /// Scans physical cache payload storage and reports payload bytes by store kind.
    ///
    /// This traverses the cache directories and small-object payload store, so its cost scales with
    /// the number of physical cache entries. Do not use it on write/cleanup hot paths.
    pub async fn scan_physical_cache_usage(
        &self,
    ) -> StorageResult<PhysicalCacheUsage> {
        let mut visitor = PhysicalUsageVisitor::default();
        self.visit_physical_cache_entries(&mut visitor).await?;
        Ok(visitor.usage())
    }

    /// Returns a best-effort diagnostic usage snapshot.
    ///
    /// Physical usage is collected by traversing cache directories and the small-object payload
    /// store, so this can be expensive on large caches. The logical and physical values are not an
    /// atomic snapshot under concurrent cache mutations.
    pub async fn scan_usage_snapshot(&self) -> StorageResult<CacheUsageSnapshot> {
        Ok(CacheUsageSnapshot {
            logical: self.logical_cache_usage().await?,
            physical: self.scan_physical_cache_usage().await?,
        })
    }

    pub(crate) fn clear_orphan_candidates(&self) {
        self.orphan_candidates.clear_all();
    }

    pub(in crate::cache) fn orphan_candidate_snapshot(
        &self,
    ) -> RuntimeOrphanCandidateSnapshot {
        self.orphan_candidates.snapshot()
    }

    pub(crate) async fn visit_physical_cache_entries(
        &self,
        visitor: &mut dyn PhysicalCacheEntryVisitor,
    ) -> StorageResult<()> {
        self.file_cache_store().visit_entries(visitor).await?;
        self.small_object_store().visit_entries(visitor).await?;
        Ok(())
    }

    pub async fn prepare_dirs(&self) -> StorageResult<()> {
        tokio::fs::create_dir_all(self.paths.objects_dir()).await?;
        Ok(())
    }

    /// Install runtime logical usage and delete unclaimed physical payloads with one startup
    /// physical scan and one paged metadata scan. This does **not** acquire the `CleanupCoordinator` gate used by
    /// `cleanup` / `run_cleanup`.
    ///
    /// **Production contract:** `StorageServerBuilder::bind` calls this once while the server is
    /// still private, then optional startup-only capacity cleanup, then wraps the manager in `Arc`
    /// and starts the periodic task (`sleep` before first run). So recover never races startup
    /// periodic cleanup on that default path.
    ///
    /// **Do not** call this while `cleanup` / periodic `spawn_cleanup_task` work may be active on
    /// the same manager unless you deliberately accept overlapping janitor traversals.
    pub async fn recover(&self) -> StorageResult<super::CacheRecoveryReport> {
        StartupRecovery::new(self).recover().await
    }

    pub async fn cleanup(
        &self,
        policy: CacheCleanupPolicy,
    ) -> StorageResult<CacheCleanupReport> {
        self.run_cleanup(policy, CleanupTrigger::Manual)
            .await?
            .ok_or_else(|| {
                StorageError::cache("manual cleanup was unexpectedly skipped")
            })
    }

    /// Startup-only capacity cleanup. `recover` has already removed startup orphans, so this path
    /// deliberately skips the orphan pass and evicts only through the persistent LRU index.
    pub(crate) async fn cleanup_capacity_only(
        &self,
        policy: CacheCleanupPolicy,
    ) -> StorageResult<CacheCleanupReport> {
        CacheJanitor::new(self).cleanup_capacity(policy).await
    }

    /// Single place that takes `CleanupCoordinator`'s gate for `CacheJanitor::cleanup` (not for
    /// `recover`). Write/periodic triggers use `try_lock` to avoid piling up traversals.
    pub(in crate::cache) async fn run_cleanup(
        &self,
        policy: CacheCleanupPolicy,
        trigger: CleanupTrigger,
    ) -> StorageResult<Option<CacheCleanupReport>> {
        match trigger {
            CleanupTrigger::Manual => {
                let _cleanup_guard = self.cleanup.lock().await;
                Ok(Some(CacheJanitor::new(self).cleanup(policy).await?))
            }
            CleanupTrigger::WritePath | CleanupTrigger::Periodic => {
                let Some(_cleanup_guard) = self.cleanup.try_lock() else {
                    return Ok(None);
                };
                if trigger == CleanupTrigger::WritePath
                    && self.index.logical_cache_usage().await?.resident_bytes
                        <= policy.start_bytes()
                {
                    return Ok(None);
                }
                Ok(Some(CacheJanitor::new(self).cleanup(policy).await?))
            }
        }
    }

    pub fn partial_path(&self, key: &ObjectLocation) -> StorageResult<PathBuf> {
        self.paths.partial_path(key)
    }

    pub fn complete_path(&self, key: &ObjectLocation) -> StorageResult<PathBuf> {
        self.paths.complete_path(key)
    }

    pub fn validate_file_cache_paths(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<()> {
        self.partial_path(key)?;
        self.complete_path(key)?;
        Ok(())
    }

    pub(crate) fn file_cache_store(&self) -> FileCacheStore {
        FileCacheStore::new(self.paths.clone())
    }

    pub(crate) fn small_object_store(&self) -> SmallObjectStore<'_, I> {
        SmallObjectStore { index: &self.index }
    }

    pub(crate) async fn maybe_cleanup(&self) -> StorageResult<()> {
        let Some(policy) = self.cleanup_policy else {
            return Ok(());
        };
        if self.index.logical_cache_usage().await?.resident_bytes
            > policy.start_bytes()
        {
            let _ = self.run_cleanup(policy, CleanupTrigger::WritePath).await?;
        }
        Ok(())
    }
}

impl<I: CacheIndex + 'static> CacheManager<I> {
    pub fn spawn_cleanup_task(
        self: Arc<Self>,
        policy: CacheCleanupPolicy,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let _ = self.run_cleanup(policy, CleanupTrigger::Periodic).await;
            }
        })
    }
}

pub(in crate::cache) fn duration_to_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
