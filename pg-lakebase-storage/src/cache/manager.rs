//! [`CacheManager`] struct definition and core methods.
//!
//! The heavy lifting (admission, eviction, large-fill orchestration, startup recovery) lives in
//! sibling modules that add `impl CacheManager<I>` blocks; this file owns the struct layout,
//! constructors, and the methods that don't belong to any single subsystem.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::{CacheRuntimeHandle, StorageRuntime};
use crate::error::StorageResult;
use crate::object::{
    DEFAULT_CHUNK_SIZE, DEFAULT_SMALL_OBJECT_LIMIT, ObjectLocation,
    normalize_chunk_size,
};

use super::chunks::ReaperInbox;
use super::cleanup_scheduler::CleanupScheduler;
use super::index::CacheIndex;
use super::inventory::{RuntimeOrphanCandidateSnapshot, RuntimeOrphanCandidates};
use super::janitor::CacheJanitor;
use super::object_state::{ObjectStateRegistry, PerObjectState};
use super::path::CachePathResolver;
use super::startup::StartupRecovery;
use super::store::{
    CacheStore, FileCacheStore, PhysicalCacheEntryVisitor, SmallObjectStore,
};
use super::types::{CacheCleanupPolicy, CacheCleanupReport};
use super::usage::{
    CacheUsageSnapshot, LogicalCacheUsage, PhysicalCacheUsage, PhysicalUsageVisitor,
};

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
/// - **Cleanup scheduling:** the embedded [`CleanupScheduler`] owns the gate and the wake channel. Write paths only
///   `nudge_cleanup_after_write()`; the scheduler actor (started by [`Self::spawn_cleanup_scheduler`]) and manual
///   cleanup callers are the only routes that actually run [`CacheJanitor::cleanup`]. Startup [`Self::recover`]
///   deliberately bypasses the gate.
///
/// Implement [`crate::cache::index::CacheIndex`] so metadata, small rows, and LRU tracking stay mutually consistent.
pub struct CacheManager<I: CacheIndex> {
    pub(crate) paths: CachePathResolver,
    pub(crate) index: I,
    pub(in crate::cache) small_object_limit: u64,
    pub(in crate::cache) chunk_size: u64,
    pub(in crate::cache) object_states: ObjectStateRegistry,
    /// Receiver half of the large-fill reaper channel. `None` after
    /// [`Self::spawn_large_fill_reaper`] consumed it; double-spawn is a programmer error.
    pub(in crate::cache) reaper_inbox: std::sync::Mutex<Option<ReaperInbox>>,
    pub(in crate::cache) cleanup_scheduler: Arc<CleanupScheduler>,
    pub(in crate::cache) orphan_candidates: RuntimeOrphanCandidates,
    pub(in crate::cache) runtime: CacheRuntimeHandle,
}

impl<I: CacheIndex> CacheManager<I> {
    pub fn new(root: PathBuf, index: I, runtime: StorageRuntime) -> Self {
        Self::with_runtime_handle(root, index, runtime.cache_handle())
    }

    /// Variant of [`Self::new`] for code paths that already hold a [`CacheRuntimeHandle`] —
    /// today the only consumer is [`Self::new`] itself; kept as a separate `pub(crate)`
    /// constructor so future internal entry points can avoid going through `StorageRuntime`
    /// when they already have the cache slice in hand. External callers always go through
    /// [`Self::new`].
    pub(crate) fn with_runtime_handle(
        root: PathBuf,
        index: I,
        runtime: CacheRuntimeHandle,
    ) -> Self {
        let (object_states, reaper_inbox) = ObjectStateRegistry::new();
        Self {
            paths: CachePathResolver::new(root),
            index,
            small_object_limit: DEFAULT_SMALL_OBJECT_LIMIT,
            chunk_size: DEFAULT_CHUNK_SIZE,
            object_states,
            reaper_inbox: std::sync::Mutex::new(Some(reaper_inbox)),
            cleanup_scheduler: Arc::new(CleanupScheduler::new()),
            orphan_candidates: RuntimeOrphanCandidates::default(),
            runtime,
        }
    }

    pub fn with_limits(mut self, small_object_limit: u64, chunk_size: u64) -> Self {
        self.small_object_limit = small_object_limit;
        self.chunk_size = normalize_chunk_size(chunk_size);
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

    /// Snapshot of the cache touch granularity from the live runtime config.
    ///
    /// Returned in nanoseconds because every consumer (`open_hit` family) expects ns. This
    /// is on the OPEN hot path; the implementation is a single ArcSwap load + a [`Duration`]
    /// copy and allocates nothing.
    pub(in crate::cache) fn touch_granularity_ns(&self) -> u64 {
        duration_to_ns(self.runtime.touch_granularity())
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
                crate::error::StorageError::cache(format!(
                    "no live large fill for {key}"
                ))
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
    /// physical scan and one paged metadata scan. This does **not** acquire the [`CleanupScheduler`]
    /// gate used by `cleanup` and the background actor.
    ///
    /// **Production contract:** `StorageServerBuilder::bind` calls this once while the server is
    /// still private, then optional startup-only capacity cleanup, then wraps the manager in `Arc`
    /// and starts the scheduler actor (`sleep` before first run). So recover never races startup
    /// background cleanup on that default path.
    ///
    /// **Do not** call this while `cleanup` / a spawned scheduler may be active on the same
    /// manager unless you deliberately accept overlapping janitor traversals.
    pub async fn recover(&self) -> StorageResult<super::CacheRecoveryReport> {
        StartupRecovery::new(self).recover().await
    }

    /// Manual orphan reclamation pass.
    ///
    /// Drains the gate and runs [`crate::cache::janitor::CacheJanitor::cleanup`] without a
    /// capacity policy — cached payloads are not evicted, only orphan candidates (partial
    /// files left by aborted fills, complete payloads whose unlink failed) are removed.
    pub async fn cleanup_orphans(&self) -> StorageResult<CacheCleanupReport> {
        self.cleanup_scheduler.run_manual(self, None).await
    }

    /// Manual cleanup pass: orphan reclamation **plus** LRU capacity eviction toward the
    /// supplied policy's target.
    ///
    /// Manual capacity cleanup is not threshold-gated; the policy's target is honoured
    /// regardless of where the resident bytes currently sit relative to the policy's start
    /// watermark. Callers that want orphan-only semantics use [`Self::cleanup_orphans`].
    pub async fn cleanup_with_capacity(
        &self,
        policy: CacheCleanupPolicy,
    ) -> StorageResult<CacheCleanupReport> {
        self.cleanup_scheduler.run_manual(self, Some(policy)).await
    }

    /// Startup-only capacity cleanup. `recover` has already removed startup orphans, so this path
    /// deliberately skips the orphan pass and evicts only through the persistent LRU index.
    pub(crate) async fn cleanup_capacity_only(
        &self,
        policy: CacheCleanupPolicy,
    ) -> StorageResult<CacheCleanupReport> {
        CacheJanitor::new(self).cleanup_capacity(policy).await
    }

    /// Synchronous "the cache just grew" hint, called from admission and large-fill promote.
    ///
    /// Allocates nothing, never awaits. Wakes (or arms) the scheduler actor; the actor decides
    /// whether the threshold is crossed and whether to acquire the gate.
    pub(in crate::cache) fn nudge_cleanup_after_write(&self) {
        self.cleanup_scheduler.nudge();
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
}

impl<I: CacheIndex + 'static> CacheManager<I> {
    /// Spawns the cleanup scheduler actor. Call once after wrapping the manager in `Arc` and
    /// before accepting traffic; double-spawn is harmless but wastes a task.
    ///
    /// `shutdown` is the cancellation token the embedder uses to stop the actor (the
    /// [`crate::server::StorageServer`] passes its background-tasks token here). The actor also
    /// exits if the manager's last strong reference drops.
    ///
    /// The runtime-config watch is subscribed **synchronously** here, before spawning the
    /// actor task. If subscription happened inside the spawned future, a config `apply` racing
    /// the scheduler's first poll could be lost — this matters in tests but also matters in
    /// production for any embedder that applies an initial config before traffic starts.
    pub fn spawn_cleanup_scheduler(
        self: &Arc<Self>,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let scheduler = self.cleanup_scheduler.clone();
        let runtime = self.runtime.clone();
        let changes = runtime.subscribe();
        let weak = Arc::downgrade(self);
        tokio::spawn(scheduler.run(weak, runtime, changes, shutdown))
    }
}
pub(in crate::cache) fn duration_to_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
