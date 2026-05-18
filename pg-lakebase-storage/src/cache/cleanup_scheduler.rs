//! Background cleanup scheduling for [`CacheManager`].
//!
//! The cache subsystem has three categories of reclamation work:
//!
//! * **Capacity eviction** — bring `resident_bytes` below the operator-configured target by
//!   evicting LRU entries. Driven by writes that grow the cache past `start_bytes`.
//! * **Orphan reclamation** — delete partial files left by aborted fills, complete payloads
//!   whose unlink failed during eviction, and similar derived-cache debris. Driven by orphan
//!   candidates being recorded, not by capacity. This is correctness, not optimisation.
//! * **Manual cleanup** — `CacheManager::cleanup_orphans` /
//!   `CacheManager::cleanup_with_capacity` invoked from administrative tooling.
//!
//! [`CleanupScheduler`] is the single owner of the gate that serialises janitor traversals
//! and the actor that drives background work. The four call-sites converge through three
//! distinct trigger semantics:
//!
//! 1. **Write-completion nudge** ([`Self::nudge`]). Synchronous, allocation-free, never
//!    awaits. Communicates **only** "resident_bytes grew" — orphan candidates are recorded
//!    independently by the paths that create them and do not need a separate signal here.
//!    The actor runs *capacity-only* in response, and skips the pass entirely when no
//!    capacity cap is configured or usage is below `start_bytes`.
//! 2. **Periodic ticker**. Drives the orphan + capacity pass on the configured interval.
//!    Always runs the orphan pass; capacity work runs only if a cap is configured.
//! 3. **Hot-reload of [`crate::config::CacheRuntimeConfig`]**. Re-evaluates immediately so a
//!    config change that opens or tightens caps takes effect without waiting for the next
//!    nudge or tick. Behaviourally equivalent to a periodic tick.
//! 4. **Manual** ([`Self::run_manual`]). Awaits the gate, runs whatever the caller asked for.
//!
//! # Trade-off when no `cleanup_interval` is configured
//!
//! Orphan reclamation runs only on periodic ticks, hot-reloads, manual calls, and startup
//! recovery. If the embedder leaves `cleanup_interval = None`, runtime orphan candidates
//! recorded by abort paths and failed unlinks are not reaped until one of those events
//! fires. This is normally fine because:
//!
//! * orphan creation is rare (abort_large_fill, eviction unlink failure — both are
//!   exceptional paths, not the steady-state),
//! * startup recovery deletes every leftover under the cache root, so disk usage is bounded
//!   by orphan accumulation between restarts,
//! * GUC defaults set `cleanup_interval` together with `max_cache_bytes`, so any operator
//!   who cares about capacity also gets periodic orphan reclamation for free.
//!
//! Embedders that disable both `cleanup_interval` and `max_cache_bytes` accept that runtime
//! orphan reclamation is dormant until manual cleanup or restart. The trade-off is
//! intentional: it keeps the write path from running an orphan-walker scan on every admit
//! when the cache is healthy.
//!
//! # Why write nudges do not drive orphan reclamation
//!
//! Writes that succeed do not create orphans (admit_small commits to KV; large promote
//! atomically renames partial→complete and clears the orphan-candidate registry). The paths
//! that *do* create orphans — failed-unlink during eviction, abort_large_fill —
//! [`crate::cache::CacheManager::orphan_candidates`] their candidates and rely on
//! periodic / reload / manual passes for actual deletion. Routing every successful write
//! through the orphan walker would cost a `RuntimeOrphanCandidates` snapshot + janitor pass
//! per write with zero expected work to do.
//!
//! # Why background `lock().await` and not `try_lock`
//!
//! There is exactly one background actor and at most one manual caller. With a single
//! background producer, "another background pass is already in flight" cannot happen — the
//! only contender for the gate is `run_manual`. If we used `try_lock`, the actor would lose
//! a trigger every time manual cleanup happens to be running: the nudge / reload / periodic
//! event has already been consumed from `select!`, but no actual reclamation runs, and the
//! work it represented sits unprocessed until something else fires. Awaiting the gate keeps
//! the trigger paired with the work it requested. Shutdown cancellation makes the wait safe.
//!
//! # Coalescing
//!
//! [`Notify`] retains at most one permit. A burst of `nudge()` calls during a single
//! quiescent window collapses to one wake.
//!
//! # Why ordinary `tokio::spawn` and not `spawn_blocking`
//!
//! [`super::janitor::CacheJanitor::cleanup`] is async I/O end-to-end (`tokio::fs::*`, async
//! index transactions). The persistent index already offloads its blocking redb operations
//! through `spawn_blocking` at the point of contact; that is the right granularity.
//! `spawn_blocking` is the wrong tool for an async maintenance loop.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use super::CacheManager;
use super::index::CacheIndex;
use super::janitor::CacheJanitor;
use super::types::{CacheCleanupPolicy, CacheCleanupReport};
use crate::config::{CacheCleanupSnapshot, CacheRuntimeHandle};
use crate::error::StorageResult;

/// Owns the cleanup gate and the wake channel; lives on every [`CacheManager`].
pub(crate) struct CleanupScheduler {
    notify: Notify,
    gate: AsyncMutex<()>,
}

/// What kind of pass the actor is about to run after a wake. Decides which
/// [`CacheJanitor`] entry point is invoked and whether capacity work is threshold-gated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackgroundTrigger {
    /// Resident-byte growth on the write path. Capacity-only and threshold-gated.
    WriteNudge,
    /// Periodic tick or hot-reload. Orphan pass always; capacity if configured.
    Maintenance,
}

impl CleanupScheduler {
    pub(crate) fn new() -> Self {
        Self {
            notify: Notify::new(),
            gate: AsyncMutex::new(()),
        }
    }

    /// Synchronous "resident_bytes grew" hint. Used by admission / large-fill promote paths.
    ///
    /// Communicates capacity growth only. Orphan creation paths register their candidates
    /// directly and do not call this.
    pub(crate) fn nudge(&self) {
        self.notify.notify_one();
    }

    /// Manual cleanup entry point.
    ///
    /// Awaits the gate (so the manual pass cannot overlap with a background pass) and runs
    /// a janitor traversal — orphan deletes plus optional LRU eviction. Manual cleanup is
    /// not threshold-gated: callers expect "drain to target" semantics when they pass a
    /// capacity policy, or "reclaim known orphans" semantics when they pass `None`.
    pub(crate) async fn run_manual<I: CacheIndex>(
        &self,
        cache: &CacheManager<I>,
        capacity: Option<CacheCleanupPolicy>,
    ) -> StorageResult<CacheCleanupReport> {
        let _guard = self.gate.lock().await;
        CacheJanitor::new(cache).cleanup(capacity).await
    }

    /// Drives the background actor loop until either `shutdown` fires or the manager is
    /// dropped. Spawned by [`CacheManager::spawn_cleanup_scheduler`].
    ///
    /// Holds a [`Weak`] reference to the manager so the actor never extends the manager's
    /// lifetime.
    pub(crate) async fn run<I: CacheIndex + 'static>(
        self: Arc<Self>,
        cache: Weak<CacheManager<I>>,
        runtime: CacheRuntimeHandle,
        mut changes: tokio::sync::watch::Receiver<u64>,
        shutdown: CancellationToken,
    ) {
        loop {
            // Read interval off a `cleanup_snapshot` so it is consistent with the policy
            // we will read again after the wake fires — both come from the same ArcSwap
            // load that produced the snapshot.
            let interval = runtime.cleanup_snapshot().interval;

            let trigger = tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = self.notify.notified() => BackgroundTrigger::WriteNudge,
                _ = sleep_or_pending(interval) => BackgroundTrigger::Maintenance,
                changed = changes.changed() => {
                    if changed.is_err() { break; }
                    BackgroundTrigger::Maintenance
                }
            };

            let Some(cache) = cache.upgrade() else { break };

            // Acquire the gate, but stay shutdown-cancellable while waiting.
            //
            // Acquiring the gate also acts as the trigger queue: if a manual cleanup is in
            // flight, this `lock().await` is what keeps the just-consumed trigger from
            // being lost. The trigger event has been consumed from `select!` and our
            // `_guard` will not be released until we have actually run the corresponding
            // pass, so a parallel `run_manual` cannot make our work disappear.
            let guard = tokio::select! {
                _ = shutdown.cancelled() => break,
                guard = self.gate.lock() => guard,
            };

            // Re-read the snapshot under the gate. Hot reloads or further `apply()` calls
            // that arrived while we were waiting must take effect on this pass.
            let snapshot = runtime.cleanup_snapshot();
            self.run_pass(&cache, snapshot, trigger).await;
            drop(guard);
        }
        debug!(
            target: "pg_lakebase_storage::cache",
            "cleanup scheduler exiting",
        );
    }

    async fn run_pass<I: CacheIndex>(
        &self,
        cache: &CacheManager<I>,
        snapshot: CacheCleanupSnapshot,
        trigger: BackgroundTrigger,
    ) {
        let CacheCleanupSnapshot { policy, .. } = snapshot;
        match trigger {
            BackgroundTrigger::WriteNudge => {
                let Some(policy) = policy else {
                    // Cap not configured. Writes have nowhere to evict to; skip the pass.
                    return;
                };
                match cache.logical_cache_usage().await {
                    Ok(usage) if usage.resident_bytes <= policy.start_bytes() => {
                        // Below start watermark — capacity-only pass would no-op anyway.
                        // Orphan reclamation is not driven by writes; periodic / reload /
                        // manual handle it.
                    }
                    Ok(_) => {
                        if let Err(error) =
                            CacheJanitor::new(cache).cleanup_capacity(policy).await
                        {
                            warn!(
                                target: "pg_lakebase_storage::cache",
                                ?trigger,
                                %error,
                                "background capacity pass failed",
                            );
                        }
                    }
                    Err(error) => {
                        warn!(
                            target: "pg_lakebase_storage::cache",
                            %error,
                            "write-nudge skipped: usage probe failed",
                        );
                    }
                }
            }
            BackgroundTrigger::Maintenance => {
                if let Err(error) = CacheJanitor::new(cache).cleanup(policy).await {
                    warn!(
                        target: "pg_lakebase_storage::cache",
                        ?trigger,
                        %error,
                        "background maintenance pass failed",
                    );
                }
            }
        }
    }
}

/// Waits for `interval` if configured, otherwise blocks forever.
///
/// Used in the actor `select!` so the periodic branch is inert when periodic cleanup is
/// disabled (no `cleanup_interval` set) — the other branches still drive the loop.
async fn sleep_or_pending(interval: Option<Duration>) {
    match interval {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
mod tests {
    //! Direct tests for the scheduler's actor semantics.
    //!
    //! Coverage:
    //! - `nudge` only fires capacity-only work and only when over start_bytes.
    //! - `nudge` is a no-op when capacity is unconfigured (writes alone do not drive
    //!   orphan reclamation).
    //! - Periodic / config-reload trigger orphan reclamation.
    //! - Reload that opens a capacity cap evicts immediately without nudge or tick.
    //! - A nudge that arrives while a manual cleanup holds the gate is not lost — the
    //!   actor still runs capacity-only after the manual pass releases the gate.
    //! - Repeated nudges coalesce.
    //! - Shutdown / manager-drop exit cleanly.
    //! - Manual cleanup with / without capacity policy works.

    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use crate::cache::util::create_parent_dir;
    use crate::cache::{
        CacheCleanupPolicy, CacheIndex, CacheManager, CachedObjectMeta,
        InMemoryCacheIndex,
    };
    use crate::config::{
        CacheCleanupConfig, CacheRuntimeConfig, StorageRuntime, StorageRuntimeConfig,
    };
    use crate::object::{ObjectInfo, ObjectLocation};

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    fn test_cache_dir() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        PathBuf::from("/tmp").join(format!(
            "pg-lakebase-storage-cleanup-scheduler-test-{}-{stamp}-{id}",
            std::process::id()
        ))
    }

    /// Wait for `predicate` to hold, polling every 5ms up to 2s.
    async fn wait_until<F: FnMut() -> bool>(label: &str, mut predicate: F) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !predicate() {
            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for: {label}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Plant an unclaimed complete file under `cache`'s root and register it as an orphan
    /// candidate so a janitor pass would observably delete it.
    async fn plant_orphan(cache: &CacheManager<InMemoryCacheIndex>) -> PathBuf {
        cache.prepare_dirs().await.unwrap();
        let key = ObjectLocation::new("default", "bucket", "orphan").unwrap();
        let path = cache.complete_path(&key).unwrap();
        create_parent_dir(&path).await.unwrap();
        tokio::fs::write(&path, b"orphan").await.unwrap();
        cache.orphan_candidates.record_file_candidate(path.clone());
        path
    }

    /// Plant an evictable complete-file row + payload so a capacity pass has something to
    /// reclaim. Returns the on-disk path.
    async fn plant_capacity_victim(
        cache: &CacheManager<InMemoryCacheIndex>,
    ) -> PathBuf {
        cache.prepare_dirs().await.unwrap();
        let key = ObjectLocation::new("default", "bucket", "victim").unwrap();
        let meta = CachedObjectMeta::complete(
            key.clone(),
            ObjectInfo {
                size: 8,
                etag: None,
            },
        );
        cache.index().put_new_complete(meta).await.unwrap();
        let path = cache.complete_path(&key).unwrap();
        create_parent_dir(&path).await.unwrap();
        tokio::fs::write(&path, b"abcdefgh").await.unwrap();
        path
    }

    /// Aggressive capacity policy (`max=1`, `start_ratio=0.5` → `start_bytes=0`,
    /// `target_ratio=0` → `target_bytes=0`). Used by tests that need any non-zero residency
    /// to be over the watermark.
    fn aggressive_capacity_config() -> StorageRuntimeConfig {
        StorageRuntimeConfig {
            cache: CacheRuntimeConfig {
                cleanup: CacheCleanupConfig::default()
                    .with_max_cache_bytes(1)
                    .with_thresholds(50, 0),
                ..CacheRuntimeConfig::default()
            },
        }
    }

    #[tokio::test]
    async fn nudge_is_noop_when_capacity_is_unconfigured() {
        // No cleanup_interval, no max_cache_bytes — writes nudge into nothing.
        let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime,
        ));
        // Plant an orphan and a victim — neither should be touched by a write nudge.
        let orphan = plant_orphan(&cache).await;
        let victim = plant_capacity_victim(&cache).await;

        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());

        for _ in 0..10 {
            cache.nudge_cleanup_after_write();
        }
        // Give the actor time to drain.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(
            orphan.exists(),
            "write nudges must not drive orphan reclamation"
        );
        assert!(victim.exists(), "no capacity cap → write nudges no-op");

        shutdown.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn nudge_is_noop_when_below_start_watermark() {
        // Capacity configured, but cache is empty / well below start_bytes.
        let cfg = StorageRuntimeConfig {
            cache: CacheRuntimeConfig {
                cleanup: CacheCleanupConfig::default()
                    .with_max_cache_bytes(1024 * 1024)
                    .with_thresholds(90, 80),
                ..CacheRuntimeConfig::default()
            },
        };
        let runtime = StorageRuntime::new(cfg).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime,
        ));
        let orphan = plant_orphan(&cache).await;

        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());

        cache.nudge_cleanup_after_write();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            orphan.exists(),
            "below-watermark write nudge must not drive orphan reclamation"
        );

        shutdown.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn nudge_evicts_when_over_start_watermark() {
        let runtime = StorageRuntime::new(aggressive_capacity_config()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime,
        ));
        let victim = plant_capacity_victim(&cache).await;

        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());
        cache.nudge_cleanup_after_write();
        wait_until("victim evicted by over-watermark nudge", || {
            !victim.exists()
        })
        .await;

        shutdown.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn config_reload_triggers_orphan_pass() {
        let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime.clone(),
        ));
        let orphan = plant_orphan(&cache).await;

        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());

        let mut new_cfg = StorageRuntimeConfig::default();
        new_cfg.cache.touch_granularity = Duration::from_secs(1);
        let report = runtime.apply(new_cfg).unwrap();
        assert!(report.changed);

        wait_until("orphan reaped after reload", || !orphan.exists()).await;

        shutdown.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn reload_to_open_capacity_cap_evicts_immediately() {
        let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime.clone(),
        ));
        let victim = plant_capacity_victim(&cache).await;

        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());

        runtime.apply(aggressive_capacity_config()).unwrap();

        wait_until("victim evicted after reload opens capacity cap", || {
            !victim.exists()
        })
        .await;

        shutdown.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    /// Regression test for review feedback: a nudge that arrives while a manual cleanup
    /// holds the gate must not be silently dropped. The original `try_lock` design lost
    /// this trigger; the current `lock().await` design preserves it.
    #[tokio::test]
    async fn nudge_during_manual_cleanup_is_processed_after_gate_released() {
        let runtime = StorageRuntime::new(aggressive_capacity_config()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime,
        ));

        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());

        // Hold the gate by running a manual orphan-only cleanup under instrumentation:
        // we simulate "manual is busy" by spawning a long-running manual call. To make
        // the test deterministic without instrumenting the gate directly, we drive the
        // race the other way — install a victim, fire nudge, then immediately race a
        // manual `cleanup(None)`. In either ordering the eventual capacity pass must
        // still run, because the nudge is queued behind the gate, not dropped.
        let victim = plant_capacity_victim(&cache).await;
        let cache_for_manual = cache.clone();
        let manual = tokio::spawn(async move {
            cache_for_manual.cleanup_orphans().await.unwrap();
        });
        cache.nudge_cleanup_after_write();
        manual.await.unwrap();

        wait_until(
            "victim evicted by nudge that was queued behind manual cleanup",
            || !victim.exists(),
        )
        .await;

        shutdown.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn nudges_coalesce_into_a_single_pass_for_a_burst() {
        let runtime = StorageRuntime::new(aggressive_capacity_config()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime,
        ));
        let victim = plant_capacity_victim(&cache).await;
        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());

        for _ in 0..50 {
            cache.nudge_cleanup_after_write();
        }
        wait_until("victim evicted from coalesced nudges", || !victim.exists()).await;

        shutdown.cancel();
        timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_exits_actor_promptly() {
        let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime,
        ));
        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());
        shutdown.cancel();
        timeout(Duration::from_millis(500), handle)
            .await
            .expect("scheduler did not exit within 500ms after shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_manager_exits_actor() {
        let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime.clone(),
        ));
        let shutdown = CancellationToken::new();
        let handle = cache.spawn_cleanup_scheduler(shutdown.clone());
        cache.nudge_cleanup_after_write();
        drop(cache);
        let mut new_cfg = StorageRuntimeConfig::default();
        new_cfg.cache.touch_granularity = Duration::from_secs(2);
        let _ = runtime.apply(new_cfg);
        timeout(Duration::from_secs(1), handle)
            .await
            .expect("scheduler did not exit after manager drop")
            .unwrap();
    }

    #[tokio::test]
    async fn manual_cleanup_with_capacity_policy_evicts() {
        let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime,
        ));
        let victim = plant_capacity_victim(&cache).await;
        let policy = CacheCleanupPolicy {
            max_cache_bytes: 1,
            cleanup_start_ratio: 0.5,
            cleanup_target_ratio: 0.0,
            max_cleanup_batch_items: 16,
            max_cleanup_batch_bytes: 1024,
        };
        let report = cache.cleanup_with_capacity(policy).await.unwrap();
        assert!(report.evicted_objects >= 1);
        assert!(!victim.exists());
    }

    #[tokio::test]
    async fn manual_cleanup_orphans_runs_orphan_pass() {
        let runtime = StorageRuntime::new(StorageRuntimeConfig::default()).unwrap();
        let cache = Arc::new(CacheManager::new(
            test_cache_dir(),
            InMemoryCacheIndex::new(),
            runtime,
        ));
        let orphan = plant_orphan(&cache).await;
        let report = cache.cleanup_orphans().await.unwrap();
        assert_eq!(report.orphan_complete_files_deleted, 1);
        assert!(!orphan.exists());
    }
}
