//! Payload deletion and LRU eviction helpers on [`crate::cache::CacheManager`].
//!
//! **File-backed eviction order:** metadata is removed (and orphan candidates recorded) before unlinking the payload so
//! resident-byte accounting drops immediately; a failed `remove_file` leaves disk ahead of metadata until the next
//! orphan pass.
//!
//! **Small-object eviction:** uses [`crate::cache::index::CacheIndex::delete_meta_and_small`] so KV metadata and binary
//! payload disappear together from persistent indexes.
//!
//! # Orphan ownership proof per file kind
//!
//! Complete and partial cache files live in the same directory tree, distinguished by suffix.
//! [`crate::cache::CachePathResolver::parse_cache_path`] yields `(ObjectLocation, CacheFileKind)`
//! from any candidate path, so [`CacheManager::delete_orphan_file_if_unclaimed`] dispatches the
//! right ownership check under the per-object lock:
//!
//! - **Complete file:** an unclaimed complete file is one whose key has no `CompleteFile`
//!   metadata row pointing at it (see [`Self::current_meta_claims_complete_path`]).
//! - **Partial file:** an unclaimed partial file is one whose key has no live
//!   [`crate::cache::LargeFillSession`] in the per-object state's fill slot.
//!
//! Both branches serialize against chunk writes, promotion, invalidation, and reaper work via
//! the per-object async lock, so the orphan check is consistent with any concurrent critical
//! section on the same key.

use std::path::{Path, PathBuf};

use tracing::{debug, warn};

use crate::cache::meta::{CacheState, CachedObjectMeta};
use crate::cache::path::CacheFileKind;
use crate::cache::store::CacheStore;
use crate::cache::{
    BestEffortInvalidateOutcome, CacheDeleteReason, CacheEvictionOutcome, CacheIndex,
    CacheInvalidateReport, CacheManager, DeleteReport, PhysicalCacheId,
};
use crate::error::{StorageError, StorageErrorKind, StorageResult};
use crate::object::ObjectLocation;

/// Outcome of orphan deletion, kept separate from `Option<()>` so call sites can keep
/// per-kind metrics without re-parsing the path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrphanFileDeleted {
    /// Payload was the complete-file variant and was removed from disk.
    Complete,
    /// Payload was the partial variant and was removed from disk.
    Partial,
}

impl<I: CacheIndex> CacheManager<I> {
    /// Best-effort unlink of a cache payload file plus orphan-candidate bookkeeping.
    ///
    /// **Ordering:** [`crate::cache::inventory::RuntimeOrphanCandidates`] is cleared **before** the
    /// async `remove_file` await. Cleanup passes snapshot this set without taking the per-object
    /// lock; clearing first closes a race where another task observes the file gone (reaper
    /// unlink) while the path is still listed as an orphan candidate, then counts a spurious
    /// janitor delete (see large-fill reaper vs `cleanup()`).
    ///
    /// If unlink fails with an I/O error, the path is registered again so a later pass can retry.
    pub(crate) async fn delete_file_payload(
        &self,
        path: PathBuf,
    ) -> StorageResult<DeleteReport> {
        self.orphan_candidates.clear_file_candidate(&path);
        match self
            .file_cache_store()
            .delete_entry(&PhysicalCacheId::Path(path.clone()))
            .await
        {
            Ok(report) => Ok(report),
            Err(err) => {
                self.orphan_candidates.record_file_candidate(path);
                Err(err)
            }
        }
    }

    /// Unified orphan check for complete and partial cache files.
    ///
    /// `Ok(None)` means the file is still claimed (by metadata for complete files, by a live
    /// fill session for partial files) or the activity counters say the key is busy; the caller
    /// must leave the path on disk for a later pass. `Ok(Some(kind))` means the payload was
    /// unlinked and the caller can update per-kind metrics.
    pub(crate) async fn delete_orphan_file_if_unclaimed(
        &self,
        path: PathBuf,
    ) -> StorageResult<Option<OrphanFileDeleted>> {
        let Some((key, kind)) = self.paths.parse_cache_path(&path) else {
            // Unparseable path — treat as a stray we can always unlink; the kind is unknown so
            // we pick Complete as a neutral label for metrics. Unparseable files are rare in
            // practice (they require someone writing a file into the cache directory with an
            // unknown name) so the exact label is informational only.
            self.delete_file_payload(path).await?;
            return Ok(Some(OrphanFileDeleted::Complete));
        };

        let state = self.object_state(&key);
        let _object_guard = state.lock().await;
        if state.is_active() {
            return Ok(None);
        }
        match kind {
            CacheFileKind::Complete => {
                if self.current_meta_claims_complete_path(&key, &path).await? {
                    self.orphan_candidates.clear_file_candidate(&path);
                    return Ok(None);
                }
                self.delete_file_payload(path).await?;
                Ok(Some(OrphanFileDeleted::Complete))
            }
            CacheFileKind::Partial => {
                if state.live_fill_session().is_some() {
                    return Ok(None);
                }
                self.delete_file_payload(path).await?;
                Ok(Some(OrphanFileDeleted::Partial))
            }
        }
    }

    /// Retires the cache lifecycle for one caller-identified physical object.
    ///
    /// The cache does not discover same-key backend changes. Callers that know
    /// a remote object was replaced or removed invoke this method explicitly.
    /// It rejects active readers or fills with `Busy` instead of invalidating
    /// underneath a live handle.
    pub async fn invalidate_object_cache(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<CacheInvalidateReport> {
        let state = self.object_state(key);
        let _object_guard = state.lock().await;
        if state.is_active() {
            warn!(key = %key, "invalidate_object_cache rejected: object is active");
            return Err(StorageError::busy(format!("cache object is active: {key}")));
        }

        let mut report = CacheInvalidateReport::default();
        if let Some(meta) = self.index.get_meta(key).await? {
            report.removed = true;
            report.bytes_removed =
                report.bytes_removed.saturating_add(meta.cached_bytes());
            self.delete_cached_object_unlocked(meta, CacheDeleteReason::Manual)
                .await?;
        }

        let complete = self.complete_path(key)?;
        if !self
            .current_meta_claims_complete_path(key, &complete)
            .await?
        {
            let complete = self.delete_file_payload(complete).await?;
            report.bytes_removed =
                report.bytes_removed.saturating_add(complete.bytes_deleted);
            report.removed |= complete.bytes_deleted > 0;
        }

        // Regardless of whether a live session exists, the partial file for `key` lives at the
        // deterministic path `self.partial_path(key)`. Aborting the live session (if any) ensures
        // in-flight writers stop and any waiter wakes up before we unlink. Then a single unlink
        // covers both "there is a live session" and "there is only a stray orphan left by a
        // previous session whose reap could not unlink".
        //
        // `live_fill_session` returns an `Arc` upgraded from the fill slot's `Weak`; the upgrade
        // is enough to keep the session alive for the abort/remove window. The Arc we received
        // here is the same state instance the session was attached to, so clearing the slot on
        // `state` is precisely the right slot to clear. When this Arc drops the session's Drop
        // path will still fire, but its reap request carries the same state and its nonce check
        // will see the slot has been cleared, making the reap a no-op.
        if let Some(session) = state.live_fill_session() {
            self.abort_large_fill(&session).await?;
        }
        state.clear_fill_slot();
        let partial = self.delete_file_payload(self.partial_path(key)?).await?;
        report.bytes_removed =
            report.bytes_removed.saturating_add(partial.bytes_deleted);
        report.removed |= partial.bytes_deleted > 0;

        Ok(report)
    }

    /// Best-effort variant of [`Self::invalidate_object_cache`] for callers that
    /// already know the backend object changed. The responsibility for keeping the cache
    /// consistent with the backend belongs to the **caller** (per the cache invariants documented
    /// in `src/cache/README.md`); this helper only makes that explicit invalidation non-fatal.
    ///
    /// Compared to [`Self::invalidate_object_cache`] this method differs only in failure
    /// handling. It never propagates errors:
    ///
    /// * `Busy` — the key has live readers / fills. Skipped; the janitor and large-fill reaper
    ///   will reclaim the entry once activity drains. This is the only common case.
    /// * Any other error — logged at `warn` and swallowed. Cleaning the local cache is a courtesy,
    ///   not a contract; failing the `delete` API on a cache I/O hiccup would be misleading.
    ///
    /// The returned [`BestEffortInvalidateOutcome`] is observability-only.
    pub async fn invalidate_object_cache_best_effort(
        &self,
        key: &ObjectLocation,
    ) -> BestEffortInvalidateOutcome {
        if let Err(error) = self.validate_file_cache_paths(key) {
            warn!(key = %key, error = %error, "skipping best-effort cache invalidation: invalid path");
            return BestEffortInvalidateOutcome::Skipped;
        }
        match self.invalidate_object_cache(key).await {
            Ok(report) if report.removed => BestEffortInvalidateOutcome::Removed {
                bytes: report.bytes_removed,
            },
            Ok(_) => BestEffortInvalidateOutcome::NotPresent,
            Err(error) => match error.kind() {
                StorageErrorKind::Busy => {
                    debug!(key = %key, "best-effort cache invalidation skipped: object is active");
                    BestEffortInvalidateOutcome::Skipped
                }
                _ => {
                    warn!(key = %key, error = %error, "best-effort cache invalidation failed");
                    BestEffortInvalidateOutcome::Skipped
                }
            },
        }
    }

    async fn current_meta_claims_complete_path(
        &self,
        key: &ObjectLocation,
        path: &Path,
    ) -> StorageResult<bool> {
        let Some(meta) = self.index.get_meta(key).await? else {
            return Ok(false);
        };
        Ok(meta.cache_state() == CacheState::CompleteFile
            && self.complete_path(key)?.as_path() == path)
    }

    pub(crate) async fn evict_meta_if_current(
        &self,
        snapshot: CachedObjectMeta,
    ) -> StorageResult<CacheEvictionOutcome> {
        let key = snapshot.key().clone();
        let state = self.object_state(&key);
        let _object_guard = state.lock().await;
        let Some(current) = self.index.get_meta(&key).await? else {
            return Ok(CacheEvictionOutcome::AlreadyGone);
        };
        if current != snapshot {
            return Ok(CacheEvictionOutcome::Changed);
        }
        if !current.is_cache_resident() {
            return Ok(CacheEvictionOutcome::NotResident);
        }
        if state.is_active() {
            return Ok(CacheEvictionOutcome::Active);
        }
        let bytes = current.cached_bytes();
        self.delete_cached_object_unlocked(current, CacheDeleteReason::Capacity)
            .await?;
        debug!(key = %key, bytes, "cache object evicted");
        Ok(CacheEvictionOutcome::Evicted { bytes })
    }

    pub(super) async fn delete_cached_object_unlocked(
        &self,
        meta: CachedObjectMeta,
        _reason: CacheDeleteReason,
    ) -> StorageResult<()> {
        let original_state = meta.cache_state();
        let key = meta.key().clone();

        match original_state {
            CacheState::SmallKv => {
                self.index.delete_meta_and_small(&key).await?;
            }
            CacheState::CompleteFile => {
                let complete = self.complete_path(&key)?;
                // Metadata is removed before the file payload so the cache stops
                // advertising bytes that are being evicted. `delete_file_payload`
                // clears the orphan candidate before the async unlink and
                // re-registers the path on I/O error, so no separate
                // `record_file_candidate` is needed here.
                self.index.delete_meta(&key).await?;
                self.delete_file_payload(complete).await?;
            }
        }

        Ok(())
    }
}
