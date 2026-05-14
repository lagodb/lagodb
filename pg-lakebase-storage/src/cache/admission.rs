//! Cache admission paths — `lookup_for_open`, `admit_small`, `admit_large`.
//!
//! These methods decide, under the per-object cache lock, what residency a new `OPEN` gets:
//!
//! * [`CacheManager::lookup_for_open`] — classify a key as hit, single-flight follower, or
//!   newly elected single-flight leader; see [`super::establish`] for the leader/follower
//!   coordination.
//! * [`CacheManager::admit_small`] — atomically publish a small-object residency (after the
//!   elected leader has HEAD-ed and GETed the backend). This is the only production path
//!   that installs [`crate::cache::CacheState::SmallKv`] metadata after startup.
//! * [`CacheManager::admit_large`] — create or join the process-local
//!   [`crate::cache::chunks::LargeFillSession`] for an `OPEN` that did not hit cache.
//!
//! # Cache-design invariants upheld
//!
//! 1. Once admitted, `ObjectInfo` (size/etag) is frozen for the current cache lifecycle. The
//!    single-flight ensures only the leader ever issues a HEAD, so there is never more than
//!    one observed `(size, etag)` per cache lifecycle to reconcile against.
//! 2. There is no generation — one [`crate::object::ObjectLocation`] resolves to at most one
//!    residency (small-KV row, complete file, or live fill session) at a time. Cross-variant
//!    transitions go through commit/promote, not through parallel residencies.
//! 3. The only way to retire a residency is an external
//!    [`CacheManager::invalidate_object_cache`] call (which waits for every activity guard to
//!    drop) or capacity-driven eviction.
//!
//! # Design rationale
//!
//! ## Per-object lock wraps the whole `lookup → admit` decision window
//!
//! [`LockedCacheObject`] holds the object lock [`ObjectLockGuard`] for its entire lifetime and
//! carries the `Arc<PerObjectState>` it was derived from, so every metadata read and
//! activity-guard creation inside the block runs against the same per-object state instance.
//! Releasing the lock between lookup and admission would open a window for eviction or
//! invalidation to retire the observed snapshot before the lease is minted.
//!
//! ## Establishment single-flight serializes concurrent miss → HEAD
//!
//! Concurrent OPENs on the same missing key previously each issued their own HEAD (and, for
//! small objects, their own GET) before `admit_small_if_absent` deduplicated the winning
//! write at the KV layer. That pattern cost one redundant HEAD and N-1 redundant GETs on
//! every cold concurrent open. The single-flight installed in
//! [`crate::cache::PerObjectState::elect_or_join_establish`] collapses those to a single
//! leader-driven HEAD; followers wait for the leader's outcome and re-enter the lookup loop
//! on success. The KV-layer `admit_small_if_absent` is retained as defence-in-depth but is
//! no longer the primary deduplication mechanism.
//!
//! ## `admit_large` still consults `open_hit` once before joining a session
//!
//! The same race window exists for large objects, but the defence is different. A concurrent
//! promoter could have committed a complete-file meta row in the gap between miss observation
//! and admit; a new OPEN that blindly called `attach_or_join_fill_session` would either join a
//! dead session or (worse) trigger a needless re-download. One `open_hit` transaction under the
//! reacquired lock resolves that: if the row is already complete, admit binds the complete-file
//! residency instead of building a new session.

use std::sync::Arc;

use crate::cache::CacheManager;
use crate::cache::establish::{EstablishLeader, EstablishRole};
use crate::cache::index::{AdmitSmallOutcome, CacheIndex, OpenHit};
use crate::cache::manager::duration_to_ns;
use crate::cache::meta::{CacheState, CachedObjectMeta};
use crate::cache::object_state::{
    CacheActivityGuard, CacheActivityKind, ObjectLockGuard, PerObjectState,
};
use crate::cache::types::{OpenOutcome, Residency, ResidencyBody};
use crate::cache::util::now_ns;
use crate::error::StorageResult;
use crate::object::{ObjectInfo, ObjectLocation, chunk_count};

/// Per-object cache lock that also vends the activity guards callers need while holding it.
///
/// Holds the object-lock guard and the `Arc<PerObjectState>` it was derived from, so every
/// activity guard minted through this wrapper attaches to the same state instance — which keeps
/// `is_active(key)` observations consistent for the whole lookup / admit window.
pub(crate) struct LockedCacheObject {
    state: Arc<PerObjectState>,
    _guard: ObjectLockGuard,
}

impl LockedCacheObject {
    pub(crate) fn state(&self) -> &Arc<PerObjectState> {
        &self.state
    }

    fn open_lease(&self) -> CacheActivityGuard {
        self.state.activity_guard(CacheActivityKind::OpenLease)
    }
}

impl<I: CacheIndex> CacheManager<I> {
    pub(crate) async fn lock_cache_object(
        &self,
        key: &ObjectLocation,
    ) -> LockedCacheObject {
        let state = self.object_state(key);
        let guard = state.lock().await;
        LockedCacheObject {
            state,
            _guard: guard,
        }
    }

    /// Classifies an `OPEN`: hit, establishment follower (wait for the leader), or newly
    /// elected establishment leader.
    ///
    /// The election happens under the per-object lock so every concurrent `lookup_for_open`
    /// on the same key observes a consistent slot state — at most one caller sees an empty
    /// slot and is returned as the leader; every other caller joins as a follower on the
    /// same [`crate::cache::establish::EstablishFlight`].
    pub(crate) async fn lookup_for_open(
        &self,
        key: &ObjectLocation,
    ) -> StorageResult<OpenOutcome> {
        let object = self.lock_cache_object(key).await;
        if let Some(hit) = self
            .index
            .open_hit(key, now_ns(), duration_to_ns(self.touch_granularity))
            .await?
        {
            return Ok(OpenOutcome::Hit(residency_from_open_hit(
                hit,
                object.open_lease(),
            )?));
        }
        if let Some(session) = object.state().live_fill_session() {
            session.ensure_openable().await?;
            return Ok(OpenOutcome::Hit(Residency {
                lease: object.open_lease(),
                body: ResidencyBody::LargeFill { session },
            }));
        }
        Ok(match object.state().elect_or_join_establish() {
            EstablishRole::Leader(leader) => OpenOutcome::Establish(leader),
            EstablishRole::Follower(waiter) => OpenOutcome::Waiting(waiter),
        })
    }

    /// Atomically publishes a new small-object residency, or binds the one a concurrent caller
    /// already published.
    ///
    /// Borrows the [`EstablishLeader`] so the caller (OPEN's outer loop) retains control over
    /// the leader's final `succeed` / `fail` signal. The admission-kind activity guard carried
    /// by the leader remains live for the duration of this call; the `OpenLease` guard is
    /// minted inside the same per-object lock window that publishes the residency, so
    /// invalidation / eviction cannot race in between.
    ///
    /// Capacity cleanup is invoked only on the `Admitted` branch — that is the only path that
    /// raises resident byte totals. The `AlreadyPresent` race-loser path adds no bytes and
    /// therefore skips `maybe_cleanup` to keep the fast path free of redundant gate
    /// acquisitions.
    ///
    /// # On the `AlreadyPresent` branch
    ///
    /// Under today's architecture (single-process, per-object establishment single-flight),
    /// `AlreadyPresent` is effectively unreachable on the normal OPEN flow: at most one
    /// leader reaches admit per cache lifecycle. The branch is intentionally retained — not
    /// dead code to be pruned — for two reasons:
    ///
    /// * defence-in-depth against future changes that weaken or bypass the single-flight
    ///   (e.g. a direct admit entry point added for a special pathway),
    /// * cross-process durability: if the redb-backed index is ever opened by a second
    ///   process, the KV transaction's insert-if-absent is the only remaining deduplication
    ///   layer.
    ///
    /// Both reasons justify keeping the race-loser path coherent (no resident-byte delta,
    /// returns the winner's meta/payload verbatim) rather than treating `AlreadyPresent` as
    /// an invariant violation.
    pub(crate) async fn admit_small(
        &self,
        leader: &EstablishLeader,
        data: Vec<u8>,
        info: ObjectInfo,
    ) -> StorageResult<Residency> {
        let key = leader.key();
        let (residency, admitted) = {
            let object = self.lock_cache_object(key).await;
            let meta_template =
                CachedObjectMeta::small(key.clone(), info, data.len() as u64);
            let outcome = self
                .index
                .admit_small_if_absent(meta_template, data, now_ns())
                .await?;
            match outcome {
                AdmitSmallOutcome::Admitted { meta, payload } => (
                    Residency {
                        lease: object.open_lease(),
                        body: ResidencyBody::Small { meta, payload },
                    },
                    true,
                ),
                AdmitSmallOutcome::AlreadyPresent { meta, payload } => (
                    Residency {
                        lease: object.open_lease(),
                        body: ResidencyBody::Small { meta, payload },
                    },
                    false,
                ),
            }
        };
        if admitted {
            self.maybe_cleanup().await?;
        }
        Ok(residency)
    }

    /// Binds a large-fill residency.
    ///
    /// Consults the index under the object lock first: if a concurrent fill already promoted to
    /// a complete-file meta row, the new OPEN binds that row instead of starting a fresh
    /// session. Otherwise joins (or creates) a live [`crate::cache::chunks::LargeFillSession`].
    ///
    /// # Why no `maybe_cleanup` call
    ///
    /// Both return paths here are non-writing from the cache-capacity perspective:
    /// * the "Hit on a racing promote" path observed an already-accounted `Complete` row,
    /// * the "join or create a live session" path stays entirely in process memory until the
    ///   final chunk is written (see
    ///   [`crate::cache::CacheManager::store_large_chunk_for_session`], which calls
    ///   `maybe_cleanup` itself after promotion succeeds).
    ///
    /// Skipping the call here keeps the symmetry "only paths that added resident bytes run the
    /// cleanup gate" — matching the [`Self::admit_small`] rule above.
    pub(crate) async fn admit_large(
        &self,
        leader: &EstablishLeader,
        info: ObjectInfo,
    ) -> StorageResult<Residency> {
        let key = leader.key();
        let object = self.lock_cache_object(key).await;
        // Race-vs-promote guard: `open_hit` fires one read txn (or one write txn if the
        // touch window elapsed) and gives us the already-committed complete-file meta to
        // bind directly.
        if let Some(hit) = self
            .index
            .open_hit(key, now_ns(), duration_to_ns(self.touch_granularity))
            .await?
        {
            return residency_from_open_hit(hit, object.open_lease());
        }
        let chunks = chunk_count(info.size, self.chunk_size) as usize;
        let partial_path = self.partial_path(key)?;
        // `info` is only used for a freshly created session. When an existing live session is
        // joined, its frozen ObjectInfo is authoritative and `info` is discarded; see
        // `ObjectStateRegistry::attach_or_join_fill_session` for the cache-lifecycle invariant.
        let session = self
            .object_states
            .attach_or_join_fill_session(object.state(), info, chunks, partial_path)
            .await?;
        Ok(Residency {
            lease: object.open_lease(),
            body: ResidencyBody::LargeFill { session },
        })
    }

    pub(crate) async fn download_guard(
        &self,
        key: &ObjectLocation,
    ) -> CacheActivityGuard {
        let state = self.object_state(key);
        let _guard = state.lock().await;
        state.activity_guard(CacheActivityKind::Download)
    }

    pub(crate) async fn read_guard(
        &self,
        key: &ObjectLocation,
    ) -> CacheActivityGuard {
        let state = self.object_state(key);
        let _guard = state.lock().await;
        state.activity_guard(CacheActivityKind::Read)
    }

    pub(crate) fn promotion_guard(&self, key: &ObjectLocation) -> CacheActivityGuard {
        self.object_state(key)
            .activity_guard(CacheActivityKind::Promotion)
    }
}

fn residency_from_open_hit(
    hit: OpenHit,
    lease: CacheActivityGuard,
) -> StorageResult<Residency> {
    let OpenHit { meta, payload } = hit;
    let body = match (meta.cache_state(), payload) {
        (CacheState::SmallKv, Some(payload)) => {
            ResidencyBody::Small { meta, payload }
        }
        (CacheState::CompleteFile, None) => ResidencyBody::Complete { meta },
        (CacheState::SmallKv, None) => {
            return Err(crate::error::StorageError::cache(format!(
                "cache index returned SmallKv meta for {} without payload",
                meta.key()
            )));
        }
        (CacheState::CompleteFile, Some(_)) => {
            return Err(crate::error::StorageError::cache(format!(
                "cache index returned CompleteFile meta for {} with unexpected small payload",
                meta.key()
            )));
        }
    };
    Ok(Residency { lease, body })
}
