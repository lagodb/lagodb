//! Per-object runtime state shared by the object lock, cache activity leases, and live
//! large-fill sessions.
//!
//! # Why one state per key
//!
//! The cache manager needs three kinds of per-[`ObjectLocation`] runtime bookkeeping:
//!
//! * a mutex that serializes every lookup / admit / write / evict / invalidate critical
//!   section on that key,
//! * a set of activity counters (admission / read / download / promotion / open-lease) that
//!   tell invalidation and eviction whether the key is quiescent,
//! * at most one live [`LargeFillSession`] reference, used to join concurrent OPENs on the
//!   same in-progress download and to drive reaper cleanup for incomplete fills.
//!
//! These three are describing the same thing — "what does the runtime currently know about
//! this object" — so they live together in [`PerObjectState`]. [`ObjectStateRegistry`] is the
//! process-local `ObjectLocation → Weak<PerObjectState>` index that hands out (or creates) the
//! state for a given key.
//!
//! # Lifetime rule
//!
//! The registry stores only a [`Weak<PerObjectState>`]. The state lives **exactly** as long as
//! any of the following holds a strong [`Arc`]:
//!
//! * an outstanding [`ObjectLockGuard`] (one per live object-lock holder),
//! * an outstanding [`CacheActivityGuard`] (one per live admission / read / download / promotion /
//!   open lease),
//! * a live [`LargeFillSession`] (one session per key while any Arc survives),
//! * a pending [`super::chunks::ReapRequest`] waiting to be processed by the reaper.
//!
//! Each of the four kinds above embeds an `Arc<PerObjectState>` for exactly this reason. When
//! all of them drop, the `Weak` in the registry becomes stale and is quietly overwritten on the
//! next `get_or_create` call for the same key. There is no separate sweep / reaper on the map
//! itself — the [`Weak`] is the sweep.
//!
//! This is the "load-bearing lifetime" contract that the reap path in particular depends on:
//! the nonce-based identity check on reap would otherwise race against a state that had already
//! been recreated by a new OPEN. The `Arc<PerObjectState>` carried by [`super::chunks::ReapRequest`]
//! pins the same state instance the session was attached to, so the reaper always sees the slot
//! it actually owned.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use super::chunks::{LargeFillSession, ReaperHandle, ReaperInbox, reaper_channel};
use super::establish::{
    EstablishFlight, EstablishLeader, EstablishRole, EstablishWaiter, FlightClaim,
    claim_or_join,
};
use crate::error::StorageResult;
use crate::object::{ObjectInfo, ObjectLocation};

/// Which kind of cache activity a [`CacheActivityGuard`] represents.
///
/// The classification is diagnostic-only today — [`PerObjectState::is_active`] returns `true`
/// whenever **any** counter is non-zero — but keeping the variants separate makes logs /
/// metrics / future per-kind policies cheap to add without revisiting this module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheActivityKind {
    Admission,
    Read,
    Download,
    Promotion,
    OpenLease,
}

/// Per-object runtime state. See the module-level docs for the lifetime rule.
///
/// The inner mutexes are independent; a caller ever takes at most one at a time:
///
/// * `lock` — the object-lock [`AsyncMutex`], held across awaits;
/// * `activity` — a synchronous counters mutex, released before any await;
/// * `fill_slot` — a synchronous slot for the live fill session, released before the session is
///   handed out;
/// * `establish_slot` — a synchronous slot for the single-flight that coordinates concurrent
///   miss → residency establishment on this key (see [`super::establish`]).
pub(crate) struct PerObjectState {
    key: ObjectLocation,
    lock: Arc<AsyncMutex<()>>,
    activity: Mutex<ActivityCounters>,
    fill_slot: Mutex<LargeFillSlot>,
    establish_slot: Mutex<Weak<EstablishFlight>>,
}

#[derive(Default)]
struct ActivityCounters {
    admissions: usize,
    reads: usize,
    downloads: usize,
    promotions: usize,
    open_leases: usize,
}

/// At most one in-progress fill session per key. The `Weak` goes stale automatically when the
/// last [`Arc<LargeFillSession>`] drops; `nonce` is the identity that the reaper and
/// `commit_large_fill_unlocked` use to refuse clobbering a replacement session.
#[derive(Default)]
struct LargeFillSlot {
    session: Weak<LargeFillSession>,
    nonce: u64,
}

impl PerObjectState {
    fn new(key: ObjectLocation) -> Self {
        Self {
            key,
            lock: Arc::new(AsyncMutex::new(())),
            activity: Mutex::new(ActivityCounters::default()),
            fill_slot: Mutex::new(LargeFillSlot::default()),
            establish_slot: Mutex::new(Weak::new()),
        }
    }

    pub(crate) fn key(&self) -> &ObjectLocation {
        &self.key
    }

    /// Acquires the per-object async lock. The returned guard embeds an `Arc<Self>` so the
    /// state (and therefore the underlying [`AsyncMutex`]) cannot be recycled while any
    /// holder or waiter exists.
    pub(crate) async fn lock(self: &Arc<Self>) -> ObjectLockGuard {
        let lock = self.lock.clone();
        let inner = lock.lock_owned().await;
        ObjectLockGuard {
            _state: self.clone(),
            _inner: inner,
        }
    }

    /// Increments the activity counter for `kind`, returning a guard that decrements on drop.
    pub(crate) fn activity_guard(
        self: &Arc<Self>,
        kind: CacheActivityKind,
    ) -> CacheActivityGuard {
        self.lock_activity().increment(kind);
        CacheActivityGuard {
            state: self.clone(),
            kind,
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.lock_activity().is_active()
    }

    fn lock_activity(&self) -> MutexGuard<'_, ActivityCounters> {
        // Activity counters gate invalidation and eviction; a poisoned mutex here means we can
        // no longer trust whether concurrent I/O is still in flight on this key, so fail fast
        // rather than silently skip the check.
        self.activity
            .lock()
            .expect("critical per-object activity mutex poisoned; cache activity invariants are no longer trustworthy")
    }

    fn lock_fill_slot(&self) -> MutexGuard<'_, LargeFillSlot> {
        // The fill-slot mutex mediates the single-version invariant: taking it in a poisoned
        // state could let a second session register for a key that already has a live one.
        self.fill_slot
            .lock()
            .expect("critical per-object fill-slot mutex poisoned; large-fill registry state is no longer trustworthy")
    }

    fn lock_establish_slot(&self) -> MutexGuard<'_, Weak<EstablishFlight>> {
        // The establish-slot mutex mediates the single-flight invariant for miss → residency
        // establishment: taking it in a poisoned state could let two concurrent OPENs both
        // believe they are the leader and each fire a backend HEAD, defeating the whole
        // single-flight.
        self.establish_slot.lock().expect(
            "critical per-object establish-slot mutex poisoned; residency establishment is no longer trustworthy",
        )
    }

    /// Claim the establishment role for this key under the per-object lock.
    ///
    /// Callers must hold the [`ObjectLockGuard`] for `self` across the election so every
    /// concurrent `lookup_for_open` observes a consistent slot state — at most one caller
    /// sees an empty slot and is returned as the leader; every other caller joins as a
    /// follower on the same [`super::establish::EstablishFlight`].
    ///
    /// The two inner mutexes (`establish_slot`, then `activity`) are taken sequentially,
    /// never overlapping, so this method preserves the per-object "at most one synchronous
    /// mutex held at a time" invariant documented on [`PerObjectState`]. The admission
    /// guard for a fresh leader is minted **after** the slot lock is released; the outer
    /// async [`ObjectLockGuard`] keeps eviction and invalidation out during that gap.
    pub(crate) fn elect_or_join_establish(self: &Arc<Self>) -> EstablishRole {
        let claim = {
            let mut slot = self.lock_establish_slot();
            claim_or_join(&mut slot)
        };
        match claim {
            FlightClaim::Existing(flight) => {
                EstablishRole::Follower(EstablishWaiter::new(flight))
            }
            FlightClaim::Fresh(flight) => {
                let admission = self.activity_guard(CacheActivityKind::Admission);
                EstablishRole::Leader(EstablishLeader::new(
                    self.key.clone(),
                    flight,
                    admission,
                ))
            }
        }
    }

    /// Returns the live fill session, if any. Used by OPEN to decide whether a miss should join
    /// a session instead of starting a new one, by orphan cleanup, and by invalidate.
    pub(crate) fn live_fill_session(&self) -> Option<Arc<LargeFillSession>> {
        self.lock_fill_slot().session.upgrade()
    }

    /// True iff the slot's nonce still equals `nonce`. Used by the reaper to skip requests whose
    /// session has since been replaced or cleared.
    ///
    /// This intentionally does **not** require the slot's `Weak` to still upgrade: at reap time
    /// the session's last `Arc` has already dropped (it is the very thing that enqueued the
    /// request), so the upgrade would always fail. What matters for identity is that no newer
    /// session has overwritten the slot — which is exactly what the nonce comparison answers.
    pub(crate) fn fill_slot_nonce_matches(&self, nonce: u64) -> bool {
        self.lock_fill_slot().nonce == nonce
    }

    /// Clears the slot only if the stored nonce still equals `nonce`. Called from the commit
    /// path and the reaper, both under the object lock.
    pub(crate) fn clear_fill_slot_if_matches(&self, nonce: u64) {
        let mut slot = self.lock_fill_slot();
        if slot.nonce == nonce {
            *slot = LargeFillSlot::default();
        }
    }

    /// Unconditionally clears the slot. Used by [`crate::cache::CacheManager::invalidate_object_cache`]
    /// after it has already taken the object lock, verified no activity, aborted the session (if
    /// any) and deleted the partial — invalidation is the externally-driven "this cache
    /// lifecycle is over" boundary, so there is no safer identity to gate on.
    pub(crate) fn clear_fill_slot(&self) {
        *self.lock_fill_slot() = LargeFillSlot::default();
    }

    /// Test-only: directly bind `session` / `nonce` into the fill slot. Production code installs
    /// the slot inside [`ObjectStateRegistry::attach_or_join_fill_session`] under the same
    /// fill-slot mutex; this helper exists so unit tests for the reap-pin lifetime rule can build
    /// a session that mirrors the production install without going through the registry.
    #[cfg(test)]
    pub(crate) fn install_fill_slot_for_test(
        &self,
        session: &Arc<LargeFillSession>,
        nonce: u64,
    ) {
        *self.lock_fill_slot() = LargeFillSlot {
            session: Arc::downgrade(session),
            nonce,
        };
    }
}

impl ActivityCounters {
    fn increment(&mut self, kind: CacheActivityKind) {
        *self.counter_mut(kind) += 1;
    }

    fn decrement(&mut self, kind: CacheActivityKind) {
        let counter = self.counter_mut(kind);
        *counter = counter.saturating_sub(1);
    }

    fn counter_mut(&mut self, kind: CacheActivityKind) -> &mut usize {
        match kind {
            CacheActivityKind::Admission => &mut self.admissions,
            CacheActivityKind::Read => &mut self.reads,
            CacheActivityKind::Download => &mut self.downloads,
            CacheActivityKind::Promotion => &mut self.promotions,
            CacheActivityKind::OpenLease => &mut self.open_leases,
        }
    }

    fn is_active(&self) -> bool {
        self.admissions > 0
            || self.reads > 0
            || self.downloads > 0
            || self.promotions > 0
            || self.open_leases > 0
    }
}

/// RAII guard for the per-object async lock.
///
/// The `_state` field is load-bearing: it keeps the [`PerObjectState`] alive — and therefore
/// the underlying [`AsyncMutex`] identity stable — for every concurrent locker. Without it, a
/// later locker could observe a freshly recreated state with a different mutex and split the
/// critical section.
pub(crate) struct ObjectLockGuard {
    #[allow(dead_code)]
    _state: Arc<PerObjectState>,
    _inner: OwnedMutexGuard<()>,
}

/// RAII activity counter increment. Drops decrement the same counter on the same state.
///
/// The embedded `state` field is load-bearing for the same reason as [`ObjectLockGuard::_state`]:
/// it keeps `is_active(key)` observable to anyone else asking about the same state instance.
pub struct CacheActivityGuard {
    state: Arc<PerObjectState>,
    kind: CacheActivityKind,
}

impl Drop for CacheActivityGuard {
    fn drop(&mut self) {
        self.state.lock_activity().decrement(self.kind);
    }
}

/// Process-local `ObjectLocation → Weak<PerObjectState>` index.
///
/// Also owns the nonce source for fill sessions and the [`ReaperHandle`] that every new
/// [`LargeFillSession`] is initialised with. The [`ReaperInbox`] is handed out exactly once to
/// [`crate::cache::CacheManager::spawn_large_fill_reaper`].
pub(crate) struct ObjectStateRegistry {
    entries: Mutex<HashMap<ObjectLocation, Weak<PerObjectState>>>,
    reaper: ReaperHandle,
    next_nonce: AtomicU64,
}

impl ObjectStateRegistry {
    /// Threshold at which `get_or_create` opportunistically retains only live entries. Stale
    /// `Weak`s in the map are harmless for correctness but unbounded growth under adversarial
    /// one-shot access patterns is not, so the sweep kicks in when the map passes this size.
    const CLEANUP_THRESHOLD: usize = 4096;

    pub(crate) fn new() -> (Self, ReaperInbox) {
        let (reaper, inbox) = reaper_channel();
        let registry = Self {
            entries: Mutex::new(HashMap::new()),
            reaper,
            next_nonce: AtomicU64::new(1),
        };
        (registry, inbox)
    }

    fn lock_entries(
        &self,
    ) -> MutexGuard<'_, HashMap<ObjectLocation, Weak<PerObjectState>>> {
        // The registry map is a pure lookup index; losing it means `get_or_create` might hand
        // out a second `PerObjectState` for a key that already has waiters on the first, which
        // would split every critical section. Fail fast.
        self.entries
            .lock()
            .expect("critical object state registry mutex poisoned; per-object lock identity is no longer trustworthy")
    }

    /// Returns the [`PerObjectState`] for `key`, creating one if no live state exists.
    pub(crate) fn get_or_create(&self, key: &ObjectLocation) -> Arc<PerObjectState> {
        let mut entries = self.lock_entries();
        if entries.len() > Self::CLEANUP_THRESHOLD {
            entries.retain(|_, weak| weak.strong_count() > 0);
        }
        if let Some(weak) = entries.get(key) {
            if let Some(state) = weak.upgrade() {
                return state;
            }
        }
        let state = Arc::new(PerObjectState::new(key.clone()));
        entries.insert(key.clone(), Arc::downgrade(&state));
        state
    }

    /// Returns the live [`PerObjectState`] for `key`, or `None` if no consumer currently holds
    /// one. Unlike [`Self::get_or_create`], this never inserts a fresh entry — it is the
    /// cheap read-only probe callers use when they only care about "is anything happening
    /// right now".
    pub(crate) fn get_existing(
        &self,
        key: &ObjectLocation,
    ) -> Option<Arc<PerObjectState>> {
        self.lock_entries().get(key).and_then(Weak::upgrade)
    }

    /// Attaches (or joins) a large-fill session on `state`. Exactly one session exists per key
    /// at any time: a caller that races in while a live session still occupies the slot joins
    /// that session and its `info` / `partial_path` arguments are discarded, matching the
    /// "size/etag frozen for the cache lifecycle" invariant.
    ///
    /// Returns an error if the existing session has aborted (callers should treat this as a
    /// transient failure and retry after the reap path has cleaned up).
    pub(crate) async fn attach_or_join_fill_session(
        &self,
        state: &Arc<PerObjectState>,
        info: ObjectInfo,
        chunks: usize,
        partial_path: PathBuf,
    ) -> StorageResult<Arc<LargeFillSession>> {
        // The slot mutex cannot be held across `ensure_openable`'s await, so the block below
        // either mints a brand-new session (trivially openable) or releases the lock and hands
        // out the existing `Arc` for the post-lock openability check.
        let existing = {
            let mut slot = state.lock_fill_slot();
            if let Some(session) = slot.session.upgrade() {
                session
            } else {
                let nonce = self.next_nonce.fetch_add(1, Ordering::Relaxed);
                let session = Arc::new(LargeFillSession::new(
                    state.clone(),
                    info,
                    chunks,
                    partial_path,
                    nonce,
                    self.reaper.clone(),
                ));
                *slot = LargeFillSlot {
                    session: Arc::downgrade(&session),
                    nonce,
                };
                return Ok(session);
            }
        };
        existing.ensure_openable().await?;
        Ok(existing)
    }

    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> usize {
        self.lock_entries().len()
    }
}
