//! Value types exchanged with [`crate::cache::CacheManager`].
//!
//! Reports, policies, and OPEN/READ residency envelopes live here so `mod.rs` stays focused on the
//! [`CacheManager`] definition, its lifecycle invariants, and its construction wiring.
//!
//! # Residency and the three cache-design invariants
//!
//! A [`Residency`] is the authoritative product of `OPEN`: once it exists, the data required to
//! serve every subsequent `READ` on the same handle is already in memory (for SmallKV), already
//! mapped to a stable on-disk path via immutable metadata (for CompleteFile), or already anchored
//! to a live fill session (for LargeFill). `READ` never needs to touch the KV again.
//!
//! 1. `size`/`etag` (and the entire [`CachedObjectMeta`]) observed at `OPEN` are a frozen fact for
//!    the current cache lifecycle. The [`CacheActivityGuard`] embedded in every `Residency` keeps
//!    that lifecycle alive; invalidation and capacity eviction refuse to delete an active key.
//!    Because the server never reconciles the backend, the meta snapshot carried by the
//!    `Residency` remains valid for the entire handle lifetime.
//! 2. There is no generation. A single `ObjectLocation` resolves to exactly one `Residency`
//!    variant at `OPEN` time; cross-variant transitions (for example `LargeFill → Complete`)
//!    happen *inside* the same variant payload (see [`LargeFillSession::complete_meta`]) without
//!    creating a second residency.
//! 3. External [`crate::cache::CacheManager::invalidate_object_cache`] is the only freshness
//!    boundary. It waits for every `Residency` (and therefore every activity guard) to drop
//!    before retiring the cache lifecycle, so a handle can keep reading its observed snapshot
//!    without re-validating against the index.

use std::sync::Arc;
use std::time::Duration;

use super::chunks::LargeFillSession;
use super::establish::{EstablishLeader, EstablishWaiter};
use super::meta::CachedObjectMeta;
use super::object_state::CacheActivityGuard;
use super::usage::{LogicalCacheUsage, PhysicalCacheUsage};

pub const DEFAULT_CACHE_CLEANUP_START_PERCENT: u8 = 90;
pub const DEFAULT_CACHE_CLEANUP_TARGET_PERCENT: u8 = 80;
pub const DEFAULT_CACHE_CLEANUP_BATCH_ITEMS: usize = 256;
pub const DEFAULT_CACHE_CLEANUP_BATCH_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Why cached payload deletion ran (capacity LRU, explicit invalidation, startup repair, etc.).
///
/// This is diagnostic-only today but anchors eviction versus integrity paths when reading logs or extending metrics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheDeleteReason {
    Capacity,
    RecoveryInvalid,
    Manual,
}

/// Result of comparing an LRU snapshot to live metadata before eviction (`evict_meta_if_current`).
///
/// `Changed` / `AlreadyGone` mean cleanup must refresh logical usage from the index rather than
/// blindly decrementing by the snapshot's `cached_bytes`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CacheEvictionOutcome {
    Evicted { bytes: u64 },
    Active,
    Changed,
    AlreadyGone,
    NotResident,
}

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct CacheRecoveryReport {
    pub objects_seen: usize,
    /// Orphaned complete files removed by the startup recovery scan.
    pub orphan_complete_files: usize,
    /// Orphaned partial files removed by the startup recovery scan. Every partial file is
    /// orphan at startup (there are no live fill sessions yet), so this counts every `.part`
    /// file discovered under the unified cache directory.
    pub orphan_partial_files: usize,
    pub logical_usage_after: LogicalCacheUsage,
    /// Physical payload usage discovered during the startup physical scan before orphan deletion.
    pub physical_usage_before: PhysicalCacheUsage,
}

/// Tunables for capacity and orphan cleanup (thresholds, batch caps).
///
/// `start_bytes` / `target_bytes` derive from ratios against [`Self::max_cache_bytes`] so periodic or
/// write-triggered cleanup can stop early once usage sits below the start watermark again.
///
/// Orphan cleanup is always performed unconditionally and is not a tunable.
#[derive(Clone, Copy, Debug)]
pub struct CacheCleanupPolicy {
    pub max_cache_bytes: u64,
    pub cleanup_start_ratio: f64,
    pub cleanup_target_ratio: f64,
    pub max_cleanup_batch_items: usize,
    pub max_cleanup_batch_bytes: u64,
}

impl CacheCleanupPolicy {
    pub fn new(max_cache_bytes: u64) -> Self {
        Self {
            max_cache_bytes,
            cleanup_start_ratio: DEFAULT_CACHE_CLEANUP_START_PERCENT as f64 / 100.0,
            cleanup_target_ratio: DEFAULT_CACHE_CLEANUP_TARGET_PERCENT as f64 / 100.0,
            max_cleanup_batch_items: DEFAULT_CACHE_CLEANUP_BATCH_ITEMS,
            max_cleanup_batch_bytes: DEFAULT_CACHE_CLEANUP_BATCH_BYTES,
        }
    }

    pub(crate) fn start_bytes(&self) -> u64 {
        ratio_bytes(self.max_cache_bytes, self.cleanup_start_ratio)
    }

    pub(crate) fn target_bytes(&self) -> u64 {
        ratio_bytes(self.max_cache_bytes, self.cleanup_target_ratio)
    }
}

#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct CacheCleanupReport {
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub orphan_complete_files_deleted: usize,
    pub orphan_partial_files_deleted: usize,
    pub active_objects_skipped: usize,
    pub evicted_objects: usize,
    pub bytes_evicted: u64,
}

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub struct CachePurgeReport {
    pub objects_removed: usize,
    pub bytes_removed: u64,
}

#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub struct CacheInvalidateReport {
    pub removed: bool,
    pub bytes_removed: u64,
}

/// Outcome of [`crate::cache::CacheManager::invalidate_object_cache_best_effort`], returned for
/// observability only. The caller never branches on this value: the contract is that the cache
/// is given a chance to drop the entry, and the result records what actually happened.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BestEffortInvalidateOutcome {
    /// The cache entry was removed.
    Removed { bytes: u64 },
    /// No cache entry was present for the key.
    NotPresent,
    /// The entry could not be removed right now (active readers / fill in progress, or a
    /// transient cache I/O error). The janitor and large-fill reaper will eventually reclaim
    /// the entry; the caller should not retry.
    Skipped,
}

/// Self-contained cache residency produced by `OPEN` and carried for the handle's lifetime.
///
/// `Residency` is the only thing `READ` needs to consult: the embedded [`CacheActivityGuard`] keeps
/// the cache lifecycle alive, the variant payload carries every byte or stable file path required
/// to serve reads without hitting the KV again, and the frozen `meta` / `session` lives as long
/// as this value does. Dropping it releases the lease and allows invalidation / eviction to
/// proceed.
pub(crate) struct Residency {
    /// Held purely for its [`Drop`] effect: keeps `is_active(key)` true for as long as any
    /// handle (or in-flight READ, via a cloned `Arc<Residency>`) still owns this value, so
    /// eviction and invalidation stay out. Never accessed directly; `#[allow(dead_code)]` is
    /// the explicit signal that this field is load-bearing RAII state rather than an unused
    /// remnant.
    #[allow(dead_code)]
    pub(crate) lease: CacheActivityGuard,
    pub(crate) body: ResidencyBody,
}

impl Residency {
    pub(crate) fn size(&self) -> u64 {
        match &self.body {
            ResidencyBody::Small { meta, .. } | ResidencyBody::Complete { meta } => {
                meta.size()
            }
            ResidencyBody::LargeFill { session } => session.info().size,
        }
    }

    pub(crate) fn state_hint(&self) -> ResidencyStateHint {
        match &self.body {
            ResidencyBody::Small { .. } => ResidencyStateHint::SmallKv,
            ResidencyBody::Complete { .. } => ResidencyStateHint::CompleteFile,
            ResidencyBody::LargeFill { .. } => ResidencyStateHint::LargeFill,
        }
    }

    #[cfg(test)]
    pub(crate) fn large_fill_session(&self) -> Option<Arc<LargeFillSession>> {
        match &self.body {
            ResidencyBody::LargeFill { session } => Some(session.clone()),
            _ => None,
        }
    }
}

/// Payload of a [`Residency`] — carries whatever `READ` needs to serve bytes with zero KV calls.
pub(crate) enum ResidencyBody {
    Small {
        meta: CachedObjectMeta,
        payload: Arc<[u8]>,
    },
    Complete {
        meta: CachedObjectMeta,
    },
    LargeFill {
        session: Arc<LargeFillSession>,
    },
}

/// Externally observable classification of a [`Residency`] — the tests and a few log sites care,
/// no one else should.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResidencyStateHint {
    SmallKv,
    CompleteFile,
    LargeFill,
}

/// Result of `CacheManager::lookup_for_open`.
///
/// Concurrent OPENs on the same missing key are serialized through a per-object
/// single-flight ([`super::establish`]): at most one caller is returned the `Establish`
/// variant and is expected to drive HEAD + admit; every other concurrent caller is returned
/// the `Waiting` variant and must await the leader's outcome before re-entering the lookup
/// loop. A `Hit` carries a finished [`Residency`] that `OPEN` can attach straight to the
/// handle.
pub(crate) enum OpenOutcome {
    Hit(Residency),
    /// The caller is the elected establishment leader. It owns the HEAD + GET + admit path
    /// for this miss; see [`EstablishLeader`] for the finalization contract.
    Establish(EstablishLeader),
    /// The caller joined an in-progress establishment. Awaits the leader's outcome through
    /// [`EstablishWaiter::wait`]; on success the caller should retry `lookup_for_open` and
    /// expect a hit, on failure the leader's error is surfaced unchanged.
    Waiting(EstablishWaiter),
}

fn ratio_bytes(bytes: u64, ratio: f64) -> u64 {
    if !ratio.is_finite() || ratio <= 0.0 {
        return 0;
    }
    ((bytes as f64) * ratio).floor() as u64
}
