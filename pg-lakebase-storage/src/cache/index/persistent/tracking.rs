use std::sync::atomic::{AtomicU64, Ordering};

use super::super::LogicalCacheUsage;
use crate::cache::meta::CachedObjectMeta;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct TrackingDelta {
    pub(super) old_bytes: u64,
    pub(super) new_bytes: u64,
}

impl TrackingDelta {
    pub(super) fn from_metas(old: Option<&CachedObjectMeta>, new: Option<&CachedObjectMeta>) -> Self {
        Self {
            old_bytes: resident_bytes(old),
            new_bytes: resident_bytes(new),
        }
    }

    pub(super) fn is_empty(self) -> bool {
        self.old_bytes == self.new_bytes
    }
}

/// Runtime resident-byte mirror updated only after successful persistent index commits.
///
/// The persistent KV serializes write transactions, but without a dedicated worker the task
/// that commits first is not guaranteed to update this mirror first. Deltas are
/// therefore accumulated as commutative additions to separate positive and
/// negative buckets instead of applying `total = total - old + new` directly.
///
/// The mirror is eventually consistent under concurrent persistent writes: a usage
/// read can briefly observe only part of the committed delta stream, but once
/// all completed write tasks have applied their deltas the value converges.
#[derive(Default)]
pub(super) struct RuntimeCacheTracking {
    base_bytes: AtomicU64,
    added_bytes: AtomicU64,
    removed_bytes: AtomicU64,
}

impl RuntimeCacheTracking {
    pub(super) fn logical_usage(&self) -> LogicalCacheUsage {
        let resident_bytes = self
            .base_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.added_bytes.load(Ordering::Relaxed))
            .saturating_sub(self.removed_bytes.load(Ordering::Relaxed));
        LogicalCacheUsage::resident(resident_bytes)
    }

    pub(super) fn apply_delta(&self, delta: TrackingDelta) {
        if delta.is_empty() {
            return;
        }
        if delta.new_bytes >= delta.old_bytes {
            fetch_saturating_add(&self.added_bytes, delta.new_bytes - delta.old_bytes);
        } else {
            fetch_saturating_add(&self.removed_bytes, delta.old_bytes - delta.new_bytes);
        }
    }

    pub(super) fn replace_total(&self, total: u64) {
        // Startup reconciliation installs an authoritative total before the
        // cache manager is shared with request handling or cleanup tasks.
        self.base_bytes.store(total, Ordering::Relaxed);
        self.added_bytes.store(0, Ordering::Relaxed);
        self.removed_bytes.store(0, Ordering::Relaxed);
    }
}

fn fetch_saturating_add(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_add(amount)));
}

fn resident_bytes(meta: Option<&CachedObjectMeta>) -> u64 {
    meta.filter(|meta| meta.is_cache_resident())
        .map(|meta| meta.cached_bytes())
        .unwrap_or(0)
}
