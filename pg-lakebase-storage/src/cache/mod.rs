//! On-disk object cache: metadata index, large-file fills, small-object KV, eviction, and recovery.
//!
//! The public entry point is [`CacheManager`]. Submodules are organized by concern:
//!
//! - [`index`]   — [`CacheIndex`] trait and implementations (in-memory, redb-persistent).
//! - [`path`]    — Deterministic on-disk path resolution for cache entries.
//! - [`store`]   — Physical cache store abstraction (file-based and small-object).
//! - [`manager`] — [`CacheManager`] struct, constructors, and core orchestration methods.
//! - `chunks`    — Large-object fill sessions (leader/follower, partial files, reaper).
//! - `admission` — Cache admission logic (open-hit, small-object, complete-file).
//! - `eviction`  — LRU-based capacity eviction and invalidation.
//! - `janitor`   — Periodic and on-demand cleanup coordination.
//! - `startup`   — Recovery and reconciliation at server boot.

mod admission;
mod chunks;
mod establish;
mod eviction;
pub mod index;
mod inventory;
mod janitor;
mod manager;
mod meta;
mod object_state;
pub mod path;
mod startup;
pub mod store;
mod types;
mod usage;
mod util;

pub(crate) use chunks::{ChunkFillClaim, LargeFillSession};
pub(crate) use establish::EstablishLeader;
pub use index::{
    AdmitSmallOutcome, CacheIndex, InMemoryCacheIndex, LruScanCursor, LruScanPage, MetaScanCursor, MetaScanPage,
    OpenHit, RedbCacheIndex, SmallCacheEntry, SmallScanCursor, SmallScanPage,
};
pub use manager::CacheManager;
pub use meta::{CacheState, CachedObjectMeta};
pub(crate) use object_state::CacheActivityGuard;
pub use path::{CacheFileKind, CachePathResolver};
pub use store::{
    CacheStore, CacheStoreKind, DeleteReport, PhysicalCacheEntry, PhysicalCacheEntryVisitor, PhysicalCacheId,
    PhysicalCacheStat, ScanControl,
};
pub use types::{
    BestEffortInvalidateOutcome, CacheCleanupPolicy, CacheCleanupReport, CacheDeleteReason, CacheInvalidateReport,
    CachePurgeReport, CacheRecoveryReport,
};
pub(crate) use types::{CacheEvictionOutcome, OpenOutcome, Residency, ResidencyBody, ResidencyStateHint};
pub use usage::{CacheUsageSnapshot, LogicalCacheUsage, PhysicalCacheUsage};
pub(crate) use util::{create_parent_dir, now_ns, should_touch};

#[cfg(test)]
mod tests;
