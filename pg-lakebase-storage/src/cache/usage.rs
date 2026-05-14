use async_trait::async_trait;

use super::store::{
    CacheStoreKind, PhysicalCacheEntry, PhysicalCacheEntryVisitor, ScanControl,
};
use crate::error::StorageResult;

/// Sum of [`crate::cache::index`] resident-byte counters (`cached_bytes` totals for resident objects).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LogicalCacheUsage {
    pub resident_bytes: u64,
}

impl LogicalCacheUsage {
    pub fn resident(resident_bytes: u64) -> Self {
        Self { resident_bytes }
    }
}

/// Bytes observed by walking on-disk complete files, partial files, and small-object payload rows.
///
/// Compare with [`LogicalCacheUsage`] for drift diagnostics; values are not atomic under concurrent mutations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PhysicalCacheUsage {
    pub complete_file_bytes: u64,
    pub partial_file_bytes: u64,
    pub small_object_bytes: u64,
}

impl PhysicalCacheUsage {
    pub fn total_bytes(&self) -> u64 {
        self.complete_file_bytes
            .saturating_add(self.partial_file_bytes)
            .saturating_add(self.small_object_bytes)
    }

    pub(crate) fn add_entry(&mut self, entry: &PhysicalCacheEntry) {
        match entry.store_kind {
            CacheStoreKind::CompleteFile => {
                self.complete_file_bytes = self
                    .complete_file_bytes
                    .saturating_add(entry.physical_bytes);
            }
            CacheStoreKind::PartialPayload => {
                self.partial_file_bytes =
                    self.partial_file_bytes.saturating_add(entry.physical_bytes);
            }
            CacheStoreKind::SmallObject => {
                self.small_object_bytes =
                    self.small_object_bytes.saturating_add(entry.physical_bytes);
            }
        }
    }
}

/// Point-in-time pairing of logical index totals and physical scan totals from
/// [`crate::cache::CacheManager::scan_usage_snapshot`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheUsageSnapshot {
    pub logical: LogicalCacheUsage,
    pub physical: PhysicalCacheUsage,
}

#[derive(Default)]
pub(crate) struct PhysicalUsageVisitor {
    usage: PhysicalCacheUsage,
}

impl PhysicalUsageVisitor {
    pub(crate) fn usage(self) -> PhysicalCacheUsage {
        self.usage
    }
}

#[async_trait]
impl PhysicalCacheEntryVisitor for PhysicalUsageVisitor {
    async fn visit(
        &mut self,
        entry: PhysicalCacheEntry,
    ) -> StorageResult<ScanControl> {
        self.usage.add_entry(&entry);
        Ok(ScanControl::Continue)
    }
}
