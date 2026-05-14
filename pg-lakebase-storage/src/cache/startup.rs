use async_trait::async_trait;
use tracing::debug;

use crate::cache::eviction::OrphanFileDeleted;
use crate::cache::{
    CacheIndex, CacheManager, CacheRecoveryReport, LogicalCacheUsage,
    PhysicalCacheEntry, PhysicalCacheEntryVisitor, PhysicalCacheId,
    PhysicalCacheUsage, ScanControl,
};
use crate::error::StorageResult;

const STARTUP_META_PAGE_SIZE: usize = 1024;

/// Startup-only cache recovery.
///
/// Durable metadata is no longer repaired against file-backed partial state. The
/// index is the ownership table, and extra physical payloads are cache garbage.
pub(crate) struct StartupRecovery<'a, I: CacheIndex> {
    cache: &'a CacheManager<I>,
}

impl<'a, I: CacheIndex> StartupRecovery<'a, I> {
    pub(crate) fn new(cache: &'a CacheManager<I>) -> Self {
        Self { cache }
    }

    pub(crate) async fn recover(&self) -> StorageResult<CacheRecoveryReport> {
        self.cache.prepare_dirs().await?;
        self.cache.clear_orphan_candidates();

        let mut report = CacheRecoveryReport::default();
        let resident_bytes = self.scan_metadata_usage(&mut report).await?;
        report.logical_usage_after = LogicalCacheUsage::resident(resident_bytes);
        self.cache
            .index
            .replace_runtime_cache_usage(report.logical_usage_after)
            .await?;

        let mut orphan_visitor = StartupOrphanVisitor {
            cache: self.cache,
            report: &mut report,
            usage: PhysicalCacheUsage::default(),
        };
        self.cache
            .visit_physical_cache_entries(&mut orphan_visitor)
            .await?;
        report.physical_usage_before = orphan_visitor.usage;

        Ok(report)
    }

    async fn scan_metadata_usage(
        &self,
        report: &mut CacheRecoveryReport,
    ) -> StorageResult<u64> {
        let mut resident_bytes = 0_u64;
        let mut cursor = None;
        loop {
            let page = self
                .cache
                .index
                .scan_meta_page(cursor, STARTUP_META_PAGE_SIZE)
                .await?;
            cursor = page.next_cursor;
            for meta in page.metas {
                report.objects_seen += 1;
                resident_bytes = resident_bytes.saturating_add(meta.cached_bytes());
            }
            if cursor.is_none() {
                break;
            }
        }
        Ok(resident_bytes)
    }
}

struct StartupOrphanVisitor<'a, 'r, I: CacheIndex> {
    cache: &'a CacheManager<I>,
    report: &'r mut CacheRecoveryReport,
    usage: PhysicalCacheUsage,
}

#[async_trait]
impl<I: CacheIndex> PhysicalCacheEntryVisitor for StartupOrphanVisitor<'_, '_, I> {
    async fn visit(
        &mut self,
        entry: PhysicalCacheEntry,
    ) -> StorageResult<ScanControl> {
        self.usage.add_entry(&entry);

        match entry.id {
            PhysicalCacheId::Path(path) => match self
                .cache
                .delete_orphan_file_if_unclaimed(path.clone())
                .await?
            {
                Some(OrphanFileDeleted::Complete) => {
                    debug!(path = %path.display(), "startup: deleted orphan complete file");
                    self.report.orphan_complete_files += 1;
                }
                Some(OrphanFileDeleted::Partial) => {
                    debug!(path = %path.display(), "startup: deleted orphan partial file");
                    self.report.orphan_partial_files += 1;
                }
                None => {}
            },
            PhysicalCacheId::SmallObject(_) => {}
        }

        Ok(ScanControl::Continue)
    }
}
