//! Page and block geometry PostgreSQL sees for Iceberg storage.

use pgrx::pg_sys;

/// Maps Iceberg storage bytes onto PostgreSQL's page and ANALYZE block counts.
///
/// PostgreSQL samples the blocks reported through `relation_size`, scales its
/// live-row estimate by them, stores them as `pg_class.relpages`, and, across
/// an inheritance tree, splits `targrows` between children in proportion.
///
/// PG17 and PG18 use [`Self::pages`] directly. PG15 and PG16 prefetch these synthetic
/// block numbers before the table AM can intercept them, so without an executor
/// fork they must stay in the first physical segment. Its `relpages` is
/// therefore about 1/1024 of the page count, short of rounding and the cap. The
/// planner is unaffected, because `relation_estimate_size` reports
/// [`Self::pages`]. The coarse and capped mapping is valid for a non-inherited
/// scan, whose live-row correction cancels both the ticket and block counts
/// when they come from one snapshot, but it can distort inherited sample shares
/// when different child sizes map to the same block count.
pub(crate) struct AnalyzeBlockDomain;

impl AnalyzeBlockDomain {
    /// PostgreSQL 15 and 16's `acquire_sample_rows()` calls `PrefetchBuffer()` on
    /// sampled blocks, and on `USE_PREFETCH` builds `mdprefetch()` raises ERROR
    /// for a block past the relation's first segment file. Staying inside that
    /// segment requires both a coarse unit and a cap: children below 8 MiB can
    /// all map to one block, while children at or above the cap can all map to
    /// `RELSEG_SIZE`. In both cases inherited ANALYZE loses their storage-size
    /// ratio; changing this constant only moves the quantization boundary.
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    const BYTES_PER_BLOCK: u64 = 8 * 1024 * 1024;
    #[cfg(any(feature = "pg15", feature = "pg16"))]
    const MAX_BLOCKS: u64 = pg_sys::RELSEG_SIZE as u64;

    /// `storage_bytes` in `BLCKSZ` pages, capped below `InvalidBlockNumber`.
    pub(crate) fn pages(storage_bytes: u64) -> pg_sys::BlockNumber {
        let pages = storage_bytes
            .div_ceil(u64::from(pg_sys::BLCKSZ))
            .min(u64::from(u32::MAX - 1));
        pg_sys::BlockNumber::try_from(pages)
            .expect("capped Iceberg page count fits PostgreSQL BlockNumber")
    }

    #[cfg(any(feature = "pg15", feature = "pg16"))]
    pub(crate) fn blocks(storage_bytes: u64) -> u64 {
        storage_bytes
            .div_ceil(Self::BYTES_PER_BLOCK)
            .min(Self::MAX_BLOCKS)
    }

    #[cfg(any(feature = "pg17", feature = "pg18"))]
    pub(crate) fn blocks(storage_bytes: u64) -> u64 {
        u64::from(Self::pages(storage_bytes))
    }

    /// The `table_relation_size` that yields [`Self::blocks`]. PostgreSQL only
    /// reads this value through `RelationGetNumberOfBlocksInFork()`.
    pub(crate) fn relation_size(storage_bytes: u64) -> u64 {
        Self::blocks(storage_bytes) * u64::from(pg_sys::BLCKSZ)
    }
}
