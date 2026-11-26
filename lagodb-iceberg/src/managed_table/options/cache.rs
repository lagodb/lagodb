//! `rd_amcache` layout and relation-local Parquet writer option access.

use iceberg_lite::spec::FormatVersion;
use lagodb_core::handles::RelationHandle;
use lagodb_core::options::table::{
    AmCache, AmCacheValue, AmCacheable, TableOptionError, TableOptions,
};
use parquet::file::properties::{DEFAULT_MAX_ROW_GROUP_ROW_COUNT, WriterProperties};

use super::resolved::{CompressionOption, ResolvedIcebergOptions, WriteFormatOption};

/// Address-independent Iceberg writer options stored in `rd_amcache`.
///
/// All fields are compact `#[repr(u8)]` enums, so copying this value cannot
/// detach address-relative state from its backing allocation.
///
/// ```text
/// +----------------------------------+
/// | format_version: u8               |
/// | compression: u8                  |
/// | write_format: u8                 |
/// +----------------------------------+
/// ```
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(super) struct IcebergTableOptionCache {
    pub(super) format_version: FormatVersion,
    pub(super) compression: CompressionOption,
    pub(super) write_format: WriteFormatOption,
}

// SAFETY: `IcebergTableOptionCache` is non-zero-sized, `#[repr(C)]`, and every
// field is a fixed-size `#[repr(u8)]`, `Copy` enum. It contains no pointers,
// owned allocations, or address-dependent offsets.
unsafe impl AmCacheable for IcebergTableOptionCache {
    fn from_options(
        opts: Option<&TableOptions>,
    ) -> Result<AmCacheValue<Self>, TableOptionError> {
        Ok(AmCacheValue::fixed(
            ResolvedIcebergOptions::from_table_options(opts)?.into_cache(),
        ))
    }
}

/// Row limit of every Parquet row group the managed-table writers produce.
pub(crate) const MANAGED_ROW_GROUP_MAX_ROWS: usize = DEFAULT_MAX_ROW_GROUP_ROW_COUNT;

/// Iceberg-specific safe access to this AM's single `rd_amcache` type.
///
/// Values are copied out of the cache before returning: `AmCache::get`
/// requires that no relcache invalidation is processed while its borrow is
/// alive, which a borrow escaping to the caller could not guarantee.
pub(crate) struct IcebergTableOptions;

impl IcebergTableOptions {
    /// Parquet writer settings for `rel`'s data files.
    pub(crate) fn writer_properties(
        rel: &RelationHandle<'_>,
    ) -> Result<WriterProperties, TableOptionError> {
        // SAFETY: `IcebergTableOptionCache` is private to `options` and this is
        // the only production accessor for an Iceberg relation's `rd_amcache`,
        // so every access uses the same concrete cache type. The borrow ends
        // before this function returns, and building the properties processes
        // no invalidation.
        let cached = unsafe { AmCache::get::<IcebergTableOptionCache>(rel)? };
        Ok(WriterProperties::builder()
            .set_compression(cached.header().compression.parquet())
            .set_max_row_group_row_count(Some(MANAGED_ROW_GROUP_MAX_ROWS))
            .build())
    }
}
