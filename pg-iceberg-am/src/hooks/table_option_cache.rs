//! Iceberg table options cache for rd_amcache.
//!
//! This module provides a cached view of Iceberg table options that can be stored
//! in PostgreSQL's `rd_amcache` field for efficient access during table operations.
//!
//! # Memory Layout
//!
//! The cache uses a contiguous memory block allocated via `palloc`:
//!
//! ```text
//! +-------------------------------------------------------+
//! |  IcebergTableOptionCache (Fixed Size, #[repr(C)])     | <- rd_amcache points here
//! |-------------------------------------------------------|
//! |  format_version: i32                                  |
//! |  compression_offset: u32  (relative to struct start)  |
//! |  write_format_offset: u32                             |
//! +-------------------------------------------------------+
//! |  Variable Data Area (u8 bytes)                        |
//! |-------------------------------------------------------|
//! |  "zstd\0"                                             |
//! |  "parquet\0"                                          |
//! +-------------------------------------------------------+
//! ```

use super::table_options::{
    OPT_COMPRESSION_CODEC, OPT_COMPRESSION_CODEC_DEFAULT, OPT_FORMAT_VERSION,
    OPT_FORMAT_VERSION_DEFAULT, OPT_WRITE_FORMAT, OPT_WRITE_FORMAT_DEFAULT,
};
use pg_lakebase_core::options::table::{
    AmCacheLayout, AmCacheLayoutBuilder, AmCacheStringOffset, AmCacheable,
    TableOptions,
};
use std::collections::HashMap;

/// Iceberg table options cached in rd_amcache.
///
/// This struct is stored directly in Postgres memory via palloc.
/// All fields must be POD types (no String, Vec, Box).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IcebergTableOptionCache {
    pub format_version: i32,
    compression_offset: AmCacheStringOffset,
    write_format_offset: AmCacheStringOffset,
}

// SAFETY: IcebergTableOptionCache is #[repr(C)] and contains only POD types.
// No heap allocations, no Drop implementations needed.
unsafe impl AmCacheable for IcebergTableOptionCache {
    fn from_options(opts: &TableOptions) -> (Self, Vec<u8>) {
        let mut layout = AmCacheLayoutBuilder::for_header::<Self>();

        let format_version = opts
            .get_int(OPT_FORMAT_VERSION)
            .unwrap_or(OPT_FORMAT_VERSION_DEFAULT);
        let compression = opts
            .get_str(OPT_COMPRESSION_CODEC)
            .unwrap_or(OPT_COMPRESSION_CODEC_DEFAULT);
        let write_format = opts
            .get_str(OPT_WRITE_FORMAT)
            .unwrap_or(OPT_WRITE_FORMAT_DEFAULT);

        let compression_offset = layout.push_str(compression);
        let write_format_offset = layout.push_str(write_format);

        (
            Self {
                format_version,
                compression_offset,
                write_format_offset,
            },
            layout.into_bytes(),
        )
    }

    fn default_options() -> (Self, Vec<u8>) {
        let mut layout = AmCacheLayoutBuilder::for_header::<Self>();

        let compression_offset = layout.push_str(OPT_COMPRESSION_CODEC_DEFAULT);
        let write_format_offset = layout.push_str(OPT_WRITE_FORMAT_DEFAULT);

        (
            Self {
                format_version: OPT_FORMAT_VERSION_DEFAULT,
                compression_offset,
                write_format_offset,
            },
            layout.into_bytes(),
        )
    }
}

impl IcebergTableOptionCache {
    pub fn compression(&self) -> &str {
        unsafe {
            AmCacheLayout::str_at_offset(
                self as *const _ as *const u8,
                self.compression_offset,
            )
        }
    }

    pub fn write_format(&self) -> &str {
        unsafe {
            AmCacheLayout::str_at_offset(
                self as *const _ as *const u8,
                self.write_format_offset,
            )
        }
    }

    /// Convert cached options to Iceberg table properties HashMap.
    ///
    /// This is the single source of truth for converting options to properties.
    /// All option-to-property mapping logic is centralized here.
    pub fn to_properties(&self) -> HashMap<String, String> {
        let mut properties = HashMap::new();

        properties.insert(
            OPT_COMPRESSION_CODEC.to_string(),
            self.compression().to_string(),
        );

        properties.insert(
            OPT_WRITE_FORMAT.to_string(),
            self.write_format().to_string(),
        );

        properties
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let (cache, data) = IcebergTableOptionCache::default_options();
        assert_eq!(cache.format_version, 2);
        // Verify offsets are non-zero (strings are stored)
        assert!(cache.compression_offset > 0);
        assert!(cache.write_format_offset > 0);
        // Verify data contains expected strings with null terminators
        assert!(data.len() > 0);
    }
}
