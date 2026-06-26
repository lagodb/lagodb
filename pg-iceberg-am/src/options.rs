//! Iceberg table option definitions and cache.
//!
//! This module centralizes all Iceberg table option constants, their validation
//! definitions, and the `rd_amcache` layout. By living at the crate root it
//! breaks the former bidirectional dependency between `catalog` (which reads
//! options) and `hooks` (which validates and persists options during DDL).
//!
//! This is the *schema* layer for reloptions: it defines what is valid and
//! how cached options are laid out in shared memory. The *DDL path* that
//! parses, validates, and persists those options on `CREATE TABLE` lives in
//! `crate::hooks::table_ddl`.

use iceberg_lite::spec::FormatVersion;
use pg_lakebase_core::options::table::{
    AmCacheLayout, AmCacheLayoutBuilder, AmCacheStringOffset, AmCacheable,
    TableOptionError, TableOptions,
};
use pg_lakebase_core::options::{OptionDef, OptionKind};
use std::collections::HashMap;

// ============================================================================
//  Option Name Constants
// ============================================================================

/// Iceberg table format version.
pub const OPT_FORMAT_VERSION: &str = "format-version";
pub const OPT_FORMAT_VERSION_MIN: i32 = 1;
pub const OPT_FORMAT_VERSION_MAX: i32 = 3;
pub const OPT_FORMAT_VERSION_DEFAULT: i32 = 2;

/// Parquet compression codec (snappy, zstd, etc.)
pub const OPT_COMPRESSION_CODEC: &str = "write.parquet.compression-codec";
pub const OPT_COMPRESSION_CODEC_DEFAULT: &str = "zstd";

/// Default file format for writing (parquet, avro, orc)
pub const OPT_WRITE_FORMAT: &str = "write.format.default";
pub const OPT_WRITE_FORMAT_DEFAULT: &str = "parquet";
pub const OPT_WRITE_FORMAT_VALUES: &[&str] = &["parquet", "avro", "orc"];

// ============================================================================
//  Option Definitions
// ============================================================================

/// Iceberg-specific table options definition.
pub static ICEBERG_TABLE_OPTIONS: &[OptionDef] = &[
    OptionDef {
        name: OPT_FORMAT_VERSION,
        kind: OptionKind::Int {
            default: OPT_FORMAT_VERSION_DEFAULT,
            min: Some(OPT_FORMAT_VERSION_MIN),
            max: Some(OPT_FORMAT_VERSION_MAX),
        },
        description: "Iceberg table format version (1, 2, or 3)",
    },
    OptionDef {
        name: OPT_COMPRESSION_CODEC,
        kind: OptionKind::String {
            default: Some(OPT_COMPRESSION_CODEC_DEFAULT),
        },
        description: "Parquet compression codec (snappy, zstd)",
    },
    OptionDef {
        name: OPT_WRITE_FORMAT,
        kind: OptionKind::Enum {
            default: OPT_WRITE_FORMAT_DEFAULT,
            values: OPT_WRITE_FORMAT_VALUES,
        },
        description: "Default file format (parquet, avro, orc)",
    },
];

// ============================================================================
//  Table Option Cache (rd_amcache)
// ============================================================================

/// Iceberg table options cached in rd_amcache.
///
/// This struct is stored directly in Postgres memory via palloc.
/// All fields must be POD types (no String, Vec, Box).
///
/// ```text
/// +-------------------------------------------------------+
/// |  IcebergTableOptionCache (Fixed Size, #[repr(C)])     |
/// |-------------------------------------------------------|
/// |  format_version: i32                                  |
/// |  compression_offset: u32  (relative to struct start)  |
/// |  write_format_offset: u32                             |
/// +-------------------------------------------------------+
/// |  Variable Data Area (u8 bytes)                        |
/// |-------------------------------------------------------|
/// |  "zstd\0"                                             |
/// |  "parquet\0"                                          |
/// +-------------------------------------------------------+
/// ```
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IcebergTableOptionCache {
    pub format_version: i32,
    compression_offset: AmCacheStringOffset,
    write_format_offset: AmCacheStringOffset,
}

// SAFETY: IcebergTableOptionCache is #[repr(C)] and contains only POD types.
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
    pub fn from_table_options(opts: Option<&TableOptions>) -> Self {
        match opts {
            Some(opts) => {
                let (cache, _) = <Self as AmCacheable>::from_options(opts);
                cache
            }
            None => {
                let (cache, _) = <Self as AmCacheable>::default_options();
                cache
            }
        }
    }

    pub fn iceberg_format_version(&self) -> Result<FormatVersion, TableOptionError> {
        match self.format_version {
            1 => Ok(FormatVersion::V1),
            2 => Ok(FormatVersion::V2),
            3 => Ok(FormatVersion::V3),
            value => Err(TableOptionError::InvalidOption(format!(
                "{} must be between {} and {}, got {}",
                OPT_FORMAT_VERSION,
                OPT_FORMAT_VERSION_MIN,
                OPT_FORMAT_VERSION_MAX,
                value
            ))),
        }
    }

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
    pub fn to_properties(&self) -> HashMap<String, String> {
        let mut properties = HashMap::with_capacity(2);

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
        assert_eq!(cache.iceberg_format_version().unwrap(), FormatVersion::V2);
        assert!(cache.compression_offset > 0);
        assert!(cache.write_format_offset > 0);
        assert!(!data.is_empty());
    }

    #[test]
    fn format_version_maps_supported_values() {
        let (mut cache, _) = IcebergTableOptionCache::default_options();

        cache.format_version = 1;
        assert_eq!(cache.iceberg_format_version().unwrap(), FormatVersion::V1);

        cache.format_version = 2;
        assert_eq!(cache.iceberg_format_version().unwrap(), FormatVersion::V2);

        cache.format_version = 3;
        assert_eq!(cache.iceberg_format_version().unwrap(), FormatVersion::V3);
    }

    #[test]
    fn format_version_rejects_unvalidated_values() {
        let (mut cache, _) = IcebergTableOptionCache::default_options();
        cache.format_version = 4;

        let err = cache.iceberg_format_version().unwrap_err();
        assert!(err.to_string().contains(OPT_FORMAT_VERSION));
    }
}
