//! Iceberg table option definitions and cache.
//!
//! This module centralizes all Iceberg table option constants, their validation
//! definitions, and the `rd_amcache` layout. By living at the crate root it
//! breaks the former bidirectional dependency between `catalog` (which reads
//! options) and `hooks` (which validates and persists options during DDL).
//!
//! This is the *schema* layer for reloptions: it defines what is valid and
//! how cached options are laid out in PostgreSQL cache memory. The *DDL path*
//! that parses, validates, and persists those options on `CREATE TABLE` lives
//! in `crate::hooks::table_ddl`.

use iceberg_lite::spec::{FormatVersion, IsolationLevel, TableProperties};
use parquet::basic::{Compression as ParquetCompression, ZstdLevel};
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

/// Supported Parquet compression codecs.
pub const OPT_COMPRESSION_CODEC: &str = "write.parquet.compression-codec";
pub const OPT_COMPRESSION_CODEC_DEFAULT: &str = "zstd";
pub const OPT_COMPRESSION_CODEC_VALUES: &[&str] = &["snappy", "zstd"];

/// File format used for writes. The writer currently supports Parquet only.
pub const OPT_WRITE_FORMAT: &str = "write.format.default";
pub const OPT_WRITE_FORMAT_DEFAULT: &str = "parquet";
pub const OPT_WRITE_FORMAT_VALUES: &[&str] = &["parquet"];

/// Command-specific Iceberg row-level DML isolation.
pub const OPT_WRITE_DELETE_ISOLATION_LEVEL: &str =
    TableProperties::PROPERTY_WRITE_DELETE_ISOLATION_LEVEL;
pub const OPT_WRITE_UPDATE_ISOLATION_LEVEL: &str =
    TableProperties::PROPERTY_WRITE_UPDATE_ISOLATION_LEVEL;
pub const OPT_WRITE_MERGE_ISOLATION_LEVEL: &str =
    TableProperties::PROPERTY_WRITE_MERGE_ISOLATION_LEVEL;
pub const OPT_WRITE_ISOLATION_LEVEL_DEFAULT: &str =
    TableProperties::PROPERTY_WRITE_ISOLATION_LEVEL_DEFAULT.as_str();
pub const OPT_WRITE_ISOLATION_LEVEL_VALUES: &[&str] = &[
    IsolationLevel::Snapshot.as_str(),
    IsolationLevel::Serializable.as_str(),
];

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
        kind: OptionKind::Enum {
            default: OPT_COMPRESSION_CODEC_DEFAULT,
            values: OPT_COMPRESSION_CODEC_VALUES,
        },
        description: "Parquet compression codec (snappy, zstd)",
    },
    OptionDef {
        name: OPT_WRITE_FORMAT,
        kind: OptionKind::Enum {
            default: OPT_WRITE_FORMAT_DEFAULT,
            values: OPT_WRITE_FORMAT_VALUES,
        },
        description: "Default file format (parquet)",
    },
    OptionDef {
        name: OPT_WRITE_DELETE_ISOLATION_LEVEL,
        kind: OptionKind::Enum {
            default: OPT_WRITE_ISOLATION_LEVEL_DEFAULT,
            values: OPT_WRITE_ISOLATION_LEVEL_VALUES,
        },
        description: "Iceberg DELETE isolation level (snapshot or serializable)",
    },
    OptionDef {
        name: OPT_WRITE_UPDATE_ISOLATION_LEVEL,
        kind: OptionKind::Enum {
            default: OPT_WRITE_ISOLATION_LEVEL_DEFAULT,
            values: OPT_WRITE_ISOLATION_LEVEL_VALUES,
        },
        description: "Iceberg UPDATE isolation level (snapshot or serializable)",
    },
    OptionDef {
        name: OPT_WRITE_MERGE_ISOLATION_LEVEL,
        kind: OptionKind::Enum {
            default: OPT_WRITE_ISOLATION_LEVEL_DEFAULT,
            values: OPT_WRITE_ISOLATION_LEVEL_VALUES,
        },
        description: "Iceberg MERGE isolation level (snapshot or serializable)",
    },
];

/// Validated semantic table options shared by DDL and `rd_amcache` creation.
///
/// String-valued writer options borrow either the parsed [`TableOptions`] or
/// static defaults. Isolation values are parsed into Iceberg's domain enum.
/// Ownership is introduced only when metadata properties require a
/// `HashMap<String, String>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedIcebergOptions<'a> {
    format_version: FormatVersion,
    compression: &'a str,
    write_format: &'a str,
    delete_isolation: IsolationLevel,
    update_isolation: IsolationLevel,
    merge_isolation: IsolationLevel,
}

impl<'a> ResolvedIcebergOptions<'a> {
    /// Resolve parsed options, applying defaults and validating semantic values.
    pub(crate) fn from_table_options(
        options: Option<&'a TableOptions>,
    ) -> Result<Self, TableOptionError> {
        let format_version =
            match options.and_then(|options| options.get_str(OPT_FORMAT_VERSION)) {
                Some(value) => value.parse::<i32>().map_err(|_| {
                    TableOptionError::InvalidOption(format!(
                        "{} must be an integer between {} and {}, got {:?}",
                        OPT_FORMAT_VERSION,
                        OPT_FORMAT_VERSION_MIN,
                        OPT_FORMAT_VERSION_MAX,
                        value,
                    ))
                })?,
                None => OPT_FORMAT_VERSION_DEFAULT,
            };
        let compression = options
            .and_then(|options| options.get_str(OPT_COMPRESSION_CODEC))
            .unwrap_or(OPT_COMPRESSION_CODEC_DEFAULT);
        let write_format = options
            .and_then(|options| options.get_str(OPT_WRITE_FORMAT))
            .unwrap_or(OPT_WRITE_FORMAT_DEFAULT);
        let delete_isolation = options
            .and_then(|options| {
                options.get_str(OPT_WRITE_DELETE_ISOLATION_LEVEL)
            })
            .unwrap_or(OPT_WRITE_ISOLATION_LEVEL_DEFAULT);
        let update_isolation = options
            .and_then(|options| {
                options.get_str(OPT_WRITE_UPDATE_ISOLATION_LEVEL)
            })
            .unwrap_or(OPT_WRITE_ISOLATION_LEVEL_DEFAULT);
        let merge_isolation = options
            .and_then(|options| {
                options.get_str(OPT_WRITE_MERGE_ISOLATION_LEVEL)
            })
            .unwrap_or(OPT_WRITE_ISOLATION_LEVEL_DEFAULT);

        Self::from_parts(
            format_version,
            compression,
            write_format,
            delete_isolation,
            update_isolation,
            merge_isolation,
        )
    }

    fn from_parts(
        format_version: i32,
        compression: &'a str,
        write_format: &'a str,
        delete_isolation: &'a str,
        update_isolation: &'a str,
        merge_isolation: &'a str,
    ) -> Result<Self, TableOptionError> {
        Self::resolve_parquet_compression(compression)?;
        if !OPT_WRITE_FORMAT_VALUES.contains(&write_format) {
            return Err(TableOptionError::InvalidOption(format!(
                "{} must be one of {}, got {:?}",
                OPT_WRITE_FORMAT,
                OPT_WRITE_FORMAT_VALUES.join(", "),
                write_format,
            )));
        }
        let delete_isolation = Self::resolve_isolation_level(
            OPT_WRITE_DELETE_ISOLATION_LEVEL,
            delete_isolation,
        )?;
        let update_isolation = Self::resolve_isolation_level(
            OPT_WRITE_UPDATE_ISOLATION_LEVEL,
            update_isolation,
        )?;
        let merge_isolation = Self::resolve_isolation_level(
            OPT_WRITE_MERGE_ISOLATION_LEVEL,
            merge_isolation,
        )?;

        Ok(Self {
            format_version: Self::resolve_format_version(format_version)?,
            compression,
            write_format,
            delete_isolation,
            update_isolation,
            merge_isolation,
        })
    }

    fn resolve_parquet_compression(
        value: &str,
    ) -> Result<ParquetCompression, TableOptionError> {
        match value {
            "snappy" => Ok(ParquetCompression::SNAPPY),
            "zstd" => Ok(ParquetCompression::ZSTD(ZstdLevel::default())),
            value => Err(TableOptionError::InvalidOption(format!(
                "{} must be one of {}, got {:?}",
                OPT_COMPRESSION_CODEC,
                OPT_COMPRESSION_CODEC_VALUES.join(", "),
                value,
            ))),
        }
    }

    fn resolve_format_version(value: i32) -> Result<FormatVersion, TableOptionError> {
        match value {
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

    fn resolve_isolation_level(
        option: &str,
        value: &str,
    ) -> Result<IsolationLevel, TableOptionError> {
        value.parse::<IsolationLevel>().map_err(|_| {
            TableOptionError::InvalidOption(format!(
                "{} must be one of {}, got {:?}",
                option,
                OPT_WRITE_ISOLATION_LEVEL_VALUES.join(", "),
                value,
            ))
        })
    }

    pub(crate) fn format_version(self) -> FormatVersion {
        self.format_version
    }

    fn format_version_number(self) -> i32 {
        self.format_version as u8 as i32
    }

    pub(crate) fn properties(self) -> HashMap<String, String> {
        HashMap::from([
            (
                OPT_COMPRESSION_CODEC.to_owned(),
                self.compression.to_owned(),
            ),
            (OPT_WRITE_FORMAT.to_owned(), self.write_format.to_owned()),
            (
                OPT_WRITE_DELETE_ISOLATION_LEVEL.to_owned(),
                self.delete_isolation.as_str().to_owned(),
            ),
            (
                OPT_WRITE_UPDATE_ISOLATION_LEVEL.to_owned(),
                self.update_isolation.as_str().to_owned(),
            ),
            (
                OPT_WRITE_MERGE_ISOLATION_LEVEL.to_owned(),
                self.merge_isolation.as_str().to_owned(),
            ),
        ])
    }

    /// Cache only values used by the relation-local write hot path.
    ///
    /// DML isolation remains authoritative in Iceberg metadata and is resolved
    /// from `TableProperties` when a modify session opens.
    fn into_cache(self) -> (IcebergTableOptionCache, Vec<u8>) {
        let mut layout =
            AmCacheLayoutBuilder::for_header::<IcebergTableOptionCache>();
        let compression_offset = layout.push_str(self.compression);
        let write_format_offset = layout.push_str(self.write_format);

        (
            IcebergTableOptionCache {
                format_version: self.format_version_number(),
                compression_offset,
                write_format_offset,
            },
            layout.into_bytes(),
        )
    }
}

// ============================================================================
//  Table Option Cache (rd_amcache)
// ============================================================================

/// Hot-path Iceberg writer options cached in `rd_amcache`.
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
    fn from_options(
        opts: Option<&TableOptions>,
    ) -> Result<(Self, Vec<u8>), TableOptionError> {
        Ok(ResolvedIcebergOptions::from_table_options(opts)?.into_cache())
    }
}

impl IcebergTableOptionCache {
    pub fn iceberg_format_version(&self) -> Result<FormatVersion, TableOptionError> {
        ResolvedIcebergOptions::resolve_format_version(self.format_version)
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

    pub fn parquet_compression(
        &self,
    ) -> Result<ParquetCompression, TableOptionError> {
        ResolvedIcebergOptions::resolve_parquet_compression(self.compression())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation_options_apply_defaults_without_cache_storage() {
        let options = ResolvedIcebergOptions::from_table_options(None).unwrap();
        let format_version = options.format_version();
        let properties = options.properties();

        assert_eq!(format_version, FormatVersion::V2);
        assert_eq!(
            properties.get(OPT_COMPRESSION_CODEC).map(String::as_str),
            Some(OPT_COMPRESSION_CODEC_DEFAULT),
        );
        assert_eq!(
            properties.get(OPT_WRITE_FORMAT).map(String::as_str),
            Some(OPT_WRITE_FORMAT_DEFAULT),
        );
        for option in [
            OPT_WRITE_DELETE_ISOLATION_LEVEL,
            OPT_WRITE_UPDATE_ISOLATION_LEVEL,
            OPT_WRITE_MERGE_ISOLATION_LEVEL,
        ] {
            assert_eq!(
                properties.get(option).map(String::as_str),
                Some(OPT_WRITE_ISOLATION_LEVEL_DEFAULT),
            );
        }
    }

    #[test]
    fn creation_options_preserve_explicit_values_and_partial_defaults() {
        let options = TableOptions::new(vec![
            (OPT_FORMAT_VERSION.to_owned(), Some("1".to_owned())),
            (OPT_COMPRESSION_CODEC.to_owned(), Some("snappy".to_owned())),
            (
                OPT_WRITE_DELETE_ISOLATION_LEVEL.to_owned(),
                Some("snapshot".to_owned()),
            ),
            (
                OPT_WRITE_UPDATE_ISOLATION_LEVEL.to_owned(),
                Some("snapshot".to_owned()),
            ),
            (
                OPT_WRITE_MERGE_ISOLATION_LEVEL.to_owned(),
                Some("snapshot".to_owned()),
            ),
        ]);
        let resolved =
            ResolvedIcebergOptions::from_table_options(Some(&options)).unwrap();
        let format_version = resolved.format_version();
        let properties = resolved.properties();

        assert_eq!(format_version, FormatVersion::V1);
        assert_eq!(
            properties.get(OPT_COMPRESSION_CODEC).map(String::as_str),
            Some("snappy"),
        );
        assert_eq!(
            properties.get(OPT_WRITE_FORMAT).map(String::as_str),
            Some(OPT_WRITE_FORMAT_DEFAULT),
        );
        for option in [
            OPT_WRITE_DELETE_ISOLATION_LEVEL,
            OPT_WRITE_UPDATE_ISOLATION_LEVEL,
            OPT_WRITE_MERGE_ISOLATION_LEVEL,
        ] {
            assert_eq!(
                properties.get(option).map(String::as_str),
                Some("snapshot"),
            );
        }
    }

    #[test]
    fn creation_options_defensively_reject_invalid_format_version() {
        let options = TableOptions::new(vec![(
            OPT_FORMAT_VERSION.to_owned(),
            Some("4".to_owned()),
        )]);

        assert!(matches!(
            ResolvedIcebergOptions::from_table_options(Some(&options)),
            Err(TableOptionError::InvalidOption(_)),
        ));
    }

    #[test]
    fn test_default_options() {
        let (cache, data) = IcebergTableOptionCache::from_options(None).unwrap();
        assert_eq!(cache.format_version, 2);
        assert_eq!(cache.iceberg_format_version().unwrap(), FormatVersion::V2);
        assert!(cache.compression_offset > 0);
        assert!(cache.write_format_offset > 0);
        assert!(!data.is_empty());
    }

    #[test]
    fn format_version_maps_supported_values() {
        let (mut cache, _) = IcebergTableOptionCache::from_options(None).unwrap();

        cache.format_version = 1;
        assert_eq!(cache.iceberg_format_version().unwrap(), FormatVersion::V1);

        cache.format_version = 2;
        assert_eq!(cache.iceberg_format_version().unwrap(), FormatVersion::V2);

        cache.format_version = 3;
        assert_eq!(cache.iceberg_format_version().unwrap(), FormatVersion::V3);
    }

    #[test]
    fn format_version_rejects_unvalidated_values() {
        let (mut cache, _) = IcebergTableOptionCache::from_options(None).unwrap();
        cache.format_version = 4;

        let err = cache.iceberg_format_version().unwrap_err();
        assert!(err.to_string().contains(OPT_FORMAT_VERSION));
    }

    #[test]
    fn resolved_options_reject_non_integer_format_version() {
        let options = TableOptions::new(vec![(
            OPT_FORMAT_VERSION.to_owned(),
            Some("not-an-integer".to_owned()),
        )]);

        assert!(matches!(
            ResolvedIcebergOptions::from_table_options(Some(&options)),
            Err(TableOptionError::InvalidOption(_)),
        ));
    }

    #[test]
    fn resolved_options_reject_unknown_write_format() {
        let options = TableOptions::new(vec![(
            OPT_WRITE_FORMAT.to_owned(),
            Some("avro".to_owned()),
        )]);

        assert!(matches!(
            ResolvedIcebergOptions::from_table_options(Some(&options)),
            Err(TableOptionError::InvalidOption(_)),
        ));
    }

    #[test]
    fn resolved_options_reject_unknown_isolation_level() {
        let options = TableOptions::new(vec![(
            OPT_WRITE_UPDATE_ISOLATION_LEVEL.to_owned(),
            Some("read-committed".to_owned()),
        )]);

        assert!(matches!(
            ResolvedIcebergOptions::from_table_options(Some(&options)),
            Err(TableOptionError::InvalidOption(_)),
        ));
    }

    #[test]
    fn parquet_compression_values_are_validated_and_mapped() {
        assert_eq!(
            ResolvedIcebergOptions::resolve_parquet_compression("snappy").unwrap(),
            ParquetCompression::SNAPPY,
        );
        assert!(matches!(
            ResolvedIcebergOptions::resolve_parquet_compression("invalid"),
            Err(TableOptionError::InvalidOption(_)),
        ));
    }
}

#[cfg(feature = "pg_test")]
mod pg_test;
