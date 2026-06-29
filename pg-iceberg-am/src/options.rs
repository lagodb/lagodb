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
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::options::table::{
    AmCache, AmCacheRef, AmCacheValue, AmCacheable, TableOptionError, TableOptions,
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
pub const OPT_COMPRESSION_CODEC_DEFAULT: &str = CompressionOption::Zstd.as_str();
pub const OPT_COMPRESSION_CODEC_VALUES: &[&str] = &[
    CompressionOption::Snappy.as_str(),
    CompressionOption::Zstd.as_str(),
];

/// File format used for writes. The writer currently supports Parquet only.
pub const OPT_WRITE_FORMAT: &str = "write.format.default";
pub const OPT_WRITE_FORMAT_DEFAULT: &str = WriteFormatOption::Parquet.as_str();
pub const OPT_WRITE_FORMAT_VALUES: &[&str] = &[WriteFormatOption::Parquet.as_str()];

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

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompressionOption {
    Snappy,
    Zstd,
}

impl CompressionOption {
    fn parse(value: &str) -> Result<Self, TableOptionError> {
        if value == Self::Snappy.as_str() {
            Ok(Self::Snappy)
        } else if value == Self::Zstd.as_str() {
            Ok(Self::Zstd)
        } else {
            Err(TableOptionError::InvalidOption(format!(
                "{} must be one of {}, got {:?}",
                OPT_COMPRESSION_CODEC,
                OPT_COMPRESSION_CODEC_VALUES.join(", "),
                value,
            )))
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Snappy => "snappy",
            Self::Zstd => "zstd",
        }
    }

    fn parquet(self) -> ParquetCompression {
        match self {
            Self::Snappy => ParquetCompression::SNAPPY,
            Self::Zstd => ParquetCompression::ZSTD(ZstdLevel::default()),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteFormatOption {
    Parquet,
}

impl WriteFormatOption {
    fn parse(value: &str) -> Result<Self, TableOptionError> {
        if value == Self::Parquet.as_str() {
            Ok(Self::Parquet)
        } else {
            Err(TableOptionError::InvalidOption(format!(
                "{} must be one of {}, got {:?}",
                OPT_WRITE_FORMAT,
                OPT_WRITE_FORMAT_VALUES.join(", "),
                value,
            )))
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
        }
    }
}

/// Validated semantic table options shared by DDL and `rd_amcache` creation.
///
/// Every finite option is parsed into a compact domain enum. Ownership is
/// introduced only when metadata properties require a `HashMap<String, String>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedIcebergOptions {
    format_version: FormatVersion,
    compression: CompressionOption,
    write_format: WriteFormatOption,
    delete_isolation: IsolationLevel,
    update_isolation: IsolationLevel,
    merge_isolation: IsolationLevel,
}

impl ResolvedIcebergOptions {
    /// Resolve parsed options, applying defaults and validating semantic values.
    pub(crate) fn from_table_options(
        options: Option<&TableOptions>,
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
            .and_then(|options| options.get_str(OPT_WRITE_DELETE_ISOLATION_LEVEL))
            .unwrap_or(OPT_WRITE_ISOLATION_LEVEL_DEFAULT);
        let update_isolation = options
            .and_then(|options| options.get_str(OPT_WRITE_UPDATE_ISOLATION_LEVEL))
            .unwrap_or(OPT_WRITE_ISOLATION_LEVEL_DEFAULT);
        let merge_isolation = options
            .and_then(|options| options.get_str(OPT_WRITE_MERGE_ISOLATION_LEVEL))
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
        compression: &str,
        write_format: &str,
        delete_isolation: &str,
        update_isolation: &str,
        merge_isolation: &str,
    ) -> Result<Self, TableOptionError> {
        let compression = CompressionOption::parse(compression)?;
        let write_format = WriteFormatOption::parse(write_format)?;
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

    pub(crate) fn properties(self) -> HashMap<String, String> {
        HashMap::from([
            (
                OPT_COMPRESSION_CODEC.to_owned(),
                self.compression.as_str().to_owned(),
            ),
            (
                OPT_WRITE_FORMAT.to_owned(),
                self.write_format.as_str().to_owned(),
            ),
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
    fn into_cache(self) -> IcebergTableOptionCache {
        IcebergTableOptionCache {
            format_version: self.format_version,
            compression: self.compression,
            write_format: self.write_format,
        }
    }
}

// ============================================================================
//  Table Option Cache (rd_amcache)
// ============================================================================

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
struct IcebergTableOptionCache {
    format_version: FormatVersion,
    compression: CompressionOption,
    write_format: WriteFormatOption,
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

/// Iceberg-specific safe access to this AM's single `rd_amcache` type.
#[derive(Clone, Copy)]
pub(crate) struct IcebergTableOptions<'a>(AmCacheRef<'a, IcebergTableOptionCache>);

impl<'a> IcebergTableOptions<'a> {
    pub(crate) fn for_relation(
        rel: &RelationHandle<'a>,
    ) -> Result<Self, TableOptionError> {
        // SAFETY: `IcebergTableOptionCache` is private to this module and this is
        // the only production accessor for an Iceberg relation's `rd_amcache`.
        // Every access therefore uses the same concrete cache type.
        let cached = unsafe { AmCache::get::<IcebergTableOptionCache>(rel)? };
        Ok(Self(cached))
    }

    pub(crate) fn parquet_compression(self) -> ParquetCompression {
        self.0.header().compression.parquet()
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
            assert_eq!(properties.get(option).map(String::as_str), Some("snapshot"),);
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
    fn compact_cache_remains_valid_after_copy() {
        let options = TableOptions::new(vec![
            (OPT_FORMAT_VERSION.to_owned(), Some("1".to_owned())),
            (OPT_COMPRESSION_CODEC.to_owned(), Some("snappy".to_owned())),
            (OPT_WRITE_FORMAT.to_owned(), Some("parquet".to_owned())),
        ]);
        let cache = ResolvedIcebergOptions::from_table_options(Some(&options))
            .unwrap()
            .into_cache();

        let copied = cache;

        assert_eq!(std::mem::size_of_val(&cache), 3);
        assert_eq!(copied.format_version, FormatVersion::V1);
        assert_eq!(copied.compression.as_str(), "snappy");
        assert_eq!(copied.write_format.as_str(), "parquet");
        assert_eq!(copied.compression.parquet(), ParquetCompression::SNAPPY);
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
            CompressionOption::parse("snappy").unwrap().parquet(),
            ParquetCompression::SNAPPY,
        );
        assert!(matches!(
            CompressionOption::parse("invalid"),
            Err(TableOptionError::InvalidOption(_)),
        ));
    }
}
