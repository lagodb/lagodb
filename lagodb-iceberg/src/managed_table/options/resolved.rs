//! Domain option enums and semantic table-option resolution.

use std::collections::HashMap;

use iceberg_lite::spec::{FormatVersion, IsolationLevel};
use lagodb_core::options::table::{TableOptionError, TableOptions};
use parquet::basic::{Compression as ParquetCompression, ZstdLevel};

use crate::scan::parallel::TaskGrouping;

use super::cache::IcebergTableOptionCache;
use super::{
    ICEBERG_TABLE_OPTIONS, OPT_COMPRESSION_CODEC, OPT_COMPRESSION_CODEC_DEFAULT,
    OPT_COMPRESSION_CODEC_VALUES, OPT_FORMAT_VERSION, OPT_FORMAT_VERSION_DEFAULT,
    OPT_FORMAT_VERSION_MAX, OPT_FORMAT_VERSION_MIN, OPT_MANIFEST_MERGE,
    OPT_MANIFEST_MERGE_DEFAULT, OPT_MANIFEST_MIN_COUNT,
    OPT_MANIFEST_MIN_COUNT_DEFAULT, OPT_MANIFEST_TARGET_SIZE,
    OPT_MANIFEST_TARGET_SIZE_DEFAULT, OPT_MAX_REF_AGE, OPT_MAX_REF_AGE_DEFAULT,
    OPT_MAX_SNAPSHOT_AGE, OPT_MAX_SNAPSHOT_AGE_DEFAULT, OPT_METADATA_VERSIONS,
    OPT_METADATA_VERSIONS_DEFAULT, OPT_MIN_SNAPSHOTS, OPT_MIN_SNAPSHOTS_DEFAULT,
    OPT_READ_SPLIT_OPEN_FILE_COST, OPT_READ_SPLIT_PLANNING_LOOKBACK,
    OPT_READ_SPLIT_TARGET_SIZE, OPT_TARGET_FILE_SIZE, OPT_TARGET_FILE_SIZE_DEFAULT,
    OPT_WRITE_DELETE_ISOLATION_LEVEL, OPT_WRITE_FORMAT, OPT_WRITE_FORMAT_DEFAULT,
    OPT_WRITE_FORMAT_VALUES, OPT_WRITE_ISOLATION_LEVEL_DEFAULT,
    OPT_WRITE_ISOLATION_LEVEL_VALUES, OPT_WRITE_MERGE_ISOLATION_LEVEL,
    OPT_WRITE_UPDATE_ISOLATION_LEVEL,
};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompressionOption {
    Snappy,
    Zstd,
}

impl CompressionOption {
    pub(super) fn parse(value: &str) -> Result<Self, TableOptionError> {
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

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Snappy => "snappy",
            Self::Zstd => "zstd",
        }
    }

    pub(super) fn parquet(self) -> ParquetCompression {
        match self {
            Self::Snappy => ParquetCompression::SNAPPY,
            Self::Zstd => ParquetCompression::ZSTD(ZstdLevel::default()),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriteFormatOption {
    Parquet,
}

impl WriteFormatOption {
    pub(super) fn parse(value: &str) -> Result<Self, TableOptionError> {
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

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
        }
    }
}

/// Validated semantic table options shared by DDL and `rd_amcache` creation.
///
/// Every finite option is parsed into a compact domain type. Ownership is
/// introduced only when metadata properties require a `HashMap<String, String>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedIcebergOptions {
    format_version: FormatVersion,
    compression: CompressionOption,
    write_format: WriteFormatOption,
    delete_isolation: IsolationLevel,
    update_isolation: IsolationLevel,
    merge_isolation: IsolationLevel,
    target_file_size: usize,
    max_snapshot_age_ms: i64,
    min_snapshots_to_keep: usize,
    max_ref_age_ms: i64,
    metadata_versions: usize,
    manifest_target_size: u64,
    manifest_min_count: usize,
    manifest_merge: bool,
    task_grouping: TaskGrouping,
}

struct UnresolvedIcebergOptions<'a> {
    format_version: i32,
    compression: &'a str,
    write_format: &'a str,
    delete_isolation: &'a str,
    update_isolation: &'a str,
    merge_isolation: &'a str,
    target_file_size: &'a str,
    max_snapshot_age_ms: &'a str,
    min_snapshots_to_keep: &'a str,
    max_ref_age_ms: &'a str,
    metadata_versions: &'a str,
    manifest_target_size: &'a str,
    manifest_min_count: &'a str,
    manifest_merge: &'a str,
    task_grouping: TaskGrouping,
}

impl ResolvedIcebergOptions {
    /// Resolve parsed options, applying defaults and validating semantic values.
    pub(crate) fn from_table_options(
        options: Option<&TableOptions>,
    ) -> Result<Self, TableOptionError> {
        if let Some(options) = options {
            for (name, _) in options.iter() {
                if !ICEBERG_TABLE_OPTIONS
                    .iter()
                    .any(|option| option.name == name)
                {
                    return Err(TableOptionError::InvalidOption(format!(
                        "unsupported Iceberg table option {name:?}"
                    )));
                }
            }
        }
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
        let task_grouping = TaskGrouping::from_values(
            options.and_then(|options| options.get_str(OPT_READ_SPLIT_TARGET_SIZE)),
            options.and_then(|options| {
                options.get_str(OPT_READ_SPLIT_PLANNING_LOOKBACK)
            }),
            options
                .and_then(|options| options.get_str(OPT_READ_SPLIT_OPEN_FILE_COST)),
        )
        .map_err(|err| TableOptionError::InvalidOption(err.to_string()))?;

        let value = |name, default| {
            options
                .and_then(|options| options.get_str(name))
                .unwrap_or(default)
        };

        Self::from_parts(UnresolvedIcebergOptions {
            format_version,
            compression,
            write_format,
            delete_isolation,
            update_isolation,
            merge_isolation,
            target_file_size: value(
                OPT_TARGET_FILE_SIZE,
                OPT_TARGET_FILE_SIZE_DEFAULT,
            ),
            max_snapshot_age_ms: value(
                OPT_MAX_SNAPSHOT_AGE,
                OPT_MAX_SNAPSHOT_AGE_DEFAULT,
            ),
            min_snapshots_to_keep: value(
                OPT_MIN_SNAPSHOTS,
                OPT_MIN_SNAPSHOTS_DEFAULT,
            ),
            max_ref_age_ms: value(OPT_MAX_REF_AGE, OPT_MAX_REF_AGE_DEFAULT),
            metadata_versions: value(
                OPT_METADATA_VERSIONS,
                OPT_METADATA_VERSIONS_DEFAULT,
            ),
            manifest_target_size: value(
                OPT_MANIFEST_TARGET_SIZE,
                OPT_MANIFEST_TARGET_SIZE_DEFAULT,
            ),
            manifest_min_count: value(
                OPT_MANIFEST_MIN_COUNT,
                OPT_MANIFEST_MIN_COUNT_DEFAULT,
            ),
            manifest_merge: value(
                OPT_MANIFEST_MERGE,
                if OPT_MANIFEST_MERGE_DEFAULT {
                    "true"
                } else {
                    "false"
                },
            ),
            task_grouping,
        })
    }

    fn from_parts(
        parts: UnresolvedIcebergOptions<'_>,
    ) -> Result<Self, TableOptionError> {
        let UnresolvedIcebergOptions {
            format_version,
            compression,
            write_format,
            delete_isolation,
            update_isolation,
            merge_isolation,
            target_file_size,
            max_snapshot_age_ms,
            min_snapshots_to_keep,
            max_ref_age_ms,
            metadata_versions,
            manifest_target_size,
            manifest_min_count,
            manifest_merge,
            task_grouping,
        } = parts;
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
            target_file_size: usize::try_from(Self::parse_positive(
                OPT_TARGET_FILE_SIZE,
                target_file_size,
            )?)
            .map_err(|_| {
                TableOptionError::InvalidOption(format!(
                    "{OPT_TARGET_FILE_SIZE} is too large"
                ))
            })?,
            max_snapshot_age_ms: Self::parse_i64_nonnegative(
                OPT_MAX_SNAPSHOT_AGE,
                max_snapshot_age_ms,
            )?,
            min_snapshots_to_keep: usize::try_from(Self::parse_positive(
                OPT_MIN_SNAPSHOTS,
                min_snapshots_to_keep,
            )?)
            .map_err(|_| {
                TableOptionError::InvalidOption(format!(
                    "{OPT_MIN_SNAPSHOTS} is too large"
                ))
            })?,
            max_ref_age_ms: Self::parse_i64_nonnegative(
                OPT_MAX_REF_AGE,
                max_ref_age_ms,
            )?,
            metadata_versions: usize::try_from(Self::parse_positive(
                OPT_METADATA_VERSIONS,
                metadata_versions,
            )?)
            .map_err(|_| {
                TableOptionError::InvalidOption(format!(
                    "{OPT_METADATA_VERSIONS} is too large"
                ))
            })?,
            manifest_target_size: Self::parse_positive(
                OPT_MANIFEST_TARGET_SIZE,
                manifest_target_size,
            )?,
            manifest_min_count: usize::try_from(Self::parse_positive(
                OPT_MANIFEST_MIN_COUNT,
                manifest_min_count,
            )?)
            .map_err(|_| {
                TableOptionError::InvalidOption(format!(
                    "{OPT_MANIFEST_MIN_COUNT} is too large"
                ))
            })?,
            manifest_merge: manifest_merge.parse::<bool>().map_err(|_| {
                TableOptionError::InvalidOption(format!(
                    "{OPT_MANIFEST_MERGE} must be true or false"
                ))
            })?,
            task_grouping,
        })
    }

    fn parse_positive(name: &str, value: &str) -> Result<u64, TableOptionError> {
        let parsed = value.parse::<u64>().map_err(|_| {
            TableOptionError::InvalidOption(format!(
                "{name} must be a positive integer"
            ))
        })?;
        (parsed > 0).then_some(parsed).ok_or_else(|| {
            TableOptionError::InvalidOption(format!(
                "{name} must be greater than zero"
            ))
        })
    }

    fn parse_i64_nonnegative(
        name: &str,
        value: &str,
    ) -> Result<i64, TableOptionError> {
        let parsed = value.parse::<i64>().map_err(|_| {
            TableOptionError::InvalidOption(format!(
                "{name} must be a non-negative integer"
            ))
        })?;
        (parsed >= 0).then_some(parsed).ok_or_else(|| {
            TableOptionError::InvalidOption(format!("{name} must not be negative"))
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
        let mut properties = HashMap::from([
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
            (
                OPT_TARGET_FILE_SIZE.to_owned(),
                self.target_file_size.to_string(),
            ),
            (
                OPT_MAX_SNAPSHOT_AGE.to_owned(),
                self.max_snapshot_age_ms.to_string(),
            ),
            (
                OPT_MIN_SNAPSHOTS.to_owned(),
                self.min_snapshots_to_keep.to_string(),
            ),
            (OPT_MAX_REF_AGE.to_owned(), self.max_ref_age_ms.to_string()),
            (
                OPT_METADATA_VERSIONS.to_owned(),
                self.metadata_versions.to_string(),
            ),
            (
                OPT_MANIFEST_TARGET_SIZE.to_owned(),
                self.manifest_target_size.to_string(),
            ),
            (
                OPT_MANIFEST_MIN_COUNT.to_owned(),
                self.manifest_min_count.to_string(),
            ),
            (
                OPT_MANIFEST_MERGE.to_owned(),
                self.manifest_merge.to_string(),
            ),
        ]);
        self.task_grouping.insert_properties(&mut properties);
        properties
    }

    /// Cache only values used by the relation-local write hot path.
    ///
    /// Write isolation and scan split sizing remain authoritative in Iceberg
    /// metadata and are resolved from table properties when a session opens.
    pub(super) fn into_cache(self) -> IcebergTableOptionCache {
        IcebergTableOptionCache {
            format_version: self.format_version,
            compression: self.compression,
            write_format: self.write_format,
        }
    }
}
