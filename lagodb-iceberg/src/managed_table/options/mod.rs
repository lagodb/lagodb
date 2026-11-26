//! Iceberg table option definitions, semantic resolution, and `rd_amcache`.
//!
//! This module owns the schema layer for Iceberg reloptions: option names,
//! defaults, semantic validation (`ResolvedIcebergOptions`), and the compact
//! writer cache (`IcebergTableOptions`). The DDL hooks that extract and persist
//! options live in `crate::managed_table::hooks::table_ddl`.

mod cache;
mod resolved;
#[cfg(test)]
mod tests;

use iceberg_lite::spec::{IsolationLevel, TableProperties};
use lagodb_core::options::{OptionAccess, OptionDef, OptionKind};

use crate::scan::parallel::TaskGrouping;

pub(crate) use cache::{IcebergTableOptions, MANAGED_ROW_GROUP_MAX_ROWS};
pub(crate) use resolved::ResolvedIcebergOptions;

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
pub const OPT_COMPRESSION_CODEC_DEFAULT: &str =
    resolved::CompressionOption::Zstd.as_str();
pub const OPT_COMPRESSION_CODEC_VALUES: &[&str] = &[
    resolved::CompressionOption::Snappy.as_str(),
    resolved::CompressionOption::Zstd.as_str(),
];

/// File format used for writes. The writer currently supports Parquet only.
pub const OPT_WRITE_FORMAT: &str = "write.format.default";
pub const OPT_WRITE_FORMAT_DEFAULT: &str =
    resolved::WriteFormatOption::Parquet.as_str();
pub const OPT_WRITE_FORMAT_VALUES: &[&str] =
    &[resolved::WriteFormatOption::Parquet.as_str()];

/// Command-specific Iceberg row-level write isolation.
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

// Each default is declared once here and shared by both the `OptionDef` that
// CREATE applies and the `from_table_options` fallback, so the two paths cannot
// drift. The string form matches the Iceberg table-property wire value.
pub const OPT_TARGET_FILE_SIZE: &str =
    TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES;
pub const OPT_TARGET_FILE_SIZE_DEFAULT: &str = "536870912";
pub const OPT_MAX_SNAPSHOT_AGE: &str = TableProperties::PROPERTY_MAX_SNAPSHOT_AGE_MS;
pub const OPT_MAX_SNAPSHOT_AGE_DEFAULT: &str = "432000000";
pub const OPT_MIN_SNAPSHOTS: &str = TableProperties::PROPERTY_MIN_SNAPSHOTS_TO_KEEP;
pub const OPT_MIN_SNAPSHOTS_DEFAULT: &str = "1";
pub const OPT_MAX_REF_AGE: &str = TableProperties::PROPERTY_MAX_REF_AGE_MS;
pub const OPT_MAX_REF_AGE_DEFAULT: &str = "9223372036854775807";
pub const OPT_METADATA_VERSIONS: &str =
    TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX;
pub const OPT_METADATA_VERSIONS_DEFAULT: &str = "100";
pub const OPT_MANIFEST_TARGET_SIZE: &str =
    TableProperties::PROPERTY_MANIFEST_TARGET_SIZE_BYTES;
pub const OPT_MANIFEST_TARGET_SIZE_DEFAULT: &str = "8388608";
pub const OPT_MANIFEST_MIN_COUNT: &str =
    TableProperties::PROPERTY_MANIFEST_MIN_COUNT_TO_MERGE;
pub const OPT_MANIFEST_MIN_COUNT_DEFAULT: &str = "100";
pub const OPT_MANIFEST_MERGE: &str = TableProperties::PROPERTY_MANIFEST_MERGE_ENABLED;
pub const OPT_MANIFEST_MERGE_DEFAULT: bool = true;

/// Iceberg parallel scan split sizing and bin-packing options.
///
/// Updates staged by `ALTER TABLE ... SET/RESET` take effect on scans after the
/// top-level transaction commits its new Iceberg metadata.
pub const OPT_READ_SPLIT_TARGET_SIZE: &str = TaskGrouping::PROPERTY_TARGET_SIZE;
pub const OPT_READ_SPLIT_TARGET_SIZE_DEFAULT: &str =
    TaskGrouping::DEFAULT_TARGET_SIZE_STR;
pub const OPT_READ_SPLIT_PLANNING_LOOKBACK: &str =
    TaskGrouping::PROPERTY_PLANNING_LOOKBACK;
pub const OPT_READ_SPLIT_PLANNING_LOOKBACK_DEFAULT: &str =
    TaskGrouping::DEFAULT_PLANNING_LOOKBACK_STR;
pub const OPT_READ_SPLIT_OPEN_FILE_COST: &str = TaskGrouping::PROPERTY_OPEN_FILE_COST;
pub const OPT_READ_SPLIT_OPEN_FILE_COST_DEFAULT: &str =
    TaskGrouping::DEFAULT_OPEN_FILE_COST_STR;

/// Stable object-storage identity generated and owned by the access method.
pub const OPT_LOCATION: &str = "location";

pub const LOCATION_OPTION_DEF: OptionDef = OptionDef {
    name: OPT_LOCATION,
    access: OptionAccess::AccessMethodManaged,
    kind: OptionKind::String { default: None },
    description: "Iceberg table storage location managed by the access method",
};

// ============================================================================
//  Option Definitions
// ============================================================================

/// Iceberg-specific table options definition.
pub static ICEBERG_TABLE_OPTIONS: &[OptionDef] = &[
    LOCATION_OPTION_DEF,
    OptionDef {
        name: OPT_FORMAT_VERSION,
        access: OptionAccess::UserCreateOnly,
        kind: OptionKind::Int {
            default: OPT_FORMAT_VERSION_DEFAULT,
            min: Some(OPT_FORMAT_VERSION_MIN),
            max: Some(OPT_FORMAT_VERSION_MAX),
        },
        description: "Iceberg table format version (1, 2, or 3)",
    },
    OptionDef {
        name: OPT_COMPRESSION_CODEC,
        access: OptionAccess::UserMutable,
        kind: OptionKind::Enum {
            default: OPT_COMPRESSION_CODEC_DEFAULT,
            values: OPT_COMPRESSION_CODEC_VALUES,
        },
        description: "Parquet compression codec (snappy, zstd)",
    },
    OptionDef {
        name: OPT_WRITE_FORMAT,
        access: OptionAccess::UserMutable,
        kind: OptionKind::Enum {
            default: OPT_WRITE_FORMAT_DEFAULT,
            values: OPT_WRITE_FORMAT_VALUES,
        },
        description: "Default file format (parquet)",
    },
    OptionDef {
        name: OPT_WRITE_DELETE_ISOLATION_LEVEL,
        access: OptionAccess::UserMutable,
        kind: OptionKind::Enum {
            default: OPT_WRITE_ISOLATION_LEVEL_DEFAULT,
            values: OPT_WRITE_ISOLATION_LEVEL_VALUES,
        },
        description: "Iceberg DELETE isolation level (snapshot or serializable)",
    },
    OptionDef {
        name: OPT_WRITE_UPDATE_ISOLATION_LEVEL,
        access: OptionAccess::UserMutable,
        kind: OptionKind::Enum {
            default: OPT_WRITE_ISOLATION_LEVEL_DEFAULT,
            values: OPT_WRITE_ISOLATION_LEVEL_VALUES,
        },
        description: "Iceberg UPDATE isolation level (snapshot or serializable)",
    },
    OptionDef {
        name: OPT_WRITE_MERGE_ISOLATION_LEVEL,
        access: OptionAccess::UserMutable,
        kind: OptionKind::Enum {
            default: OPT_WRITE_ISOLATION_LEVEL_DEFAULT,
            values: OPT_WRITE_ISOLATION_LEVEL_VALUES,
        },
        description: "Iceberg MERGE isolation level (snapshot or serializable)",
    },
    OptionDef {
        name: OPT_TARGET_FILE_SIZE,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_TARGET_FILE_SIZE_DEFAULT),
        },
        description: "Target data-file size in bytes",
    },
    OptionDef {
        name: OPT_MAX_SNAPSHOT_AGE,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_MAX_SNAPSHOT_AGE_DEFAULT),
        },
        description: "Maximum snapshot age in milliseconds",
    },
    OptionDef {
        name: OPT_MIN_SNAPSHOTS,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_MIN_SNAPSHOTS_DEFAULT),
        },
        description: "Minimum snapshots retained per branch",
    },
    OptionDef {
        name: OPT_MAX_REF_AGE,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_MAX_REF_AGE_DEFAULT),
        },
        description: "Maximum non-main reference age in milliseconds",
    },
    OptionDef {
        name: OPT_METADATA_VERSIONS,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_METADATA_VERSIONS_DEFAULT),
        },
        description: "Maximum previous metadata versions retained",
    },
    OptionDef {
        name: OPT_MANIFEST_TARGET_SIZE,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_MANIFEST_TARGET_SIZE_DEFAULT),
        },
        description: "Target manifest size in bytes",
    },
    OptionDef {
        name: OPT_MANIFEST_MIN_COUNT,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_MANIFEST_MIN_COUNT_DEFAULT),
        },
        description: "Minimum live manifests before merging",
    },
    OptionDef {
        name: OPT_MANIFEST_MERGE,
        access: OptionAccess::UserMutable,
        kind: OptionKind::Bool {
            default: OPT_MANIFEST_MERGE_DEFAULT,
        },
        description: "Enable automatic manifest merging",
    },
    OptionDef {
        name: OPT_READ_SPLIT_TARGET_SIZE,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_READ_SPLIT_TARGET_SIZE_DEFAULT),
        },
        description: "Target scan split size in bytes",
    },
    OptionDef {
        name: OPT_READ_SPLIT_PLANNING_LOOKBACK,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_READ_SPLIT_PLANNING_LOOKBACK_DEFAULT),
        },
        description: "Number of open bins considered when packing scan splits",
    },
    OptionDef {
        name: OPT_READ_SPLIT_OPEN_FILE_COST,
        access: OptionAccess::UserMutable,
        kind: OptionKind::String {
            default: Some(OPT_READ_SPLIT_OPEN_FILE_COST_DEFAULT),
        },
        description: "Minimum byte weight per opened file when packing scan splits",
    },
];
