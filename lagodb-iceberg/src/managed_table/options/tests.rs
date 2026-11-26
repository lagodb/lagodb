use std::mem::size_of_val;

use iceberg_lite::spec::FormatVersion;
use lagodb_core::options::table::{TableOptionError, TableOptions};
use parquet::basic::Compression as ParquetCompression;

use super::resolved::CompressionOption;
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
fn reset_to_empty_overrides_matches_create_without_options() {
    let create_without_options =
        ResolvedIcebergOptions::from_table_options(None).unwrap();
    let empty_overrides = TableOptions::new(Vec::new());
    let after_reset =
        ResolvedIcebergOptions::from_table_options(Some(&empty_overrides)).unwrap();

    assert_eq!(after_reset, create_without_options);
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
        assert_eq!(properties.get(option).map(String::as_str), Some("snapshot"));
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

    assert_eq!(size_of_val(&cache), 3);
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
fn managed_table_rejects_location_and_gc_overrides() {
    for option in [
        "write.data.path",
        "write.metadata.path",
        "write.folder-storage.path",
        "gc.enabled",
    ] {
        let options =
            TableOptions::new(vec![(option.to_owned(), Some("unsafe".to_owned()))]);
        assert!(matches!(
            ResolvedIcebergOptions::from_table_options(Some(&options)),
            Err(TableOptionError::InvalidOption(_)),
        ));
    }
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
