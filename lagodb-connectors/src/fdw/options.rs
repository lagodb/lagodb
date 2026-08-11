//! FDW catalog-option routing and foreign-table option resolution.

use pg_lakebase_core::storage::foreign::ForeignOptionView;
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::{
    FormatKind, FormatOption, ResolvedForeignFormat, StreamCompression,
};
use crate::storage::{ObjectUri, validate_storage_options};

pub(crate) fn validate_catalog_options(
    options: &[Option<String>],
    catalog: Option<pg_sys::Oid>,
) -> Result<(), ConnectorError> {
    match catalog {
        Some(catalog) if catalog == pg_sys::ForeignTableRelationId => {
            RawTableOptions::parse(options)?.resolve().map(|_| ())
        }
        Some(catalog) if catalog == pg_sys::AttributeRelationId => {
            ResolvedForeignFormat::validate_column_catalog_options(options)
        }
        _ => validate_storage_options(options, catalog),
    }
}

pub(crate) fn resolve_table_options(
    options: ForeignOptionView<'_>,
) -> Result<ResolvedTableOptions, ConnectorError> {
    RawTableOptions::parse_view(options)?.resolve()
}

pub(crate) struct ResolvedTableOptions {
    pub(crate) object: ObjectUri,
    pub(crate) format: ResolvedForeignFormat,
}

struct RawTableOptions<'a> {
    object: Option<ObjectUri>,
    kind: Option<FormatKind>,
    compression: Option<&'a str>,
    format_options: Vec<FormatOption<'a>>,
}

impl<'a> RawTableOptions<'a> {
    fn empty() -> Self {
        Self {
            object: None,
            kind: None,
            compression: None,
            format_options: Vec::new(),
        }
    }

    fn parse(options: &'a [Option<String>]) -> Result<Self, ConnectorError> {
        let mut parsed = Self::empty();
        for option in options.iter().flatten() {
            let (name, value) = option.split_once('=').ok_or_else(|| {
                ConnectorError::invalid_option(
                    "foreign table option",
                    "expected name=value",
                )
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    fn parse_view(options: ForeignOptionView<'a>) -> Result<Self, ConnectorError> {
        let mut parsed = Self::empty();
        for option in options.iter() {
            let name = option.name().to_str().map_err(|_| {
                ConnectorError::invalid_option(
                    "foreign table option",
                    "must be valid UTF-8",
                )
            })?;
            let value = option.value_str().map_err(|_| {
                ConnectorError::invalid_option(name, "must be valid UTF-8")
            })?;
            parsed.set(name, value)?;
        }
        Ok(parsed)
    }

    fn set(&mut self, name: &'a str, value: &'a str) -> Result<(), ConnectorError> {
        match name {
            "path" => {
                Self::set_once(&mut self.object, name, ObjectUri::parse(value)?)
            }
            "format" => {
                let kind = FormatKind::parse(value)
                    .ok_or_else(|| ConnectorError::invalid_format(value))?;
                Self::set_once(&mut self.kind, name, kind)
            }
            "compression" => Self::set_once(&mut self.compression, name, value),
            _ => {
                self.format_options.push(FormatOption::new(name, value));
                Ok(())
            }
        }
    }

    fn set_once<T>(
        target: &mut Option<T>,
        name: &str,
        value: T,
    ) -> Result<(), ConnectorError> {
        if target.replace(value).is_some() {
            return Err(ConnectorError::invalid_option(
                name,
                "must not be specified more than once",
            ));
        }
        Ok(())
    }

    fn resolve(self) -> Result<ResolvedTableOptions, ConnectorError> {
        let object = self.object.ok_or_else(|| {
            ConnectorError::invalid_option("path", "is required for a foreign table")
        })?;
        let kind = self
            .kind
            .or_else(|| FormatKind::infer_from_key(object.key()))
            .ok_or_else(ConnectorError::format_required)?;
        let suffix_compression = StreamCompression::from_suffix(object.key());
        if suffix_compression.is_some()
            && matches!(kind, FormatKind::Parquet | FormatKind::Avro)
        {
            return Err(ConnectorError::invalid_option(
                "compression",
                "compression suffixes are supported only for text, csv, and json",
            ));
        }
        Ok(ResolvedTableOptions {
            object,
            format: ResolvedForeignFormat::resolve(
                kind,
                self.compression,
                suffix_compression,
                &self.format_options,
            )?,
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod fdw_options_tests {
    use super::*;
    use pgrx::pg_test;

    #[pg_test]
    fn explicit_compression_overrides_object_suffix() {
        let options = [
            Some("path=s3://bucket/data.csv.gz".to_owned()),
            Some("format=csv".to_owned()),
            Some("compression=none".to_owned()),
        ];
        let resolved = RawTableOptions::parse(&options).unwrap().resolve().unwrap();
        assert_eq!(
            resolved.format.stream_compression(),
            Some(StreamCompression::None)
        );
    }

    #[pg_test]
    fn stream_compression_is_inferred_from_supported_suffixes() {
        for (suffix, expected) in [
            ("gz", StreamCompression::Gzip),
            ("gzip", StreamCompression::Gzip),
            ("zst", StreamCompression::Zstd),
            ("zstd", StreamCompression::Zstd),
        ] {
            let options = [Some(format!("path=s3://bucket/data.csv.{suffix}"))];
            let resolved =
                RawTableOptions::parse(&options).unwrap().resolve().unwrap();
            assert_eq!(resolved.format.kind(), FormatKind::Csv);
            assert_eq!(resolved.format.stream_compression(), Some(expected));
        }
    }
}
