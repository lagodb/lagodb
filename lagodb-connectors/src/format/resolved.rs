//! Format-specific option dispatch and validated format state.

use pg_lakebase_core::storage::foreign::ForeignOptionView;
use pg_lakebase_storage::StorageFile;
use pgrx::pg_sys;

use crate::error::ConnectorError;

use super::avro::AvroFormat;
use super::csv::CsvFormat;
use super::json::JsonFormat;
use super::parquet::ParquetFormat;
use super::text::TextFormat;
use super::{
    AvroWriteCompression, FormatKind, FormatReader, FormatSchemaReader, FormatWriter,
    InferredSchema, ParquetWriteCompression, StreamCompression,
};

/// One borrowed format-specific option after common foreign-table options have
/// been consumed.
#[derive(Clone, Copy)]
pub(crate) struct FormatOption<'a> {
    name: &'a str,
    value: &'a str,
}

impl<'a> FormatOption<'a> {
    pub(crate) const fn new(name: &'a str, value: &'a str) -> Self {
        Self { name, value }
    }

    pub(crate) const fn name(self) -> &'a str {
        self.name
    }

    pub(crate) const fn value(self) -> &'a str {
        self.value
    }
}

/// A format together with the options already parsed and validated by that
/// concrete format implementation.
pub(crate) enum ResolvedForeignFormat {
    Text(TextFormat),
    Csv(CsvFormat),
    Json(JsonFormat),
    Avro(AvroFormat),
    Parquet(ParquetFormat),
}

impl ResolvedForeignFormat {
    pub(crate) fn validate_column_catalog_options(
        options: &[Option<String>],
    ) -> Result<(), ConnectorError> {
        CsvFormat::validate_column_options(options)
    }

    pub(crate) fn resolve(
        kind: FormatKind,
        explicit_compression: Option<&str>,
        suffix_compression: Option<StreamCompression>,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        match kind {
            FormatKind::Text => Ok(Self::Text(TextFormat::resolve(
                Self::parse_stream_compression(
                    explicit_compression,
                    suffix_compression,
                )?,
                options,
            )?)),
            FormatKind::Csv => Ok(Self::Csv(CsvFormat::resolve(
                Self::parse_stream_compression(
                    explicit_compression,
                    suffix_compression,
                )?,
                options,
            )?)),
            FormatKind::Json => Ok(Self::Json(JsonFormat::resolve(
                Self::parse_stream_compression(
                    explicit_compression,
                    suffix_compression,
                )?,
                options,
            )?)),
            FormatKind::Avro => Ok(Self::Avro(AvroFormat::resolve(
                match explicit_compression {
                    Some(value) => {
                        AvroWriteCompression::parse(value).ok_or_else(|| {
                            ConnectorError::invalid_option(
                                "compression",
                                "must be none, deflate, snappy, or zstd for avro",
                            )
                        })?
                    }
                    None => AvroWriteCompression::default(),
                },
                options,
            )?)),
            FormatKind::Parquet => Ok(Self::Parquet(ParquetFormat::resolve(
                match explicit_compression {
                    Some(value) => {
                        ParquetWriteCompression::parse(value).ok_or_else(|| {
                            ConnectorError::invalid_option(
                                "compression",
                                "must be none, snappy, gzip, or zstd for parquet",
                            )
                        })?
                    }
                    None => ParquetWriteCompression::default(),
                },
                options,
            )?)),
        }
    }

    fn parse_stream_compression(
        explicit: Option<&str>,
        suffix: Option<StreamCompression>,
    ) -> Result<StreamCompression, ConnectorError> {
        match explicit {
            Some(value) => StreamCompression::parse(value).ok_or_else(|| {
                ConnectorError::invalid_option(
                    "compression",
                    "must be none, gzip, or zstd for a stream format",
                )
            }),
            None => Ok(suffix.unwrap_or(StreamCompression::None)),
        }
    }

    pub(crate) const fn kind(&self) -> FormatKind {
        match self {
            Self::Text(_) => FormatKind::Text,
            Self::Csv(_) => FormatKind::Csv,
            Self::Json(_) => FormatKind::Json,
            Self::Avro(_) => FormatKind::Avro,
            Self::Parquet(_) => FormatKind::Parquet,
        }
    }

    #[cfg(any(test, feature = "pg_test"))]
    pub(crate) const fn stream_compression(&self) -> Option<StreamCompression> {
        match self {
            Self::Text(format) => Some(format._compression),
            Self::Csv(format) => Some(format._compression),
            Self::Json(format) => Some(format.compression),
            Self::Avro(_) | Self::Parquet(_) => None,
        }
    }

    pub(crate) fn validate_column_view(
        &self,
        options: ForeignOptionView<'_>,
    ) -> Result<(), ConnectorError> {
        match self {
            Self::Csv(_) => CsvFormat::validate_column_view(options),
            _ if options.iter().next().is_none() => Ok(()),
            _ => Err(ConnectorError::invalid_option(
                "foreign column option",
                "force_null and force_not_null are only valid for csv",
            )),
        }
    }

    pub(crate) fn validate_relation_columns(
        &self,
        relation_oid: pg_sys::Oid,
        natts: usize,
    ) -> Result<(), ConnectorError> {
        for attno in 1..=natts {
            // PostgreSQL bounds TupleDesc::natts by AttrNumber, so this
            // relation-level cold-path conversion cannot truncate.
            // SAFETY: the FDW callback supplies a live foreign relation and
            // every attno is within its TupleDesc.
            let options = unsafe {
                pg_sys::GetForeignColumnOptions(
                    relation_oid,
                    attno as pg_sys::AttrNumber,
                )
            };
            // SAFETY: PostgreSQL owns this option list for the duration of the
            // catalog lookup and validation call.
            self.validate_column_view(unsafe {
                ForeignOptionView::from_raw(options)
            })?;
        }
        Ok(())
    }

    pub(crate) fn into_reader(self) -> Box<dyn FormatReader> {
        match self {
            Self::Text(format) => Box::new(format),
            Self::Csv(format) => Box::new(format),
            Self::Json(format) => Box::new(format),
            Self::Avro(format) => Box::new(format),
            Self::Parquet(format) => Box::new(format),
        }
    }

    pub(crate) fn into_writer(self) -> Box<dyn FormatWriter> {
        match self {
            Self::Text(format) => Box::new(format),
            Self::Csv(format) => Box::new(format),
            Self::Json(format) => Box::new(format),
            Self::Avro(format) => Box::new(format),
            Self::Parquet(format) => Box::new(format),
        }
    }

    pub(crate) fn infer_schema(
        &self,
        file: &mut StorageFile,
    ) -> Result<InferredSchema, ConnectorError> {
        match self {
            Self::Json(format) => format.infer_schema(file),
            Self::Avro(format) => format.infer_schema(file),
            Self::Parquet(format) => format.infer_schema(file),
            Self::Text(_) | Self::Csv(_) => {
                Err(ConnectorError::schema_inference_unsupported(
                    self.kind(),
                    "the format has no embedded schema; specify foreign-table columns explicitly",
                ))
            }
        }
    }
}
