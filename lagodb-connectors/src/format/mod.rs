//! Format identity, codec capabilities, and cold-path factories.
//!
//! Reader, writer, and predicate capabilities remain independent traits. FDW
//! and COPY adapters may share concrete codecs without sharing execution state.

mod avro;
mod codec;
mod compression;
mod copy;
mod csv;
mod delimited;
mod delimited_schema;
mod delimited_scan;
mod delimited_write;
mod filter;
mod json;
mod object_writer;
mod parquet;
mod resolved;
mod scan;
mod schema;
mod stream_writer;
mod text;
mod write;

use std::fmt::{self, Display, Formatter};

use pg_lakebase_core::plan_data::PlanDataReader;

use crate::error::ConnectorError;

pub(crate) use codec::{
    AvroWriteCompression, ParquetWriteCompression, StreamCompression,
};
pub(crate) use copy::{
    FormatCopyDestination, FormatCopySource, ResolvedCopyFormat,
};
pub(crate) use filter::{
    FormatBoundFilter, FormatFilterPlanner, FormatPlannedFilter,
};
pub(crate) use object_writer::{
    EmptyOutputPolicy, FileWriteProgress, ObjectFileEncoder,
    ObjectFileEncoderFactory, ObjectSetWriter,
};
pub(crate) use resolved::{FormatOption, ResolvedForeignFormat};
pub(crate) use scan::{
    FormatReader, FormatScanPlanner, FormatScanPrivate, FormatScanState,
};
pub(crate) use schema::{
    FormatSchemaReader, InferredColumn, InferredSchema, PostgresType,
    StorageFileCopySource, StorageFileReader, SCHEMA_SAMPLE_RECORDS,
};
pub(crate) use write::{FormatWritePrivate, FormatWriteState, FormatWriter};

use avro::AvroFormat;
pub(crate) use compression::{StreamDecoder, StreamEncoder};
pub(crate) use stream_writer::StreamEncoderFactory;
use csv::CsvFormat;
use json::JsonFormat;
use parquet::ParquetFormat;
pub(crate) use parquet::{
    ParquetObjectReader, ParquetObjectWriter, parquet_arrow_type,
};
use text::TextFormat;

/// A supported external object format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatKind {
    Text,
    Csv,
    Json,
    Avro,
    Parquet,
}

impl FormatKind {
    const TEXT_WIRE: i32 = 0;
    const CSV_WIRE: i32 = 1;
    const JSON_WIRE: i32 = 2;
    const AVRO_WIRE: i32 = 3;
    const PARQUET_WIRE: i32 = 4;

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "text" => Some(Self::Text),
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            "avro" => Some(Self::Avro),
            "parquet" => Some(Self::Parquet),
            _ => None,
        }
    }

    pub(crate) fn from_suffix(key: &str) -> Option<Self> {
        match key.rsplit_once('.')?.1 {
            "txt" | "text" => Some(Self::Text),
            "csv" => Some(Self::Csv),
            "json" | "jsonl" | "ndjson" => Some(Self::Json),
            "avro" => Some(Self::Avro),
            "parquet" => Some(Self::Parquet),
            _ => None,
        }
    }

    pub(crate) fn infer_from_key(key: &str) -> Option<Self> {
        let key_without_compression = StreamCompression::from_suffix(key)
            .map(|_| key.rsplit_once('.').map_or(key, |(stem, _)| stem))
            .unwrap_or(key);
        Self::from_suffix(key_without_compression)
    }

    /// Whether one concrete object key carries a suffix supported by this
    /// format. Stream wrappers are part of text/CSV/JSON object syntax, while
    /// Parquet and Avro compression belongs inside the container.
    pub(crate) fn matches_object_key(self, key: &str) -> bool {
        match self {
            Self::Avro | Self::Parquet => Self::from_suffix(key) == Some(self),
            Self::Text | Self::Csv | Self::Json => {
                Self::infer_from_key(key) == Some(self)
            }
        }
    }

    #[inline]
    pub(crate) const fn wire(self) -> i32 {
        match self {
            Self::Text => Self::TEXT_WIRE,
            Self::Csv => Self::CSV_WIRE,
            Self::Json => Self::JSON_WIRE,
            Self::Avro => Self::AVRO_WIRE,
            Self::Parquet => Self::PARQUET_WIRE,
        }
    }

    #[inline]
    pub(crate) const fn from_wire(value: i32) -> Option<Self> {
        match value {
            Self::TEXT_WIRE => Some(Self::Text),
            Self::CSV_WIRE => Some(Self::Csv),
            Self::JSON_WIRE => Some(Self::Json),
            Self::AVRO_WIRE => Some(Self::Avro),
            Self::PARQUET_WIRE => Some(Self::Parquet),
            _ => None,
        }
    }

    pub(crate) fn decode_filter(
        self,
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<FormatPlannedFilter, ConnectorError> {
        match self {
            Self::Text => TextFormat::decode_filter(self, reader, binding_count),
            Self::Csv => CsvFormat::decode_filter(self, reader, binding_count),
            Self::Json => JsonFormat::decode_filter(self, reader, binding_count),
            Self::Avro => AvroFormat::decode_filter(self, reader, binding_count),
            Self::Parquet => {
                ParquetFormat::decode_filter(self, reader, binding_count)
            }
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Avro => "avro",
            Self::Parquet => "parquet",
        }
    }
}

/// Common identity of one concrete format implementation.
pub(crate) trait FormatObject: 'static {
    fn kind(&self) -> FormatKind;
}

impl Display for FormatKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
