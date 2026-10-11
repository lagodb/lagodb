//! Format configuration, schemas, and shared file codecs.
//!
//! FDW capabilities and execution state live in `fdw::format_adapters`.
//! COPY adapters reuse the file readers, writers, and predicates exposed here.

pub(crate) mod avro;
mod compression;
mod compression_options;
mod copy;
mod csv;
mod delimited;
mod delimited_schema;
pub(crate) mod json;
mod location;
mod object_writer;
pub(crate) mod parquet;
mod resolved;
mod schema;
mod stream_writer;
mod text;

use std::fmt::{self, Display, Formatter};

pub(crate) use avro::AvroFormat;
pub(crate) use compression::{StreamDecoder, StreamEncoder};
pub(crate) use compression_options::{
    AvroWriteCompression, ParquetWriteCompression, StreamCompression,
    StreamCompressionOptions,
};
pub(crate) use copy::{FormatCopyInput, FormatCopyOutput, ResolvedCopyFormat};
pub(crate) use csv::{CsvColumnOptions, CsvFormat, CsvOptions};
pub(crate) use delimited::{DelimitedFormat, DelimitedOptions};
pub(crate) use json::JsonFormat;
pub(crate) use object_writer::{
    EmptyOutputPolicy, FileWriteProgress, ObjectFileEncoder,
    ObjectFileEncoderFactory, ObjectSetWriter,
};
pub(crate) use parquet::ParquetFormat;
pub(crate) use resolved::{FormatOption, ResolvedForeignFormat};
pub(crate) use schema::{
    FormatSchemaReader, InferredColumn, InferredSchema, PostgresType,
    SCHEMA_SAMPLE_RECORDS,
};
pub(crate) use stream_writer::{StreamEncoderFactory, StreamFormat};
pub(crate) use text::TextFormat;

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
    /// format. Stream compression is part of text/CSV/JSON object syntax, while
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
