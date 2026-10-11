//! Cold-path selection of format-specific FDW implementations.

use lagodb_core::plan_data::PlanDataReader;

use crate::error::ConnectorError;
use crate::format::{
    AvroFormat, CsvFormat, FormatKind, JsonFormat, ParquetFormat,
    ResolvedForeignFormat, TextFormat,
};

use super::{FormatFdwReader, FormatPlannedFilter, FormatWriter};

pub(in crate::fdw) struct FdwFormat;

impl FdwFormat {
    pub(crate) fn reader(format: ResolvedForeignFormat) -> Box<dyn FormatFdwReader> {
        match format {
            ResolvedForeignFormat::Text(format) => Box::new(format),
            ResolvedForeignFormat::Csv(format) => Box::new(format),
            ResolvedForeignFormat::Json(format) => Box::new(format),
            ResolvedForeignFormat::Avro(format) => Box::new(format),
            ResolvedForeignFormat::Parquet(format) => Box::new(format),
        }
    }

    pub(crate) fn writer(format: ResolvedForeignFormat) -> Box<dyn FormatWriter> {
        match format {
            ResolvedForeignFormat::Text(format) => Box::new(format),
            ResolvedForeignFormat::Csv(format) => Box::new(format),
            ResolvedForeignFormat::Json(format) => Box::new(format),
            ResolvedForeignFormat::Avro(format) => Box::new(format),
            ResolvedForeignFormat::Parquet(format) => Box::new(format),
        }
    }

    pub(crate) fn decode_filter(
        kind: FormatKind,
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<FormatPlannedFilter, ConnectorError> {
        match kind {
            FormatKind::Text => {
                TextFormat::decode_filter(kind, reader, binding_count)
            }
            FormatKind::Csv => CsvFormat::decode_filter(kind, reader, binding_count),
            FormatKind::Json => {
                JsonFormat::decode_filter(kind, reader, binding_count)
            }
            FormatKind::Avro => {
                AvroFormat::decode_filter(kind, reader, binding_count)
            }
            FormatKind::Parquet => {
                ParquetFormat::decode_filter(kind, reader, binding_count)
            }
        }
    }
}
