//! Cold-path selection of format-specific FDW implementations.

use lagodb_core::plan_data::PlanDataReader;

use crate::error::ConnectorError;
use crate::format::parquet::ParquetPlannedPredicate;
use crate::format::{FormatKind, ResolvedForeignFormat};

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
        // Executor plan decoding has no relation catalog from which to
        // rebuild configured format state; the plan tag selects the decoder.
        match kind {
            FormatKind::Parquet => {
                ParquetPlannedPredicate::decode(kind, reader, binding_count)
                    .map(|predicate| Box::new(predicate) as FormatPlannedFilter)
            }
            FormatKind::Text
            | FormatKind::Csv
            | FormatKind::Json
            | FormatKind::Avro => Err(ConnectorError::invalid_filter_plan(kind)),
        }
    }
}
