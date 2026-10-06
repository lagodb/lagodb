//! Parquet format configuration and capability composition.

mod analyze;
mod copy;
mod filter;
mod reader;
mod scan;
mod schema;
mod write;
mod writer;

use crate::storage::InputFile;
use lagodb_core::expr::pushdown::FilterPlanningContext;
use lagodb_core::fdw::StartForeignScanContext;
use lagodb_core::handles::RelationHandle;
use lagodb_core::plan_data::PlanDataReader;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::storage::{ObjectFiles, ObjectOutput};

use super::{
    FormatAnalyzer, FormatFilterPlanner, FormatKind, FormatObject, FormatOption,
    FormatPlannedFilter, FormatReader, FormatScanPlanner, FormatScanState,
    FormatSchemaReader, FormatWriteState, FormatWriter, InferredSchema,
    ParquetWriteCompression,
};

pub(super) use copy::{ParquetCopyDestination, ParquetCopySource};
pub(crate) use filter::{ParquetBoundPredicate, ParquetFilePredicate};
pub(crate) use reader::ParquetObjectReader;
pub(crate) use schema::parquet_arrow_type;
pub(crate) use writer::ParquetObjectWriter;

/// Parquet-format processor.
pub(crate) struct ParquetFormat {
    pub(super) write_compression: ParquetWriteCompression,
}

impl ParquetFormat {
    pub(crate) fn resolve(
        write_compression: ParquetWriteCompression,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        if let Some(option) = options.first() {
            return Err(ConnectorError::invalid_option(
                option.name(),
                "is not valid for parquet",
            ));
        }
        Ok(Self { write_compression })
    }
}

impl FormatObject for ParquetFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Parquet
    }
}

impl FormatReader for ParquetFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(scan::ParquetScanPlanner::new())
    }

    fn begin_filter_planning(
        self: Box<Self>,
        context: &FilterPlanningContext,
    ) -> Result<Box<dyn FormatFilterPlanner>, ConnectorError> {
        Ok(Box::new(filter::ParquetFilterPlanner::begin(context)?))
    }

    fn decode_filter(
        kind: FormatKind,
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<FormatPlannedFilter, ConnectorError> {
        filter::ParquetPlannedPredicate::decode(kind, reader, binding_count)
            .map(|predicate| Box::new(predicate) as FormatPlannedFilter)
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: ObjectFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        Ok(Box::new(scan::ParquetScanState::begin(context, files)?))
    }

    fn analyzer(self: Box<Self>) -> Option<Box<dyn FormatAnalyzer>> {
        Some(Box::new(analyze::ParquetAnalyze))
    }
}

impl FormatWriter for ParquetFormat {
    fn begin(
        self: Box<Self>,
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Ok(Box::new(write::ParquetWriteState::begin(
            relation,
            output,
            self.write_compression,
        )?))
    }
}

impl FormatSchemaReader for ParquetFormat {
    fn infer_schema(
        &self,
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError> {
        schema::infer(file)
    }
}
