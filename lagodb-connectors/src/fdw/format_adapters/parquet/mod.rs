//! FDW capabilities for the Parquet format.

mod analyze;
mod filter;
mod scan;
mod write;

use lagodb_core::expr::pushdown::FilterPlanningContext;
use lagodb_core::fdw::StartForeignScanContext;
use lagodb_core::handles::RelationHandle;
use lagodb_core::plan_data::PlanDataReader;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::format::parquet::{ParquetFilterPlanner, ParquetPlannedPredicate};
use crate::format::{FormatKind, ParquetFormat};
use crate::storage::{InputFiles, StorageOutput};

use super::{
    FormatAnalyzer, FormatFdwReader, FormatFilterPlanner, FormatPlannedFilter,
    FormatScanPlanner, FormatScanState, FormatWriteState, FormatWriter,
};

impl FormatFdwReader for ParquetFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(scan::ParquetScanPlanner::new())
    }

    fn begin_filter_planning(
        self: Box<Self>,
        context: &FilterPlanningContext,
    ) -> Result<Box<dyn FormatFilterPlanner>, ConnectorError> {
        Ok(Box::new(ParquetFilterPlanner::begin(context)?))
    }

    fn decode_filter(
        kind: FormatKind,
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<FormatPlannedFilter, ConnectorError> {
        ParquetPlannedPredicate::decode(kind, reader, binding_count)
            .map(|predicate| Box::new(predicate) as FormatPlannedFilter)
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: InputFiles,
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
        output: StorageOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Ok(Box::new(write::ParquetFdwWriteState::begin(
            relation,
            output,
            self.write_compression(),
        )?))
    }
}
