//! FDW capabilities for the Parquet format.

mod analyze;
mod filter;
mod scan;
mod write;

use lagodb_core::expr::pushdown::FilterPlanningContext;
use lagodb_core::fdw::StartForeignScanContext;
use lagodb_core::handles::RelationHandle;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::format::ParquetFormat;
use crate::format::parquet::ParquetFilterPlanner;
use crate::storage::{InputFiles, StorageOutput};

use super::{
    FormatAnalyzer, FormatFdwReader, FormatFilterPlanner, FormatScanPlanner,
    FormatScanState, FormatWriteState, FormatWriter,
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
