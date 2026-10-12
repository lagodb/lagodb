//! FDW capabilities for the JSON format.

mod scan;
mod write;

use lagodb_core::fdw::StartForeignScanContext;
use lagodb_core::handles::RelationHandle;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::format::JsonFormat;
use crate::storage::{InputFiles, StorageOutput};

use super::{
    FormatFdwReader, FormatScanPlanner, FormatScanState, FormatWriteState,
    FormatWriter,
};

impl FormatFdwReader for JsonFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(scan::JsonScanPlanner)
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: InputFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        Ok(Box::new(scan::JsonScanState::begin(
            context,
            files,
            self.compression(),
        )?))
    }
}

impl FormatWriter for JsonFormat {
    fn begin(
        self: Box<Self>,
        relation: &RelationHandle<'_>,
        output: StorageOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Ok(Box::new(write::JsonWriteState::begin(
            relation,
            output,
            self.compression().for_output(),
        )?))
    }
}
