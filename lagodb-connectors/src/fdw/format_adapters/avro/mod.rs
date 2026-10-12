//! FDW capabilities for the Avro format.

mod scan;
mod write;

use lagodb_core::fdw::StartForeignScanContext;
use lagodb_core::handles::RelationHandle;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::format::AvroFormat;
use crate::storage::{InputFiles, StorageOutput};

use super::{
    FormatFdwReader, FormatScanPlanner, FormatScanState, FormatWriteState,
    FormatWriter,
};
use scan::AvroScanState;
use write::AvroWriteState;

impl FormatFdwReader for AvroFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(scan::AvroScanPlanner::new())
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: InputFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        Ok(Box::new(AvroScanState::begin(context, files)?))
    }
}

impl FormatWriter for AvroFormat {
    fn begin(
        self: Box<Self>,
        relation: &RelationHandle<'_>,
        output: StorageOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Ok(Box::new(AvroWriteState::begin(
            relation,
            output,
            self.write_compression(),
        )?))
    }
}
