//! FDW capabilities for the Text format.

use lagodb_core::fdw::StartForeignScanContext;
use lagodb_core::handles::RelationHandle;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::format::{DelimitedFormat, FormatKind, TextFormat};
use crate::storage::{InputFiles, StorageOutput};

use super::delimited_options::DelimitedScanOptions;
use super::delimited_scan::{DelimitedScanPlanner, DelimitedScanState};
use super::delimited_write::DelimitedWriteState;
use super::{
    FormatFdwReader, FormatScanPlanner, FormatScanState, FormatWriteState,
    FormatWriter,
};

impl FormatFdwReader for TextFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(DelimitedScanPlanner::new(FormatKind::Text))
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: InputFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        let (options, compression) = (*self).into_parts();
        let mut postgres_options =
            DelimitedScanOptions::new(options.postgres_options()?);
        postgres_options
            .select_columns(&context.relation, context.required_columns)?;
        Ok(Box::new(DelimitedScanState::begin(
            context,
            files,
            compression,
            postgres_options.into_raw(),
        )?))
    }
}

impl FormatWriter for TextFormat {
    fn begin(
        self: Box<Self>,
        relation: &RelationHandle<'_>,
        output: StorageOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        let (options, compression) = (*self).into_parts();
        let postgres_options = options.postgres_options()?;
        Ok(Box::new(DelimitedWriteState::begin(
            relation,
            output,
            DelimitedFormat::Text,
            compression.for_output(),
            postgres_options,
            false,
        )?))
    }
}
