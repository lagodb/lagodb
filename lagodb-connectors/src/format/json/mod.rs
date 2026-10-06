//! Newline-delimited JSON object format.

mod record;
mod scalar;
mod scan;
mod schema;
mod stream;
mod write;

use std::io::BufReader;

use crate::storage::InputFile;
use lagodb_core::fdw::StartForeignScanContext;
use lagodb_core::handles::RelationHandle;

use crate::error::ConnectorError;
use crate::fdw::LagodbConnectors;
use crate::gucs::ReadConfig;
use crate::storage::{ObjectFiles, ObjectOutput};

use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatScanPlanner,
    FormatScanState, FormatSchemaReader, FormatWriteState, FormatWriter,
    InferredSchema, StreamCompressionOptions, StreamDecoder,
};

pub(super) use record::{JsonColumnPlan, JsonInputValue, JsonRecordDecoder};
pub(super) use stream::JsonRecordStream;

/// JSON-format processor. Every non-empty line is one JSON object.
pub(crate) struct JsonFormat {
    pub(super) compression: StreamCompressionOptions,
}

impl JsonFormat {
    pub(crate) fn resolve(
        compression: StreamCompressionOptions,
        options: &[FormatOption<'_>],
    ) -> Result<Self, ConnectorError> {
        if let Some(option) = options.first() {
            return Err(ConnectorError::invalid_option(
                option.name(),
                "is not valid for json",
            ));
        }
        Ok(Self { compression })
    }
}

impl FormatObject for JsonFormat {
    fn kind(&self) -> FormatKind {
        FormatKind::Json
    }
}

impl FormatReader for JsonFormat {
    fn planner(self: Box<Self>) -> Box<dyn FormatScanPlanner> {
        Box::new(scan::JsonScanPlanner)
    }

    fn begin(
        self: Box<Self>,
        context: StartForeignScanContext<'_, LagodbConnectors>,
        files: ObjectFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        Ok(Box::new(scan::JsonScanState::begin(
            context,
            files,
            self.compression,
        )?))
    }
}

impl FormatWriter for JsonFormat {
    fn begin(
        self: Box<Self>,
        relation: &RelationHandle<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Ok(Box::new(write::JsonWriteState::begin(
            relation,
            output,
            self.compression.for_output(),
        )?))
    }
}

impl FormatSchemaReader for JsonFormat {
    fn infer_schema(
        &self,
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError> {
        let compression = self.compression.for_file(file);
        let input =
            StreamDecoder::new(file, compression).map_err(ConnectorError::json_io)?;
        schema::JsonSchemaAccumulator::default().read(
            BufReader::new(input),
            ReadConfig::from_guc().json_max_record_bytes(),
        )
    }
}
