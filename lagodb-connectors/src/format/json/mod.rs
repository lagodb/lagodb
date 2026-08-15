//! Newline-delimited JSON object format.

mod record;
mod scalar;
mod scan;
mod schema;
mod stream;
mod write;

use std::io::BufReader;

use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignInsertBeginContext, ForeignModifyBeginContext,
    ForeignModifyCapabilities, ForeignModifyOperation, ForeignModifyPlanContext,
    ForeignModifyPlanSpec, ForeignModifyRelationContext,
};
use pg_lakebase_storage::StorageFile;

use crate::error::ConnectorError;
use crate::fdw::Lakebase;
use crate::gucs::ReadConfig;
use crate::storage::{ObjectFiles, ObjectOutput};

use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatScanPlanner,
    FormatScanState, FormatSchemaReader, FormatWritePrivate, FormatWriteState,
    FormatWriter, InferredSchema, StorageFileReader, StreamCompression,
    StreamDecoder,
};

pub(super) use record::{JsonColumnPlan, JsonInputValue, JsonRecordDecoder};
pub(super) use stream::JsonRecordStream;

/// JSON-format processor. Every non-empty line is one JSON object.
pub(crate) struct JsonFormat {
    pub(super) compression: StreamCompression,
}

impl JsonFormat {
    pub(crate) fn resolve(
        compression: StreamCompression,
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
        context: BeginForeignScanContext<'_, Lakebase>,
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
    fn capabilities(
        &self,
        _context: &ForeignModifyRelationContext<'_>,
    ) -> Result<ForeignModifyCapabilities, ConnectorError> {
        Ok(ForeignModifyCapabilities::new(true, false, false))
    }

    fn plan_modify(
        &self,
        context: &ForeignModifyPlanContext<'_>,
    ) -> Result<ForeignModifyPlanSpec<FormatWritePrivate>, ConnectorError> {
        if context.operation() != ForeignModifyOperation::Insert {
            return Err(ConnectorError::modify_not_implemented(FormatKind::Json));
        }
        Ok(ForeignModifyPlanSpec::new(FormatWritePrivate::new(
            FormatKind::Json,
        )))
    }

    fn begin_modify(
        self: Box<Self>,
        context: ForeignModifyBeginContext<'_, FormatWritePrivate>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        if context.operation() != ForeignModifyOperation::Insert {
            return Err(ConnectorError::modify_not_implemented(FormatKind::Json));
        }
        Ok(Box::new(write::JsonWriteState::begin(
            context.relation(),
            output,
            self.compression,
        )?))
    }

    fn begin_insert(
        self: Box<Self>,
        context: &mut ForeignInsertBeginContext<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Ok(Box::new(write::JsonWriteState::begin(
            context.relation(),
            output,
            self.compression,
        )?))
    }
}

impl FormatSchemaReader for JsonFormat {
    fn infer_schema(
        &self,
        file: &mut StorageFile,
    ) -> Result<InferredSchema, ConnectorError> {
        let source = StorageFileReader::new(file);
        let input = StreamDecoder::new(source, self.compression)
            .map_err(ConnectorError::json_io)?;
        schema::JsonSchemaAccumulator::default().read(
            BufReader::new(input),
            ReadConfig::from_guc().json_max_record_bytes(),
        )
    }
}
