//! Parquet format configuration and capability composition.

mod copy;
mod reader;
mod scan;
mod schema;
mod write;
mod writer;

use pg_lakebase_core::fdw::{
    BeginForeignScanContext, ForeignInsertBeginContext, ForeignModifyBeginContext,
    ForeignModifyCapabilities, ForeignModifyOperation, ForeignModifyPlanContext,
    ForeignModifyPlanSpec, ForeignModifyRelationContext,
};
use pg_lakebase_storage::StorageFile;

use crate::error::ConnectorError;
use crate::fdw::Lakebase;
use crate::storage::{ObjectFiles, ObjectOutput};

use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatScanPlanner,
    FormatScanState, FormatSchemaReader, FormatWritePrivate, FormatWriteState,
    FormatWriter, InferredSchema, ParquetWriteCompression,
};

pub(super) use copy::{ParquetCopyDestination, ParquetCopySource};
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

    fn begin(
        self: Box<Self>,
        context: BeginForeignScanContext<'_, Lakebase>,
        files: ObjectFiles,
    ) -> Result<Box<dyn FormatScanState>, ConnectorError> {
        Ok(Box::new(scan::ParquetScanState::begin(context, files)?))
    }
}

impl FormatWriter for ParquetFormat {
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
            return Err(ConnectorError::modify_not_implemented(FormatKind::Parquet));
        }
        Ok(ForeignModifyPlanSpec::new(FormatWritePrivate::new(
            FormatKind::Parquet,
        )))
    }

    fn begin_modify(
        self: Box<Self>,
        context: ForeignModifyBeginContext<'_, FormatWritePrivate>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        if context.operation() != ForeignModifyOperation::Insert {
            return Err(ConnectorError::modify_not_implemented(FormatKind::Parquet));
        }
        Ok(Box::new(write::ParquetWriteState::begin(
            context.relation(),
            output,
            self.write_compression,
        )?))
    }

    fn begin_insert(
        self: Box<Self>,
        context: &mut ForeignInsertBeginContext<'_>,
        output: ObjectOutput,
    ) -> Result<Box<dyn FormatWriteState>, ConnectorError> {
        Ok(Box::new(write::ParquetWriteState::begin(
            context.relation(),
            output,
            self.write_compression,
        )?))
    }
}

impl FormatSchemaReader for ParquetFormat {
    fn infer_schema(
        &self,
        file: &mut StorageFile,
    ) -> Result<InferredSchema, ConnectorError> {
        schema::infer(file)
    }
}
