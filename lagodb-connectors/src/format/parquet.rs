//! Parquet format configuration and capability composition.

mod copy;
mod reader;
mod scan;
mod schema;
mod write;
mod writer;

use pg_lakebase_storage::StorageFile;

use crate::error::ConnectorError;

use super::{
    FormatKind, FormatObject, FormatOption, FormatReader, FormatSchemaReader,
    FormatWriter, InferredSchema, ParquetWriteCompression,
};

pub(crate) use reader::ParquetObjectReader;
pub(crate) use schema::parquet_arrow_type;
pub(crate) use writer::ParquetObjectWriter;
pub(super) use copy::{ParquetCopyDestination, ParquetCopySource};

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
    fn planner(self: Box<Self>) -> Box<dyn super::FormatScanPlanner> {
        Box::new(scan::ParquetScanPlanner::new())
    }

    fn begin(
        self: Box<Self>,
        context: pg_lakebase_core::fdw::BeginForeignScanContext<
            '_,
            crate::fdw::Lakebase,
        >,
        files: crate::storage::ObjectFiles,
    ) -> Result<Box<dyn super::FormatScanState>, ConnectorError> {
        Ok(Box::new(scan::ParquetScanState::begin(context, files)?))
    }
}

impl FormatWriter for ParquetFormat {
    fn capabilities(
        &self,
        _context: &pg_lakebase_core::fdw::ForeignModifyRelationContext<'_>,
        output: crate::storage::ObjectLocationKind,
    ) -> Result<pg_lakebase_core::fdw::ForeignModifyCapabilities, ConnectorError>
    {
        Ok(pg_lakebase_core::fdw::ForeignModifyCapabilities::new(
            output == crate::storage::ObjectLocationKind::Prefix,
            false,
            false,
        ))
    }

    fn plan_modify(
        &self,
        context: &pg_lakebase_core::fdw::ForeignModifyPlanContext<'_>,
    ) -> Result<
        pg_lakebase_core::fdw::ForeignModifyPlanSpec<super::FormatWritePrivate>,
        ConnectorError,
    > {
        if context.operation()
            != pg_lakebase_core::fdw::ForeignModifyOperation::Insert
        {
            return Err(ConnectorError::modify_not_implemented(FormatKind::Parquet));
        }
        Ok(pg_lakebase_core::fdw::ForeignModifyPlanSpec::new(
            super::FormatWritePrivate::new(FormatKind::Parquet),
        ))
    }

    fn begin_modify(
        self: Box<Self>,
        context: pg_lakebase_core::fdw::ForeignModifyBeginContext<
            '_,
            super::FormatWritePrivate,
        >,
        output: crate::storage::ObjectOutput,
    ) -> Result<Box<dyn super::FormatWriteState>, ConnectorError> {
        if context.operation()
            != pg_lakebase_core::fdw::ForeignModifyOperation::Insert
        {
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
        context: &mut pg_lakebase_core::fdw::ForeignInsertBeginContext<'_>,
        output: crate::storage::ObjectOutput,
    ) -> Result<Box<dyn super::FormatWriteState>, ConnectorError> {
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
