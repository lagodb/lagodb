//! Parquet configuration, shared file codecs, and predicates.

mod copy;
mod filter;
mod output_writer;
mod read_plan;
mod reader;
mod schema;
mod source;
mod type_mapping;
mod write_plan;

use crate::error::ConnectorError;
use crate::storage::InputFile;

use super::{
    FormatKind, FormatObject, FormatOption, FormatSchemaReader, InferredSchema,
    ParquetWriteCompression,
};

pub(super) use copy::{ParquetCopyDestination, ParquetCopySource};
pub(crate) use filter::{
    ParquetBoundPredicate, ParquetFilePredicate, ParquetFilterPlanner,
    ParquetPlannedPredicate,
};
pub(crate) use output_writer::ParquetOutputWriter;
pub(crate) use read_plan::ParquetReadPlan;
pub(crate) use source::ParquetBatchSource;
pub(crate) use type_mapping::ParquetReadMode;
pub(crate) use write_plan::ParquetWritePlan;

/// Parquet-format processor.
pub(crate) struct ParquetFormat {
    pub(super) write_compression: ParquetWriteCompression,
}

impl ParquetFormat {
    pub(crate) const fn write_compression(&self) -> ParquetWriteCompression {
        self.write_compression
    }

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

impl FormatSchemaReader for ParquetFormat {
    fn infer_schema(
        &self,
        file: &mut InputFile,
    ) -> Result<InferredSchema, ConnectorError> {
        schema::ParquetSchema::infer(file)
    }
}
