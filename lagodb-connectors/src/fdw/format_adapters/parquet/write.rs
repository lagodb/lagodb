//! FDW write state for prefix-only immutable Parquet foreign tables.

use core::ffi::c_int;
use std::sync::Arc;

use lagodb_arrow::BoundWriteBuffer;
use lagodb_core::batch::BatchBuffer;
use lagodb_core::handles::RelationHandle;
use lagodb_core::tuple::{PgType, TupleSlotRow};

use crate::error::ConnectorError;
use crate::fdw::format_adapters::FormatWriteState;
use crate::format::ParquetWriteCompression;
use crate::format::parquet::{ParquetOutputWriter, ParquetWritePlan};
use crate::storage::StorageOutput;

const INSERT_BATCH_SIZE: i32 = 1_000;
const BUFFER_FLUSH_BYTES: usize = 8 * 1024 * 1024;

pub(super) struct ParquetFdwWriteState {
    buffer: BoundWriteBuffer,
    writer: ParquetOutputWriter,
}

impl ParquetFdwWriteState {
    pub(super) fn begin(
        relation: &RelationHandle<'_>,
        output: StorageOutput,
        compression: ParquetWriteCompression,
    ) -> Result<Self, ConnectorError> {
        let live = relation.live_columns();
        let plan = ParquetWritePlan::bind(live.iter().map(|column| {
            (
                column.name(),
                PgType::new(column.type_oid(), column.type_mod()),
                (column.attno() - 1) as usize,
            )
        }))?;
        let schema = Arc::clone(plan.schema());
        let buffer = plan.into_slot_buffer(relation.natts())?;
        let writer = ParquetOutputWriter::new(output, schema, compression)?;
        Ok(Self { buffer, writer })
    }

    fn flush_batch(&mut self) -> Result<(), ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let batch = self.buffer.finish_batch()?;
        self.writer.write_batch(&batch)
    }
}

impl FormatWriteState for ParquetFdwWriteState {
    fn batch_size(&self) -> c_int {
        INSERT_BATCH_SIZE
    }

    fn write_row(&mut self, row: TupleSlotRow<'_>) -> Result<(), ConnectorError> {
        unsafe { self.buffer.append_slot_row(row) }?;
        if self.buffer.should_flush(BUFFER_FLUSH_BYTES) {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), ConnectorError> {
        self.flush_batch()?;
        self.writer.finish(false)
    }
}
