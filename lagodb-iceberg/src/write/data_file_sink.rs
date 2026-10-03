//! PostgreSQL tuple-slot to rolling Parquet data-file pipeline.

use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{DataFile, Schema as IcebergSchema, TableMetadata};
use lagodb_arrow::BoundWriteBuffer;
use lagodb_core::batch::BatchBuffer;
use lagodb_core::prelude::TupleSlotRow;
use parquet::file::properties::WriterProperties;

use crate::error::{IcebergError, IcebergResult};
use crate::schema::column_plan::WriteColumnPlan;
use crate::schema::relation::RelationLayout;
use crate::write::table_data_writer::TableDataWriter;

/// Buffers PostgreSQL tuple slots into Arrow columns and turns them into
/// Iceberg [`DataFile`]s through a rolling Parquet writer.
///
/// A Rust-heap session field (never in a PG memory context), so per-tuple
/// context resets cannot clobber it. Exits via [`Self::finish`] (success) or
/// [`Self::abort`] (failure).
pub(crate) struct DataFileSink {
    /// Runtime encoders and source positions consumed from the schema binding.
    columns: BoundWriteBuffer,
    /// Row-buffer memory threshold for this modify state.
    flush_threshold_bytes: usize,
    /// Active table writer. Partitioned tables fan out to one rolling Parquet
    /// writer per touched partition; unpartitioned tables retain one writer.
    /// `None` only after [`Self::close_writer`] consumes it during `finish` or
    /// `abort`.
    writer: Option<TableDataWriter>,
}

impl DataFileSink {
    /// Resolve the write-side column plan / buffer and build the table writer.
    /// Fails fast on unsupported columns, partition transforms, table write
    /// properties, or a column/field desync before any row is accepted.
    pub(crate) fn new(
        file_io: &FileIO,
        iceberg_schema: &Arc<IcebergSchema>,
        relation_layout: &RelationLayout,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
        flush_threshold_bytes: usize,
    ) -> IcebergResult<Self> {
        let (arrow_schema, column_plans) =
            WriteColumnPlan::bind(iceberg_schema, relation_layout)?.into_parts();
        let columns = BoundWriteBuffer::new(arrow_schema, column_plans)?;
        let writer = TableDataWriter::new(
            file_io,
            iceberg_schema,
            table_metadata,
            writer_properties,
        )?;
        Ok(Self {
            columns,
            flush_threshold_bytes,
            writer: Some(writer),
        })
    }

    /// Append one tuple-slot row into the buffer, then flush if the memory
    /// threshold is reached. The borrowed slot view is consumed within this call.
    ///
    /// # Safety
    ///
    /// `row` must be a tuple slot from the same relation layout used to
    /// construct this sink. The mutation framework supplies that relation-local
    /// invariant at its callback boundary.
    pub(crate) unsafe fn append(
        &mut self,
        row: TupleSlotRow<'_>,
    ) -> IcebergResult<()> {
        // SAFETY: the caller's relation-local callback supplies the layout
        // captured by `WriteColumnPlan::bind` during sink construction.
        unsafe { self.columns.append_slot_row(row)? };
        self.flush_if_needed()
    }

    /// Flush remaining rows and close the writer, returning every produced
    /// data file. The writer is always closed even if the flush fails, so a
    /// failing flush cannot leak a file descriptor.
    pub(crate) fn finish(&mut self) -> IcebergResult<Vec<DataFile>> {
        let flush_res = self.flush_buffer();
        let close_res = self.close_writer();
        flush_res?;
        close_res
    }

    /// Best-effort cleanup of in-memory state for the failure path. Persistent
    /// artifacts are unwound by the adapter's ResourceOwner cleanup.
    pub(crate) fn abort(&mut self) {
        self.columns.clear();
        self.writer.take();
    }

    fn flush_if_needed(&mut self) -> IcebergResult<()> {
        if self.columns.should_flush(self.flush_threshold_bytes) {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Finish the buffered columns into a RecordBatch and write it to the writer.
    fn flush_buffer(&mut self) -> IcebergResult<()> {
        if self.columns.is_empty() {
            return Ok(());
        }

        // `finish_batch` resets the buffer, so it is cleared even if the write fails.
        let record_batch = self.columns.finish_batch()?;

        // `None` here means a tuple callback fired after finalization — a
        // framework bug worth surfacing.
        match self.writer.as_mut() {
            Some(writer) => writer.write(record_batch)?,
            None => {
                return Err(IcebergError::InvariantViolated(
                    "tuple callback after writer close",
                ));
            }
        }

        Ok(())
    }

    fn close_writer(&mut self) -> IcebergResult<Vec<DataFile>> {
        match self.writer.take() {
            Some(writer) => Ok(writer.close()?),
            None => Ok(Vec::new()),
        }
    }
}
