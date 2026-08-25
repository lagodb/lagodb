//! PostgreSQL tuple-slot to rolling Parquet data-file pipeline.

use std::sync::Arc;

use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{
    DataFile, DataFileFormat, Schema as IcebergSchema, TableMetadata,
};
use iceberg_lite::writer::base_writer::data_file_writer::{
    DataFileWriter, DataFileWriterBuilder,
};
use iceberg_lite::writer::file_writer::ParquetWriterBuilder;
use iceberg_lite::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg_lite::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg_lite::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use pg_lakebase_core::prelude::TupleSlotRow;

use crate::engine::schema::column_mapping::WriteColumns;
use crate::engine::schema::relation::RelationShape;
use crate::error::{IcebergError, IcebergResult};

type ParquetDataFileWriter = DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// Buffers PostgreSQL tuple slots into Arrow columns and turns them into
/// Iceberg [`DataFile`]s through a rolling Parquet writer.
///
/// A Rust-heap session field (never in a PG memory context), so per-tuple
/// context resets cannot clobber it. Exits via [`Self::finish`] (success) or
/// [`Self::abort`] (failure).
pub(crate) struct DataFileSink {
    /// Relation-bound columnar write buffer: owns the per-column Arrow encoders
    /// and the name-resolved source-slot mapping, so each output column pulls
    /// from the correct slot index. See [`WriteColumns`].
    ///
    /// A Rust-heap session field (never in a PG memory context), so per-tuple
    /// context resets cannot clobber it.
    columns: WriteColumns,
    /// Row-buffer memory threshold for this modify state.
    flush_threshold_bytes: usize,
    /// Active rolling Parquet writer. `None` only after [`Self::close_writer`]
    /// consumes it (during `finish` / `abort`).
    writer: Option<ParquetDataFileWriter>,
}

impl DataFileSink {
    /// Resolve the write-side column plan / buffer and build the rolling Parquet
    /// writer. Fails fast on unsupported columns or a column/field desync before
    /// any row is accepted.
    pub(crate) fn new(
        file_io: &FileIO,
        iceberg_schema: &Arc<IcebergSchema>,
        relation_shape: &RelationShape,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
        flush_threshold_bytes: usize,
    ) -> IcebergResult<Self> {
        let columns = WriteColumns::resolve(iceberg_schema, relation_shape)?;
        let writer = Self::build_writer(
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
        // captured by `WriteColumns::resolve` during sink construction.
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
            Some(mut writer) => Ok(writer.close()?),
            None => Ok(Vec::new()),
        }
    }

    /// Build the rolling Parquet data file writer for this sink.
    fn build_writer(
        file_io: &FileIO,
        schema: &Arc<IcebergSchema>,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<ParquetDataFileWriter> {
        let location_generator = DefaultLocationGenerator::new(table_metadata)?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("insert-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );

        let parquet_writer_builder =
            ParquetWriterBuilder::new(writer_properties.clone(), schema.clone());

        let rolling_writer_builder =
            RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder,
                file_io.clone(),
                location_generator,
                file_name_generator,
            );

        let data_file_writer_builder =
            DataFileWriterBuilder::new(rolling_writer_builder);
        Ok(data_file_writer_builder.build(None)?)
    }
}
