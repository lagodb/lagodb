//! Partition-aware Arrow batch writer for one Iceberg table write session.

use std::{iter, sync::Arc};

use arrow_array::RecordBatch;
use iceberg_lite::arrow::RecordBatchPartitionSplitter;
use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{
    DataFile, DataFileFormat, PartitionKey, PartitionSpecRef, Schema, Struct,
    TableMetadata,
};
use iceberg_lite::writer::base_writer::data_file_writer::{
    DataFileWriter, DataFileWriterBuilder,
};
use iceberg_lite::writer::file_writer::ParquetWriterBuilder;
use iceberg_lite::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg_lite::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg_lite::writer::partitioning::PartitioningWriter;
use iceberg_lite::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg_lite::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use crate::error::IcebergResult;

type ParquetDataFileWriter = DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;
type ParquetDataFileWriterBuilder = DataFileWriterBuilder<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

/// Writes batches using the table metadata's default partition spec.
///
/// The partition projector and transform functions are bound once when the
/// modify state starts. Partition transforms therefore operate column-wise on
/// flushed Arrow batches instead of being parsed or resolved in the per-row
/// PostgreSQL callback. Unsorted partition batches are routed through
/// iceberg-lite's fanout writer. Its map retains one rolling Parquet writer per
/// distinct partition key and neither evicts nor closes an entry before this
/// write session closes, so open writers and their buffers grow with partition
/// cardinality. DataFileSink's flush threshold only bounds the input column
/// buffer; flushing feeds this map without releasing its writers or buffers.
/// Upstream iceberg-rust has the same behavior; issue #1744 tracks the resource
/// bound. Do not add a second eviction policy in
/// LagoDB; wait for the upstream fix, merge it into iceberg-lite, and then
/// remove this limitation.
/// https://github.com/apache/iceberg-rust/issues/1744
pub(super) struct TableDataWriter {
    inner: TableDataWriterKind,
}

enum TableDataWriterKind {
    Unpartitioned(Box<ParquetDataFileWriter>),
    Partitioned(Box<PartitionedDataWriter>),
}

struct PartitionedDataWriter {
    splitter: RecordBatchPartitionSplitter,
    writer: FanoutWriter<ParquetDataFileWriterBuilder>,
}

impl TableDataWriter {
    pub(super) fn new(
        file_io: &FileIO,
        schema: &Arc<Schema>,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<Self> {
        let spec = table_metadata.default_partition_spec();
        let builder =
            Self::writer_builder(file_io, schema, table_metadata, writer_properties)?;
        if spec.is_unpartitioned() {
            // Keep the actual spec, including a non-zero id after partition
            // evolution. Passing `None` would make the DataFile fall back to
            // spec id 0, which is not necessarily the current unpartitioned
            // spec.
            let partition_key = Self::unpartitioned_partition_key(spec, schema);
            return Ok(Self {
                inner: TableDataWriterKind::Unpartitioned(Box::new(
                    builder.build(Some(partition_key))?,
                )),
            });
        }

        // Upstream iceberg-rust currently projects every partition source id,
        // including a void field whose source column was removed. iceberg-lite
        // retains the same PartitionValueCalculator/RecordBatchProjector logic.
        // A mixed evolved spec therefore returns `Field not found` during writer
        // initialization, before the void transform can produce null. All-void
        // specs take the unpartitioned branch above. Do not add a LagoDB-only
        // projector: wait for the upstream fix and merge it into iceberg-lite.
        let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
            Arc::clone(schema),
            Arc::clone(spec),
        )?;
        Ok(Self {
            inner: TableDataWriterKind::Partitioned(Box::new(
                PartitionedDataWriter {
                    splitter,
                    writer: FanoutWriter::new(builder),
                },
            )),
        })
    }

    pub(super) fn write(&mut self, batch: RecordBatch) -> IcebergResult<()> {
        match &mut self.inner {
            TableDataWriterKind::Unpartitioned(writer) => writer.write(batch)?,
            TableDataWriterKind::Partitioned(writer) => writer.write(batch)?,
        }
        Ok(())
    }

    pub(super) fn close(self) -> IcebergResult<Vec<DataFile>> {
        match self.inner {
            TableDataWriterKind::Unpartitioned(mut writer) => Ok(writer.close()?),
            TableDataWriterKind::Partitioned(writer) => writer.close(),
        }
    }

    fn writer_builder(
        file_io: &FileIO,
        schema: &Arc<Schema>,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<ParquetDataFileWriterBuilder> {
        let location_generator = DefaultLocationGenerator::new(table_metadata)?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("insert-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );
        let parquet_writer_builder =
            ParquetWriterBuilder::new(writer_properties.clone(), Arc::clone(schema));
        let target_file_size = table_metadata
            .table_properties()
            .write_target_file_size_bytes()?;
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            target_file_size,
            file_io.clone(),
            location_generator,
            file_name_generator,
        );
        Ok(DataFileWriterBuilder::new(rolling_writer_builder))
    }

    /// Build the single static key for an effectively unpartitioned spec.
    ///
    /// An all-void evolved spec still has one partition tuple field per spec
    /// field; every value is null. A zero-field spec naturally produces an
    /// empty tuple through the same construction.
    fn unpartitioned_partition_key(
        spec: &PartitionSpecRef,
        schema: &Arc<Schema>,
    ) -> PartitionKey {
        let data = Struct::from_iter(iter::repeat_n(None, spec.fields().len()));
        PartitionKey::new(spec.as_ref().clone(), Arc::clone(schema), data)
    }
}

impl PartitionedDataWriter {
    fn write(&mut self, batch: RecordBatch) -> IcebergResult<()> {
        // TODO(partition-splitter-complexity): after computing partition values
        // and grouping N row indices, upstream creates an N-bit mask and calls
        // filter_record_batch once for each of P partitions. The mask build and
        // Arrow filter each rescan the batch, making this O(N * P), or O(N^2)
        // when every row has a distinct partition. It also allocates a fresh
        // N-bit mask and a filtered set of Arrow arrays for every partition:
        // aggregate temporary mask allocation is O(N * P), or O(N^2) in that
        // worst case, and P output array sets add allocator overhead. PR #3204
        // replaced a temporary Vec<bool> and packing pass with
        // BooleanBufferBuilder, reducing the constant cost but retaining this
        // per-partition full-batch algorithm.
        // The structural fix belongs in upstream RecordBatchPartitionSplitter:
        // group indices once and materialize groups without rescanning all N rows.
        // Upstream PR #3204 is already present in iceberg-lite, so this remains
        // an upstream O(N * P) limitation rather than a missing downstream merge.
        // Do not maintain a divergent LagoDB splitter; wait for the upstream
        // structural fix, merge it into iceberg-lite, and remove this limitation.
        // https://github.com/apache/iceberg-rust/pull/3204
        for (partition_key, partition_batch) in self.splitter.split(&batch)? {
            self.writer.write(partition_key, partition_batch)?;
        }
        Ok(())
    }

    fn close(self) -> IcebergResult<Vec<DataFile>> {
        Ok(self.writer.close()?)
    }
}
