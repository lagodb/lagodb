//! Iceberg v2 position-delete writer.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, UInt8Array, UInt8DictionaryArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema};
use iceberg_lite::arrow::schema_to_arrow_schema;
use iceberg_lite::io::FileIO;
use iceberg_lite::spec::{
    DataFileFormat, PartitionKey, Schema as IcebergSchema, TableMetadata,
};
use iceberg_lite::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriter, PositionDeleteFileWriterBuilder, position_delete_schema,
};
use iceberg_lite::writer::file_writer::ParquetWriterBuilder;
use iceberg_lite::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg_lite::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg_lite::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use super::{PositionDeleteAccumulator, RowDeleteOutput};
use crate::error::IcebergResult;
use crate::write::{PlannedMutationTasks, RelationRowRegistry};

type ParquetPositionDeleteFileWriter = PositionDeleteFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

const POSITION_DELETE_BATCH_ROWS: usize = 8192;

pub(super) struct PositionDeleteSink {
    file_io: FileIO,
    schema: Arc<IcebergSchema>,
    batch_schema: arrow_schema::SchemaRef,
    location_generator: DefaultLocationGenerator,
    writer_properties: WriterProperties,
}

impl PositionDeleteSink {
    pub(super) fn new(
        file_io: &FileIO,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<Self> {
        let schema = position_delete_schema();
        let arrow_schema = schema_to_arrow_schema(&schema)?;
        let mut fields = arrow_schema.fields().to_vec();
        fields[0] = Arc::new(fields[0].as_ref().clone().with_data_type(
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        ));
        let batch_schema = Arc::new(ArrowSchema::new_with_metadata(
            fields,
            arrow_schema.metadata().clone(),
        ));
        Ok(Self {
            file_io: file_io.clone(),
            schema,
            batch_schema,
            location_generator: DefaultLocationGenerator::new(table_metadata)?,
            writer_properties: writer_properties.clone(),
        })
    }

    pub(super) fn write_files(
        &self,
        deletes: &PositionDeleteAccumulator,
        row_registry: &RelationRowRegistry,
        scan_tasks: &PlannedMutationTasks,
    ) -> IcebergResult<Vec<RowDeleteOutput>> {
        let mut outputs = Vec::new();
        for (file_id, positions) in deletes.files() {
            let referenced_data_file = row_registry.file_path(file_id)?;
            let partition_key =
                scan_tasks.partition_key_for_path(&referenced_data_file)?;
            let mut writer = self.build_writer(partition_key)?;
            let mut chunk = Vec::with_capacity(POSITION_DELETE_BATCH_ROWS);
            let positions = positions.borrow()?;
            for position in positions.iter() {
                chunk.push(i64::from(position));
                if chunk.len() == POSITION_DELETE_BATCH_ROWS {
                    writer.write(self.record_batch(
                        &referenced_data_file,
                        std::mem::take(&mut chunk),
                    )?)?;
                    chunk = Vec::with_capacity(POSITION_DELETE_BATCH_ROWS);
                }
            }
            if !chunk.is_empty() {
                writer.write(self.record_batch(&referenced_data_file, chunk)?)?;
            }
            for delete_file in writer.close()? {
                outputs.push(RowDeleteOutput {
                    delete_file,
                    referenced_data_files: vec![referenced_data_file.to_string()],
                    removed_delete_files: Vec::new(),
                });
            }
        }
        Ok(outputs)
    }

    fn build_writer(
        &self,
        partition_key: Option<PartitionKey>,
    ) -> IcebergResult<ParquetPositionDeleteFileWriter> {
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("delete-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );
        let parquet_writer_builder = ParquetWriterBuilder::new(
            self.writer_properties.clone(),
            Arc::clone(&self.schema),
        );
        let rolling_writer_builder =
            RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder,
                self.file_io.clone(),
                self.location_generator.clone(),
                file_name_generator,
            );
        let builder = PositionDeleteFileWriterBuilder::new(rolling_writer_builder);
        Ok(builder.build(partition_key)?)
    }

    fn record_batch(
        &self,
        referenced_data_file: &str,
        positions: Vec<i64>,
    ) -> IcebergResult<RecordBatch> {
        let file_keys = UInt8Array::from_value(0, positions.len());
        let file_values: ArrayRef = Arc::new(StringArray::from_iter_values(
            std::iter::once(referenced_data_file),
        ));
        let file_array: ArrayRef =
            Arc::new(UInt8DictionaryArray::try_new(file_keys, file_values)?);
        let pos_array: ArrayRef = Arc::new(Int64Array::from(positions));
        Ok(RecordBatch::try_new(
            Arc::clone(&self.batch_schema),
            vec![file_array, pos_array],
        )?)
    }
}
