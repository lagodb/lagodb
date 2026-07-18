use std::sync::Arc;

use iceberg_lite::metadata_columns::{
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_ROW_ID,
    last_updated_sequence_number_field, row_id_field,
};
use iceberg_lite::spec::{
    DataFile, DataFileFormat, FormatVersion, PartitionKey, Schema, SchemaRef,
    TableMetadata,
};
use iceberg_lite::table::Table;
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

use crate::error::{IcebergError, IcebergResult, IcebergVacuumError};

use super::types::RewriteGroup;

type VacuumDataWriter = DataFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

pub(crate) struct RewriteGroupWriter;

impl RewriteGroupWriter {
    pub(crate) fn rewrite(
        table: &Table,
        group: &RewriteGroup,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<Vec<DataFile>> {
        let (scan_schema, projection) = Self::rewrite_projection(table.metadata())?;
        let scan = table
            .scan()
            .select_field_ids(projection.clone())
            .with_concurrency_limit(1)
            .build()?;
        let tasks = group
            .inputs
            .iter()
            .map(|input| {
                let mut task = input.task.clone();
                task.schema = scan_schema.clone();
                task.project_field_ids = projection.clone();
                task
            })
            .collect::<Vec<_>>();

        let mut writer = Self::writer(table, group, scan_schema, writer_properties)?;
        for batch in scan.to_arrow_with_tasks(tasks)? {
            pgrx::pg_sys::check_for_interrupts!();
            writer.write(batch?)?;
        }
        Ok(writer.close()?)
    }

    fn rewrite_projection(
        metadata: &TableMetadata,
    ) -> IcebergResult<(SchemaRef, Vec<i32>)> {
        let current = metadata.current_schema();
        let mut fields = current.as_struct().fields().to_vec();
        let mut projection: Vec<i32> = fields.iter().map(|field| field.id).collect();
        if metadata.format_version() == FormatVersion::V3 {
            fields.push(row_id_field().clone());
            fields.push(last_updated_sequence_number_field().clone());
            projection.push(RESERVED_FIELD_ID_ROW_ID);
            projection.push(RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER);
        }
        let schema = Schema::builder()
            .with_schema_id(current.schema_id())
            .with_identifier_field_ids(current.identifier_field_ids())
            .with_fields(fields)
            .build()?;
        Ok((Arc::new(schema), projection))
    }

    fn writer(
        table: &Table,
        group: &RewriteGroup,
        schema: SchemaRef,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<VacuumDataWriter> {
        let first = group.inputs.first().ok_or_else(|| {
            IcebergError::InvariantViolated("rewrite group has no input files")
        })?;
        let spec = table
            .metadata()
            .partition_spec_by_id(first.file.partition_spec_id())
            .cloned()
            .ok_or_else(|| IcebergError::Vacuum {
                source: IcebergVacuumError::ResourceLimit(format!(
                    "partition spec {} no longer exists",
                    first.file.partition_spec_id()
                )),
            })?;
        let partition_key = (!spec.is_unpartitioned()).then(|| {
            PartitionKey::new(
                spec.as_ref().clone(),
                schema.clone(),
                first.file.partition().clone(),
            )
        });
        let target_size = table
            .metadata()
            .table_properties()?
            .write_target_file_size_bytes;
        let location_generator = DefaultLocationGenerator::new(table.metadata())?;
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("vacuum-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Parquet,
        );
        let parquet_writer =
            ParquetWriterBuilder::new(writer_properties.clone(), schema);
        let rolling_writer = RollingFileWriterBuilder::new(
            parquet_writer,
            target_size,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        Ok(DataFileWriterBuilder::new(rolling_writer).build(partition_key)?)
    }
}
