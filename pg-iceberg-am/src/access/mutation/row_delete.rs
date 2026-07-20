//! Row-delete accumulation and Iceberg delete-file backends.
//!
//! The mutation session owns only this module's facade types. Position-delete
//! Parquet files and v3 deletion vectors remain implementation details here.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, UInt8Array, UInt8DictionaryArray,
};
use arrow_schema::{DataType, Schema as ArrowSchema};
use iceberg_lite::arrow::schema_to_arrow_schema;
use iceberg_lite::io::FileIO;
use iceberg_lite::metadata_columns::{delete_file_path_field, delete_file_pos_field};
use iceberg_lite::overlay::{DeleteFileIdentity, SnapshotDelta};
use iceberg_lite::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg_lite::spec::{
    DataFile, DataFileFormat, FormatVersion, Schema as IcebergSchema, Struct,
    TableMetadata,
};
use iceberg_lite::writer::base_writer::deletion_vector_writer::{
    DeletionVectorFileWriter, ExistingPositionDeleteFile, ReferencedDataFile,
};
use iceberg_lite::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriter, PositionDeleteFileWriterBuilder,
    PositionDeleteWriterConfig,
};
use iceberg_lite::writer::file_writer::ParquetWriterBuilder;
use iceberg_lite::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator,
    LocationGenerator,
};
use iceberg_lite::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg_lite::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use crate::access::scan::PlannedScanTasks;
use crate::catalog::row_mutations::{
    IcebergFileId, OwnedRowPositions, RelationRowRegistry,
};
use crate::error::{IcebergError, IcebergResult};

type ParquetPositionDeleteFileWriter = PositionDeleteFileWriter<
    ParquetWriterBuilder,
    DefaultLocationGenerator,
    DefaultFileNameGenerator,
>;

const POSITION_DELETE_BATCH_ROWS: usize = 8192;

pub(super) struct RowDeleteOutput {
    pub(super) delete_file: DataFile,
    pub(super) referenced_data_files: Vec<String>,
    pub(super) removed_delete_files: Vec<DeleteFileIdentity>,
}

struct PlannedDataFile {
    target: ReferencedDataFile,
    position_delete_files: Vec<FileScanTaskDeleteFile>,
}

impl PlannedDataFile {
    fn from_scan_tasks(path: &str, tasks: Vec<FileScanTask>) -> IcebergResult<Self> {
        let mut tasks = tasks.into_iter();
        let first = tasks.next().ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "cannot find Iceberg scan task metadata for deletion target {path}"
            ))
        })?;
        let target = Self::target_from_task(&first)?;
        if target.file_path() != path {
            return Err(IcebergError::InvariantViolated(
                "Iceberg scan task path does not match requested deletion target",
            ));
        }
        let mut data_file = Self {
            target,
            position_delete_files: Vec::new(),
        };
        data_file.merge_delete_files(first)?;
        for task in tasks {
            data_file.merge_task(path, task)?;
        }
        Ok(data_file)
    }

    fn merge_task(&mut self, path: &str, task: FileScanTask) -> IcebergResult<()> {
        let target = Self::target_from_task(&task)?;
        if target.file_path() != path {
            return Err(IcebergError::InvariantViolated(
                "Iceberg scan task path does not match requested deletion target",
            ));
        }
        if self.target != target {
            return Err(IcebergError::InvariantViolated(
                "Iceberg scan planned conflicting metadata for one data file",
            ));
        }
        self.merge_delete_files(task)
    }

    fn merge_delete_files(&mut self, task: FileScanTask) -> IcebergResult<()> {
        for delete_file in task.deletes {
            if !delete_file.is_position_delete() {
                continue;
            }

            match delete_file.referenced_data_file_path() {
                Some(target) if target == self.target.file_path() => {
                    self.push_position_delete_file(delete_file);
                }
                Some(_) => {}
                None if delete_file.is_deletion_vector() => {
                    return Err(IcebergError::MetadataTracker(format!(
                        "deletion vector delete file {} is missing a referenced data file",
                        delete_file.file_path
                    )));
                }
                None => {
                    self.push_position_delete_file(delete_file);
                }
            }
        }
        Ok(())
    }

    fn push_position_delete_file(&mut self, delete_file: FileScanTaskDeleteFile) {
        if !self.position_delete_files.contains(&delete_file) {
            self.position_delete_files.push(delete_file);
        }
    }

    fn target_from_task(task: &FileScanTask) -> IcebergResult<ReferencedDataFile> {
        if let Some(partition_spec) = task.partition_spec.as_ref()
            && partition_spec.spec_id() != task.partition_spec_id
        {
            return Err(IcebergError::InvariantViolated(
                "Iceberg scan task partition spec id does not match its partition spec",
            ));
        }
        Ok(ReferencedDataFile::new(
            task.data_file_path.clone(),
            task.partition.clone().unwrap_or_else(Struct::empty),
            task.partition_spec_id,
        ))
    }
}

pub(super) enum RowDeleteSink {
    Position(Box<PositionDeleteSink>),
    DeletionVector(Box<DeletionVectorSink>),
}

impl RowDeleteSink {
    pub(super) fn for_table(
        format_version: FormatVersion,
        file_io: &FileIO,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
        delta: Option<&Arc<SnapshotDelta>>,
    ) -> IcebergResult<Self> {
        match format_version {
            FormatVersion::V1 => {
                unreachable!("v1 row deletes are rejected before sink construction")
            }
            FormatVersion::V2 => Ok(Self::Position(Box::new(
                PositionDeleteSink::new(file_io, table_metadata, writer_properties)?,
            ))),
            FormatVersion::V3 => Ok(Self::DeletionVector(Box::new(
                DeletionVectorSink::new(file_io, table_metadata, delta)?,
            ))),
        }
    }

    pub(super) fn write_files(
        &self,
        deletes: &PositionDeleteAccumulator,
        row_registry: &RelationRowRegistry,
        scan_tasks: Option<&Arc<PlannedScanTasks>>,
    ) -> IcebergResult<Vec<RowDeleteOutput>> {
        match self {
            Self::Position(sink) => sink.write_files(deletes, row_registry),
            Self::DeletionVector(sink) => {
                let scan_tasks =
                    scan_tasks.ok_or(IcebergError::InvariantViolated(
                        "deletion-vector write has no target scan task cache",
                    ))?;
                sink.write_files(deletes, row_registry, scan_tasks)
            }
        }
    }
}

/// Writes Iceberg position delete files for rows accumulated by
/// [`PositionDeleteAccumulator`].
pub(super) struct PositionDeleteSink {
    file_io: FileIO,
    schema: Arc<IcebergSchema>,
    batch_schema: arrow_schema::SchemaRef,
    location_generator: DefaultLocationGenerator,
    writer_properties: WriterProperties,
}

impl PositionDeleteSink {
    fn new(
        file_io: &FileIO,
        table_metadata: &TableMetadata,
        writer_properties: &WriterProperties,
    ) -> IcebergResult<Self> {
        let schema = Arc::new(
            IcebergSchema::builder()
                .with_fields([
                    Arc::clone(delete_file_path_field()),
                    Arc::clone(delete_file_pos_field()),
                ])
                .build()?,
        );
        let arrow_schema = schema_to_arrow_schema(&schema)?;
        let mut fields = arrow_schema.fields().to_vec();
        let file_path_field =
            fields.first_mut().ok_or(IcebergError::InvariantViolated(
                "position-delete schema is missing the file path field",
            ))?;
        if file_path_field.data_type() != &DataType::Utf8 {
            return Err(IcebergError::InvariantViolated(
                "position-delete file path field has an unexpected Arrow type",
            ));
        }
        *file_path_field = Arc::new(file_path_field.as_ref().clone().with_data_type(
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

    fn write_files(
        &self,
        deletes: &PositionDeleteAccumulator,
        row_registry: &RelationRowRegistry,
    ) -> IcebergResult<Vec<RowDeleteOutput>> {
        let mut outputs = Vec::new();
        for (file_id, positions) in deletes.files() {
            let referenced_data_file = row_registry.file_path(file_id)?;
            let mut writer = self.build_writer(&referenced_data_file)?;
            let mut chunk = Vec::with_capacity(POSITION_DELETE_BATCH_ROWS);
            let positions = positions.borrow()?;
            for position in positions.iter() {
                chunk.push(u64::from(position));
                if chunk.len() == POSITION_DELETE_BATCH_ROWS {
                    writer
                        .write(self.record_batch(&referenced_data_file, &chunk)?)?;
                    chunk.clear();
                }
            }
            if !chunk.is_empty() {
                writer.write(self.record_batch(&referenced_data_file, &chunk)?)?;
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
        referenced_data_file: &str,
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
        let builder = PositionDeleteFileWriterBuilder::new(
            rolling_writer_builder,
            PositionDeleteWriterConfig::new(referenced_data_file),
        );
        Ok(builder.build(None)?)
    }

    fn record_batch(
        &self,
        referenced_data_file: &str,
        positions: &[u64],
    ) -> IcebergResult<RecordBatch> {
        let mut pos_values = Vec::with_capacity(positions.len());
        for position in positions {
            pos_values.push(i64::try_from(*position).map_err(|_| {
                IcebergError::MetadataTracker(format!(
                    "Iceberg row position {position} is too large for position delete file"
                ))
            })?);
        }
        let file_keys = UInt8Array::from_value(0, positions.len());
        let file_values: ArrayRef = Arc::new(StringArray::from_iter_values(
            std::iter::once(referenced_data_file),
        ));
        let file_array: ArrayRef =
            Arc::new(UInt8DictionaryArray::try_new(file_keys, file_values)?);
        let pos_array: ArrayRef = Arc::new(Int64Array::from(pos_values));
        Ok(RecordBatch::try_new(
            Arc::clone(&self.batch_schema),
            vec![file_array, pos_array],
        )?)
    }
}

pub(super) struct DeletionVectorSink {
    file_io: FileIO,
    location_generator: DefaultLocationGenerator,
}

impl DeletionVectorSink {
    fn new(
        file_io: &FileIO,
        table_metadata: &TableMetadata,
        _delta: Option<&Arc<SnapshotDelta>>,
    ) -> IcebergResult<Self> {
        Ok(Self {
            file_io: file_io.clone(),
            location_generator: DefaultLocationGenerator::new(table_metadata)?,
        })
    }

    fn write_files(
        &self,
        deletes: &PositionDeleteAccumulator,
        row_registry: &RelationRowRegistry,
        scan_tasks: &PlannedScanTasks,
    ) -> IcebergResult<Vec<RowDeleteOutput>> {
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("delete-{}", uuid::Uuid::now_v7()),
            None,
            DataFileFormat::Puffin,
        );
        let output_path = self
            .location_generator
            .generate_location(None, &file_name_generator.generate_file_name());
        let mut writer =
            DeletionVectorFileWriter::new(self.file_io.clone(), output_path);
        let mut removed_delete_files_by_target: HashMap<
            String,
            BTreeSet<DeleteFileIdentity>,
        > = HashMap::new();

        for (file_id, positions) in deletes.files() {
            let referenced_data_file = row_registry.file_path(file_id)?;
            let planned_data_file = PlannedDataFile::from_scan_tasks(
                referenced_data_file.as_ref(),
                scan_tasks.mutation_tasks_for_path(referenced_data_file.as_ref())?,
            )?;
            let target = planned_data_file.target.clone();

            let removed_delete_files = removed_delete_files_by_target
                .entry(target.file_path().to_owned())
                .or_default();
            for existing_delete_file in &planned_data_file.position_delete_files {
                let existing = ExistingPositionDeleteFile::new(
                    &existing_delete_file.file_path,
                    existing_delete_file.file_size_in_bytes,
                    existing_delete_file.file_format,
                    existing_delete_file.referenced_data_file_path(),
                    existing_delete_file.content_offset,
                    existing_delete_file.content_size_in_bytes,
                    existing_delete_file.record_count,
                );
                let merge = writer.merge_existing_position_delete_file(
                    target.clone(),
                    &existing,
                    &self.file_io,
                )?;
                if merge.can_remove()
                    && existing_delete_file.can_remove_after_dv_rewrite()
                {
                    removed_delete_files.insert(DeleteFileIdentity::new(
                        existing_delete_file.file_path.clone(),
                        existing_delete_file.content_offset,
                        existing_delete_file.content_size_in_bytes,
                    ));
                }
            }

            let positions = positions.borrow()?;
            writer.delete_all(target, positions.iter().map(u64::from))?;
        }

        let (delete_files, referenced_data_files) = writer.close()?.into_parts();
        Ok(delete_files
            .into_iter()
            .zip(referenced_data_files)
            .map(|(delete_file, referenced_data_file)| RowDeleteOutput {
                delete_file,
                removed_delete_files: removed_delete_files_by_target
                    .remove(&referenced_data_file)
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
                referenced_data_files: vec![referenced_data_file],
            })
            .collect())
    }
}

#[derive(Debug, Default)]
pub(super) struct PositionDeleteAccumulator {
    /// One shared owner bitmap per file touched by this ModifyState. The
    /// registry performs the only per-row insertion; this list is updated only
    /// when the state first touches a file.
    files: Vec<(IcebergFileId, OwnedRowPositions)>,
}

impl PositionDeleteAccumulator {
    pub(super) fn add_file_positions(
        &mut self,
        file_id: IcebergFileId,
        positions: OwnedRowPositions,
    ) {
        debug_assert!(
            self.files.iter().all(|(existing, _)| *existing != file_id),
            "one ModifyState must own exactly one bitmap per file"
        );
        self.files.push((file_id, positions));
    }

    pub(super) fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    fn files(&self) -> impl Iterator<Item = (IcebergFileId, &OwnedRowPositions)> {
        self.files
            .iter()
            .map(|(file_id, positions)| (*file_id, positions))
    }

    pub(super) fn referenced_data_files(
        &self,
        row_registry: &RelationRowRegistry,
    ) -> IcebergResult<BTreeSet<String>> {
        self.files()
            .map(|(file_id, _)| {
                row_registry.file_path(file_id).map(|path| path.to_string())
            })
            .collect()
    }

    pub(super) fn clear(&mut self) {
        self.files.clear();
    }
}
