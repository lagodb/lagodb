//! Iceberg v3 deletion-vector writer and planned target metadata.

use std::collections::{BTreeSet, HashMap};

use iceberg_lite::io::FileIO;
use iceberg_lite::overlay::DeleteFileIdentity;
use iceberg_lite::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg_lite::spec::{DataFileFormat, Struct, TableMetadata};
use iceberg_lite::writer::base_writer::deletion_vector_writer::{
    DeletionVectorFileWriter, ExistingPositionDeleteFile, ReferencedDataFile,
};
use iceberg_lite::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator,
    LocationGenerator,
};

use super::{PositionDeleteAccumulator, RowDeleteOutput};
use crate::access::scan::PlannedMutationTasks;
use crate::catalog::row_mutations::RelationRowRegistry;
use crate::error::{IcebergError, IcebergResult};

struct PlannedDataFile {
    target: ReferencedDataFile,
    position_delete_files: Vec<FileScanTaskDeleteFile>,
}

impl PlannedDataFile {
    fn from_scan_tasks(path: &str, tasks: &[&FileScanTask]) -> IcebergResult<Self> {
        let mut tasks = tasks.iter().copied();
        let first = tasks.next().ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "cannot find Iceberg scan task metadata for deletion target {path}"
            ))
        })?;
        let target = Self::target_from_task(first)?;
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

    fn merge_task(&mut self, path: &str, task: &FileScanTask) -> IcebergResult<()> {
        let target = Self::target_from_task(task)?;
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

    fn merge_delete_files(&mut self, task: &FileScanTask) -> IcebergResult<()> {
        for delete_file in &task.deletes {
            if !delete_file.is_position_delete() {
                continue;
            }

            match delete_file.referenced_data_file_path() {
                Some(target) if target == self.target.file_path() => {
                    self.push_position_delete_file(delete_file.clone());
                }
                Some(_) => {}
                None if delete_file.is_deletion_vector() => {
                    return Err(IcebergError::MetadataTracker(format!(
                        "deletion vector delete file {} is missing a referenced data file",
                        delete_file.file_path
                    )));
                }
                None => {
                    self.push_position_delete_file(delete_file.clone());
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

pub(super) struct DeletionVectorSink {
    file_io: FileIO,
    location_generator: DefaultLocationGenerator,
}

impl DeletionVectorSink {
    pub(super) fn new(
        file_io: &FileIO,
        table_metadata: &TableMetadata,
    ) -> IcebergResult<Self> {
        Ok(Self {
            file_io: file_io.clone(),
            location_generator: DefaultLocationGenerator::new(table_metadata)?,
        })
    }

    pub(super) fn write_files(
        &self,
        deletes: &PositionDeleteAccumulator,
        row_registry: &RelationRowRegistry,
        scan_tasks: &PlannedMutationTasks,
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
            let tasks = scan_tasks.tasks_for_path(referenced_data_file.as_ref())?;
            let planned_data_file = PlannedDataFile::from_scan_tasks(
                referenced_data_file.as_ref(),
                &tasks,
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
