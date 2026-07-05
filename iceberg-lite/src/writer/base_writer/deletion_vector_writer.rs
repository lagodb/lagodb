// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Writer for Iceberg v3 deletion vectors.

use std::collections::{BTreeMap, HashMap};

use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::delete_file_loader::BasicDeleteFileLoader;
use crate::compression::CompressionCodec;
use crate::delete_vector::DeleteVector;
use crate::io::FileIO;
use crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_POS;
use crate::puffin::{Blob, DELETION_VECTOR_V1, PuffinWriter};
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct,
};
use crate::{Error, ErrorKind, Result};

const REFERENCED_DATA_FILE_PROPERTY: &str = "referenced-data-file";
const CARDINALITY_PROPERTY: &str = "cardinality";

/// Data-file metadata required to write a deletion-vector delete file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferencedDataFile {
    file_path: String,
    partition: Struct,
    partition_spec_id: i32,
}

impl ReferencedDataFile {
    /// Creates a referenced data-file descriptor.
    pub fn new(
        file_path: impl Into<String>,
        partition: Struct,
        partition_spec_id: i32,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            partition,
            partition_spec_id,
        }
    }

    /// Creates a descriptor from an Iceberg data-file manifest entry.
    pub fn from_data_file(data_file: &DataFile) -> Result<Self> {
        if data_file.content_type() != DataContentType::Data {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "deletion vector target must be a data file",
            ));
        }
        Ok(Self::new(
            data_file.file_path().to_owned(),
            data_file.partition().clone(),
            data_file.partition_spec_id,
        ))
    }

    /// Returns the target data-file path.
    pub fn file_path(&self) -> &str {
        &self.file_path
    }
}

/// Metadata required to read one existing file-scoped position delete file.
#[derive(Debug, Clone, Copy)]
pub struct ExistingPositionDeleteFile<'a> {
    file_path: &'a str,
    file_size_in_bytes: u64,
    file_format: DataFileFormat,
    referenced_data_file: Option<&'a str>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    record_count: u64,
}

impl<'a> ExistingPositionDeleteFile<'a> {
    /// Creates an existing position-delete descriptor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_path: &'a str,
        file_size_in_bytes: u64,
        file_format: DataFileFormat,
        referenced_data_file: Option<&'a str>,
        content_offset: Option<i64>,
        content_size_in_bytes: Option<i64>,
        record_count: u64,
    ) -> Self {
        Self {
            file_path,
            file_size_in_bytes,
            file_format,
            referenced_data_file,
            content_offset,
            content_size_in_bytes,
            record_count,
        }
    }

    /// Creates a descriptor from an Iceberg delete manifest entry.
    pub fn from_data_file(data_file: &'a DataFile) -> Result<Self> {
        if data_file.content_type() != DataContentType::PositionDeletes {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "deletion vector writer can only merge position delete files",
            ));
        }
        let referenced_data_file = if data_file.is_deletion_vector() {
            data_file.referenced_data_file_path()
        } else {
            data_file.position_delete_target_data_file_path()
        };
        Ok(Self::new(
            data_file.file_path(),
            data_file.file_size_in_bytes(),
            data_file.file_format(),
            referenced_data_file,
            data_file.content_offset(),
            data_file.content_size_in_bytes(),
            data_file.record_count(),
        ))
    }
}

/// Result of merging one existing position delete file into a new deletion
/// vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExistingPositionDeleteMerge {
    can_remove: bool,
}

impl ExistingPositionDeleteMerge {
    fn retained() -> Self {
        Self { can_remove: false }
    }

    fn removable() -> Self {
        Self { can_remove: true }
    }

    /// Returns true when the existing delete file was fully superseded by the
    /// new deletion vector and can be removed from table metadata.
    pub fn can_remove(self) -> bool {
        self.can_remove
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExistingPositionDeleteCacheKey {
    file_path: String,
    file_size_in_bytes: u64,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl ExistingPositionDeleteCacheKey {
    fn from_existing(delete_file: &ExistingPositionDeleteFile<'_>) -> Self {
        Self {
            file_path: delete_file.file_path.to_owned(),
            file_size_in_bytes: delete_file.file_size_in_bytes,
            content_offset: delete_file.content_offset,
            content_size_in_bytes: delete_file.content_size_in_bytes,
        }
    }
}

#[derive(Debug)]
struct PendingDeletionVector {
    target: ReferencedDataFile,
    positions: DeleteVector,
}

/// Result of writing one Puffin file that contains one or more deletion vectors.
#[derive(Debug, Default)]
pub struct DeletionVectorWriteResult {
    delete_files: Vec<DataFile>,
    referenced_data_files: Vec<String>,
}

impl DeletionVectorWriteResult {
    /// Returns true when no deletion-vector files were produced.
    pub fn is_empty(&self) -> bool {
        self.delete_files.is_empty()
    }

    /// Returns the produced delete files.
    pub fn delete_files(&self) -> &[DataFile] {
        &self.delete_files
    }

    /// Consumes this result and returns the produced delete files.
    pub fn into_delete_files(self) -> Vec<DataFile> {
        self.delete_files
    }

    /// Consumes this result and returns delete files with their referenced data files.
    pub fn into_parts(self) -> (Vec<DataFile>, Vec<String>) {
        (self.delete_files, self.referenced_data_files)
    }

    /// Returns the data files referenced by the produced deletion vectors.
    pub fn referenced_data_files(&self) -> &[String] {
        &self.referenced_data_files
    }
}

/// Writes one Puffin file containing Iceberg v3 deletion-vector blobs.
#[derive(Debug)]
pub struct DeletionVectorFileWriter {
    file_io: FileIO,
    output_file_path: String,
    pending: BTreeMap<String, PendingDeletionVector>,
    parsed_parquet_position_deletes:
        HashMap<ExistingPositionDeleteCacheKey, HashMap<String, DeleteVector>>,
}

impl DeletionVectorFileWriter {
    /// Creates a writer for one output Puffin file.
    pub fn new(file_io: FileIO, output_file_path: impl Into<String>) -> Self {
        Self {
            file_io,
            output_file_path: output_file_path.into(),
            pending: BTreeMap::new(),
            parsed_parquet_position_deletes: HashMap::new(),
        }
    }

    /// Marks one row position as deleted for `target`.
    pub fn delete(&mut self, target: ReferencedDataFile, pos: u64) -> Result<()> {
        self.pending_for(target)?.positions.insert(pos);
        Ok(())
    }

    /// Marks a set of row positions as deleted for `target`.
    pub fn delete_all<I>(
        &mut self,
        target: ReferencedDataFile,
        positions: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = u64>,
    {
        let pending = self.pending_for(target)?;
        for pos in positions {
            pending.positions.insert(pos);
        }
        Ok(())
    }

    /// Merges an existing position delete file into the pending DV for
    /// `target`.
    ///
    /// Existing Puffin deletion vectors are read by blob offset/length. Existing
    /// Parquet position deletes are parsed and converted to the same bitmap
    /// representation. File-scoped deletes can be removed from metadata after
    /// the merge; broad-scoped deletes are retained because they may still
    /// apply to other data files.
    pub fn merge_existing_delete_file(
        &mut self,
        target: ReferencedDataFile,
        delete_file: &DataFile,
        file_io: &FileIO,
    ) -> Result<ExistingPositionDeleteMerge> {
        let delete_file = ExistingPositionDeleteFile::from_data_file(delete_file)?;
        self.merge_existing_position_delete_file(target, &delete_file, file_io)
    }

    /// Merges an existing position delete descriptor into the pending DV for
    /// `target`.
    pub fn merge_existing_position_delete_file(
        &mut self,
        target: ReferencedDataFile,
        delete_file: &ExistingPositionDeleteFile<'_>,
        file_io: &FileIO,
    ) -> Result<ExistingPositionDeleteMerge> {
        let target_path = target.file_path().to_owned();
        let is_file_scoped = delete_file.referenced_data_file.is_some();

        if let Some(delete_target) = delete_file.referenced_data_file
            && delete_target != target_path.as_str()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "position delete file {} targets {}, not {}",
                    delete_file.file_path, delete_target, target_path
                ),
            ));
        }

        match delete_file.file_format {
            DataFileFormat::Puffin => {
                let delete_target = delete_file.referenced_data_file.ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "deletion vector delete file {} is missing referenced_data_file",
                            delete_file.file_path
                        ),
                    )
                })?;
                if delete_target != target_path.as_str() {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "deletion vector delete file {} targets {}, not {}",
                            delete_file.file_path, delete_target, target_path
                        ),
                    ));
                }
                let delete_vector =
                    Self::read_existing_deletion_vector(delete_file, file_io)?;
                self.merge_delete_vector(target, &delete_vector)?;
                Ok(ExistingPositionDeleteMerge::removable())
            }
            DataFileFormat::Parquet => {
                self.merge_existing_parquet_position_delete(
                    target,
                    delete_file,
                    file_io,
                    is_file_scoped,
                )?;
                Ok(if is_file_scoped {
                    ExistingPositionDeleteMerge::removable()
                } else {
                    ExistingPositionDeleteMerge::retained()
                })
            }
            format => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "cannot merge position delete file format {format} into deletion vector",
                ),
            )),
        }
    }

    /// Writes all pending deletion vectors and returns their delete-file metadata.
    pub fn close(self) -> Result<DeletionVectorWriteResult> {
        let Self {
            file_io,
            output_file_path,
            pending,
            parsed_parquet_position_deletes: _,
        } = self;

        if pending.is_empty() {
            return Ok(DeletionVectorWriteResult::default());
        }
        if pending.values().all(|pending| pending.positions.is_empty()) {
            return Ok(DeletionVectorWriteResult::default());
        }

        let output_file = file_io.new_output(&output_file_path)?;
        let mut writer = PuffinWriter::new(output_file, HashMap::new(), false)?;
        let mut pending_outputs = Vec::with_capacity(pending.len());

        for pending in pending.into_values() {
            if pending.positions.is_empty() {
                continue;
            }

            let cardinality = pending.positions.cardinality();
            let blob = Self::blob_for(&pending.target, &pending.positions)?;
            let blob_metadata =
                writer.add_with_metadata(blob, CompressionCodec::None)?;
            pending_outputs.push((pending.target, cardinality, blob_metadata));
        }

        writer.close()?;

        let file_size_in_bytes =
            file_io.new_input(&output_file_path)?.metadata()?.size;

        let mut delete_files = Vec::with_capacity(pending_outputs.len());
        let mut referenced_data_files = Vec::with_capacity(pending_outputs.len());
        for (target, cardinality, blob_metadata) in pending_outputs {
            referenced_data_files.push(target.file_path.clone());
            delete_files.push(Self::delete_file(
                &output_file_path,
                file_size_in_bytes,
                target,
                cardinality,
                blob_metadata.offset(),
                blob_metadata.length(),
            )?);
        }

        Ok(DeletionVectorWriteResult {
            delete_files,
            referenced_data_files,
        })
    }

    fn merge_delete_vector(
        &mut self,
        target: ReferencedDataFile,
        delete_vector: &DeleteVector,
    ) -> Result<()> {
        self.pending_for(target)?.positions.merge_ref(delete_vector);
        Ok(())
    }

    fn read_existing_deletion_vector(
        delete_file: &ExistingPositionDeleteFile<'_>,
        file_io: &FileIO,
    ) -> Result<DeleteVector> {
        let offset = delete_file.content_offset.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector delete file {} is missing content_offset",
                    delete_file.file_path
                ),
            )
        })?;
        let size = delete_file.content_size_in_bytes.ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "deletion vector delete file {} is missing content_size_in_bytes",
                    delete_file.file_path
                ),
            )
        })?;
        let offset = u64::try_from(offset).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                "deletion vector content_offset cannot be negative",
            )
        })?;
        let size = u64::try_from(size).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                "deletion vector content_size_in_bytes cannot be negative",
            )
        })?;
        let end = offset.checked_add(size).ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "deletion vector range overflow")
        })?;

        let reader = file_io.new_input(delete_file.file_path)?.reader()?;
        let bytes = reader.read_range(offset..end)?;
        DeleteVector::from_puffin_v1_bytes(&bytes, delete_file.record_count)
    }

    fn merge_existing_parquet_position_delete(
        &mut self,
        target: ReferencedDataFile,
        delete_file: &ExistingPositionDeleteFile<'_>,
        file_io: &FileIO,
        is_file_scoped: bool,
    ) -> Result<()> {
        let key = ExistingPositionDeleteCacheKey::from_existing(delete_file);
        if !self.parsed_parquet_position_deletes.contains_key(&key) {
            let loader = BasicDeleteFileLoader::new(file_io.clone());
            let iterator = loader.parquet_to_batch_iterator(
                delete_file.file_path,
                delete_file.file_size_in_bytes,
            )?;
            let deletes_by_file =
                CachingDeleteFileLoader::parse_positional_deletes_record_batch_iterator(
                    iterator,
                )?;
            self.parsed_parquet_position_deletes
                .insert(key.clone(), deletes_by_file);
        }

        let deletes_by_file = self
            .parsed_parquet_position_deletes
            .get(&key)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "position delete cache entry missing after load",
                )
            })?;
        let target_path = target.file_path().to_owned();
        if is_file_scoped && deletes_by_file.keys().any(|path| path != &target_path) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "file-scoped position delete file {} contains rows for another data file",
                    delete_file.file_path
                ),
            ));
        }
        if let Some(delete_vector) = deletes_by_file.get(&target_path) {
            Self::merge_delete_vector_into(&mut self.pending, target, delete_vector)?;
        }
        Ok(())
    }

    fn merge_delete_vector_into(
        pending: &mut BTreeMap<String, PendingDeletionVector>,
        target: ReferencedDataFile,
        delete_vector: &DeleteVector,
    ) -> Result<()> {
        Self::pending_for_map(pending, target)?
            .positions
            .merge_ref(delete_vector);
        Ok(())
    }

    fn pending_for(
        &mut self,
        target: ReferencedDataFile,
    ) -> Result<&mut PendingDeletionVector> {
        Self::pending_for_map(&mut self.pending, target)
    }

    fn pending_for_map(
        pending: &mut BTreeMap<String, PendingDeletionVector>,
        target: ReferencedDataFile,
    ) -> Result<&mut PendingDeletionVector> {
        use std::collections::btree_map::Entry;

        match pending.entry(target.file_path.clone()) {
            Entry::Occupied(entry) => {
                let pending = entry.into_mut();
                if pending.target.partition_spec_id != target.partition_spec_id
                    || pending.target.partition != target.partition
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "conflicting metadata for deletion vector target {}",
                            target.file_path
                        ),
                    ));
                }
                Ok(pending)
            }
            Entry::Vacant(entry) => Ok(entry.insert(PendingDeletionVector {
                target,
                positions: DeleteVector::default(),
            })),
        }
    }

    fn blob_for(
        target: &ReferencedDataFile,
        positions: &DeleteVector,
    ) -> Result<Blob> {
        let mut properties = HashMap::with_capacity(2);
        properties.insert(
            REFERENCED_DATA_FILE_PROPERTY.to_owned(),
            target.file_path.clone(),
        );
        properties.insert(
            CARDINALITY_PROPERTY.to_owned(),
            positions.cardinality().to_string(),
        );

        Ok(Blob::builder()
            .r#type(DELETION_VECTOR_V1.to_owned())
            .fields(vec![RESERVED_FIELD_ID_DELETE_FILE_POS])
            .snapshot_id(-1)
            .sequence_number(-1)
            .data(positions.to_puffin_v1_bytes()?)
            .properties(properties)
            .build())
    }

    fn delete_file(
        puffin_file_path: &str,
        file_size_in_bytes: u64,
        target: ReferencedDataFile,
        cardinality: u64,
        content_offset: u64,
        content_size_in_bytes: u64,
    ) -> Result<DataFile> {
        let content_offset = i64::try_from(content_offset).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                "deletion vector content offset does not fit Iceberg long",
            )
        })?;
        let content_size_in_bytes =
            i64::try_from(content_size_in_bytes).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector content size does not fit Iceberg long",
                )
            })?;

        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::PositionDeletes)
            .file_path(puffin_file_path.to_owned())
            .file_format(DataFileFormat::Puffin)
            .partition(target.partition)
            .partition_spec_id(target.partition_spec_id)
            .record_count(cardinality)
            .file_size_in_bytes(file_size_in_bytes)
            .equality_ids(None)
            .referenced_data_file(Some(target.file_path))
            .content_offset(Some(content_offset))
            .content_size_in_bytes(Some(content_size_in_bytes));

        builder.build().map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("failed to build deletion vector delete file: {err}"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn writes_one_delete_file_per_referenced_data_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::local();
        let path = temp_dir.path().join("dv.puffin");
        let path = path.to_str().unwrap();

        let mut writer = DeletionVectorFileWriter::new(file_io, path);
        writer
            .delete_all(
                ReferencedDataFile::new("data-a.parquet", Struct::empty(), 0),
                [1, 3, 5],
            )
            .unwrap();
        writer
            .delete(
                ReferencedDataFile::new("data-b.parquet", Struct::empty(), 0),
                8,
            )
            .unwrap();

        let result = writer.close().unwrap();

        assert_eq!(result.delete_files().len(), 2);
        assert_eq!(
            result.delete_files()[0].file_format(),
            DataFileFormat::Puffin
        );
        assert_eq!(
            result.delete_files()[0].content_type(),
            DataContentType::PositionDeletes
        );
        assert!(result.delete_files()[0].content_offset().is_some());
        assert!(result.delete_files()[0].content_size_in_bytes().is_some());
    }

    #[test]
    fn merges_existing_puffin_deletion_vector() {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::local();
        let target = ReferencedDataFile::new("data-a.parquet", Struct::empty(), 0);

        let old_path = temp_dir.path().join("old.puffin");
        let old_path = old_path.to_str().unwrap();
        let mut old_writer = DeletionVectorFileWriter::new(file_io.clone(), old_path);
        old_writer
            .delete_all(target.clone(), [1, 2])
            .expect("write old delete positions");
        let old_delete_file = old_writer
            .close()
            .expect("close old writer")
            .into_delete_files()
            .pop()
            .expect("old delete file");

        let new_path = temp_dir.path().join("new.puffin");
        let new_path = new_path.to_str().unwrap();
        let mut new_writer = DeletionVectorFileWriter::new(file_io.clone(), new_path);
        new_writer
            .merge_existing_delete_file(target.clone(), &old_delete_file, &file_io)
            .expect("merge old deletion vector");
        new_writer
            .delete_all(target, [2, 3])
            .expect("write new delete positions");
        let new_delete_file = new_writer
            .close()
            .expect("close new writer")
            .into_delete_files()
            .pop()
            .expect("new delete file");

        let new_delete_file =
            ExistingPositionDeleteFile::from_data_file(&new_delete_file)
                .expect("new delete descriptor");
        let delete_vector = DeletionVectorFileWriter::read_existing_deletion_vector(
            &new_delete_file,
            &file_io,
        )
        .expect("read merged deletion vector");
        assert_eq!(delete_vector.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }
}
