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

use crate::scan::{DeleteFileContext, FileScanTaskDeleteFile};
use crate::spec::{DataContentType, DataFile, Struct};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Builder for constructing a `DeleteFileIndex`.
/// Used during the construction phase where mutation is needed.
///
/// This builder is thread-safe and supports concurrent insertion via `Mutex`.
/// After construction is complete, call `build()` to get an immutable,
/// lock-free `DeleteFileIndex` for querying.
#[derive(Debug, Default)]
pub(crate) struct DeleteFileIndexBuilder {
    inner: Mutex<PopulatedDeleteFileIndex>,
}

impl DeleteFileIndexBuilder {
    /// Create a new empty builder.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Insert a delete file context into the index.
    ///
    /// This method is thread-safe and can be called concurrently from multiple threads.
    pub(crate) fn insert(&self, ctx: DeleteFileContext) {
        self.inner.lock().unwrap().insert(ctx);
    }

    /// Consume the builder and return an immutable, shareable `DeleteFileIndex`.
    ///
    /// After calling this method, the resulting `DeleteFileIndex` is completely
    /// lock-free and can be queried concurrently without any synchronization overhead.
    pub(crate) fn build(self) -> DeleteFileIndex {
        DeleteFileIndex {
            inner: Arc::new(self.inner.into_inner().unwrap()),
        }
    }
}

/// Immutable, shareable index of delete files.
/// Clone is cheap as it only increments the Arc reference count.
#[derive(Debug, Clone)]
pub(crate) struct DeleteFileIndex {
    inner: Arc<PopulatedDeleteFileIndex>,
}

impl DeleteFileIndex {
    /// Gets all the delete files that apply to the specified data file.
    /// This is a synchronous operation.
    pub(crate) fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        self.inner.get_deletes_for_data_file(data_file, seq_num)
    }
}

#[derive(Debug, Default)]
struct PopulatedDeleteFileIndex {
    global_equality_deletes: Vec<Arc<DeleteFileContext>>,
    eq_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<Arc<DeleteFileContext>>>,
    pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,
    // TODO: Deletion Vector support
}

impl PopulatedDeleteFileIndex {
    fn insert(&mut self, ctx: DeleteFileContext) {
        let arc_ctx = Arc::new(ctx);

        // The spec states that "Equality delete files stored with an unpartitioned spec are applied as global deletes".
        match arc_ctx.manifest_entry.content_type() {
            DataContentType::EqualityDeletes => {
                if arc_ctx
                    .manifest_entry
                    .data_file()
                    .partition()
                    .fields()
                    .is_empty()
                {
                    self.global_equality_deletes.push(arc_ctx);
                    return;
                }

                let partition =
                    arc_ctx.manifest_entry.data_file().partition().clone();
                self.eq_deletes_by_partition
                    .entry(partition)
                    .or_insert_with(Vec::new)
                    .push(arc_ctx);
            }
            DataContentType::PositionDeletes => {
                if let Some(target_path) = arc_ctx
                    .manifest_entry
                    .data_file()
                    .position_delete_target_data_file_path()
                    .map(str::to_owned)
                {
                    self.pos_deletes_by_path
                        .entry(target_path)
                        .or_insert_with(Vec::new)
                        .push(arc_ctx);
                } else {
                    let partition =
                        arc_ctx.manifest_entry.data_file().partition().clone();
                    self.pos_deletes_by_partition
                        .entry(partition)
                        .or_insert_with(Vec::new)
                        .push(arc_ctx);
                }
            }
            DataContentType::Data => unreachable!(),
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<FileScanTaskDeleteFile> {
        let mut results = vec![];

        self.global_equality_deletes
            .iter()
            // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
            .filter(|&delete| {
                seq_num
                    .map(|seq_num| {
                        delete.manifest_entry.sequence_number() > Some(seq_num)
                    })
                    .unwrap_or_else(|| true)
            })
            .for_each(|delete| results.push(delete.as_ref().into()));

        if let Some(deletes) = self.eq_deletes_by_partition.get(data_file.partition())
        {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| {
                            delete.manifest_entry.sequence_number() > Some(seq_num)
                        })
                        .unwrap_or_else(|| true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        if let Some(deletes) =
            self.pos_deletes_by_partition.get(data_file.partition())
        {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| {
                            delete.manifest_entry.sequence_number() >= Some(seq_num)
                        })
                        .unwrap_or_else(|| true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        if let Some(deletes) = self.pos_deletes_by_path.get(data_file.file_path()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&delete| {
                    seq_num
                        .map(|seq_num| {
                            delete.manifest_entry.sequence_number() >= Some(seq_num)
                        })
                        .unwrap_or_else(|| true)
                })
                .for_each(|delete| results.push(delete.as_ref().into()));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use uuid::Uuid;

    use super::*;
    use crate::metadata_columns::RESERVED_FIELD_ID_DELETE_FILE_PATH;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal,
        ManifestEntry, ManifestStatus, Struct,
    };

    #[test]
    fn test_delete_file_index_unpartitioned() {
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(5, &build_unpartitioned_pos_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_pos_delete()),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: 0,
            })
            .collect();

        let builder = DeleteFileIndexBuilder::new();
        for ctx in delete_contexts {
            builder.insert(ctx);
        }
        let delete_file_index = builder.build();

        let data_file = build_unpartitioned_data_file();

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> =
            delete_files_to_apply_for_seq_4
                .into_iter()
                .map(|file| file.file_path)
                .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> =
            delete_files_to_apply_for_seq_5
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(6));
        let actual_paths_to_apply_for_seq_6: Vec<String> =
            delete_files_to_apply_for_seq_6
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // The 2 global equality deletes should match against any partitioned file
        let partitioned_file = build_partitioned_data_file(
            &Struct::from_iter([Some(Literal::long(100))]),
            1,
        );

        let delete_files_to_apply_for_partitioned_file =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        let actual_paths_to_apply_for_partitioned_file: Vec<String> =
            delete_files_to_apply_for_partitioned_file
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_partitioned_file,
            delete_file_paths[..2]
        );
    }

    #[test]
    fn test_delete_file_index_partitioned() {
        let partition_one = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(
                4,
                &build_partitioned_eq_delete(&partition_one, spec_id),
            ),
            build_added_manifest_entry(
                6,
                &build_partitioned_eq_delete(&partition_one, spec_id),
            ),
            build_added_manifest_entry(
                5,
                &build_partitioned_pos_delete(&partition_one, spec_id),
            ),
            build_added_manifest_entry(
                6,
                &build_partitioned_pos_delete(&partition_one, spec_id),
            ),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
            })
            .collect();

        let builder = DeleteFileIndexBuilder::new();
        for ctx in delete_contexts {
            builder.insert(ctx);
        }
        let delete_file_index = builder.build();

        let partitioned_file = build_partitioned_data_file(
            &Struct::from_iter([Some(Literal::long(100))]),
            spec_id,
        );

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> =
            delete_files_to_apply_for_seq_4
                .into_iter()
                .map(|file| file.file_path)
                .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> =
            delete_files_to_apply_for_seq_5
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(6));
        let actual_paths_to_apply_for_seq_6: Vec<String> =
            delete_files_to_apply_for_seq_6
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // Data file with different partition tuples does not match any delete files
        let partitioned_second_file = build_partitioned_data_file(
            &Struct::from_iter([Some(Literal::long(200))]),
            1,
        );
        let delete_files_to_apply_for_different_partition = delete_file_index
            .get_deletes_for_data_file(&partitioned_second_file, Some(0));
        let actual_paths_to_apply_for_different_partition: Vec<String> =
            delete_files_to_apply_for_different_partition
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_partition.is_empty());

        // Data file with same tuple but different spec ID does not match any delete files
        let partitioned_different_spec =
            build_partitioned_data_file(&partition_one, 2);
        let delete_files_to_apply_for_different_spec = delete_file_index
            .get_deletes_for_data_file(&partitioned_different_spec, Some(0));
        let actual_paths_to_apply_for_different_spec: Vec<String> =
            delete_files_to_apply_for_different_spec
                .into_iter()
                .map(|file| file.file_path)
                .collect();
        assert!(actual_paths_to_apply_for_different_spec.is_empty());
    }

    #[test]
    fn test_position_delete_referenced_data_file_filter() {
        let partition = Struct::empty();
        let data_file = build_unpartitioned_data_file();
        let unrelated_data_file = build_unpartitioned_data_file();
        let delete_file =
            build_pos_delete_for_data_file(&partition, 0, data_file.file_path());

        let builder = DeleteFileIndexBuilder::new();
        builder.insert(DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &delete_file).into(),
            partition_spec_id: 0,
        });
        let delete_file_index = builder.build();

        assert_eq!(
            delete_file_index
                .get_deletes_for_data_file(&data_file, Some(0))
                .len(),
            1
        );
        assert!(
            delete_file_index
                .get_deletes_for_data_file(&unrelated_data_file, Some(0))
                .is_empty()
        );
    }

    #[test]
    fn test_position_delete_file_path_bounds_filter() {
        let partition = Struct::empty();
        let data_file = build_unpartitioned_data_file();
        let unrelated_data_file = build_unpartitioned_data_file();
        let delete_file = build_pos_delete_with_file_path_bounds(
            &partition,
            0,
            data_file.file_path(),
            data_file.file_path(),
        );

        let builder = DeleteFileIndexBuilder::new();
        builder.insert(DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &delete_file).into(),
            partition_spec_id: 0,
        });
        let delete_file_index = builder.build();

        assert_eq!(
            delete_file_index
                .get_deletes_for_data_file(&data_file, Some(0))
                .len(),
            1
        );
        assert!(
            delete_file_index
                .get_deletes_for_data_file(&unrelated_data_file, Some(0))
                .is_empty()
        );
    }

    #[test]
    fn test_position_delete_file_path_scope_does_not_require_matching_partition() {
        let data_file = build_unpartitioned_data_file();
        let delete_file = build_pos_delete_for_data_file(
            &Struct::from_iter([Some(Literal::long(100))]),
            1,
            data_file.file_path(),
        );

        let builder = DeleteFileIndexBuilder::new();
        builder.insert(DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &delete_file).into(),
            partition_spec_id: 1,
        });
        let delete_file_index = builder.build();

        assert_eq!(
            delete_file_index
                .get_deletes_for_data_file(&data_file, Some(0))
                .len(),
            1
        );
    }

    #[test]
    fn test_position_delete_non_file_scoped_bounds_stay_partition_scoped() {
        let partition = Struct::empty();
        let data_file = build_unpartitioned_data_file();
        let unrelated_data_file = build_unpartitioned_data_file();
        let delete_file = build_pos_delete_with_file_path_bounds(
            &partition,
            0,
            data_file.file_path(),
            unrelated_data_file.file_path(),
        );

        let builder = DeleteFileIndexBuilder::new();
        builder.insert(DeleteFileContext {
            manifest_entry: build_added_manifest_entry(5, &delete_file).into(),
            partition_spec_id: 0,
        });
        let delete_file_index = builder.build();

        assert_eq!(
            delete_file_index
                .get_deletes_for_data_file(&data_file, Some(0))
                .len(),
            1
        );
        assert_eq!(
            delete_file_index
                .get_deletes_for_data_file(&unrelated_data_file, Some(0))
                .len(),
            1
        );
    }

    fn build_unpartitioned_eq_delete() -> DataFile {
        build_partitioned_eq_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_eq_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}_equality_delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::EqualityDeletes)
            .equality_ids(Some(vec![1]))
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_pos_delete() -> DataFile {
        build_partitioned_pos_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_pos_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_pos_delete_for_data_file(
        partition: &Struct,
        spec_id: i32,
        data_file_path: &str,
    ) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .referenced_data_file(Some(data_file_path.to_owned()))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_pos_delete_with_file_path_bounds(
        partition: &Struct,
        spec_id: i32,
        lower_path: &str,
        upper_path: &str,
    ) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .lower_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                Datum::string(lower_path),
            )]))
            .upper_bounds(HashMap::from([(
                RESERVED_FIELD_ID_DELETE_FILE_PATH,
                Datum::string(upper_path),
            )]))
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_data_file() -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_partitioned_data_file(
        partition_value: &Struct,
        spec_id: i32,
    ) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition_value.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_added_manifest_entry(
        data_seq_number: i64,
        file: &DataFile,
    ) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(data_seq_number)
            .data_file(file.clone())
            .build()
    }
}
