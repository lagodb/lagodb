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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::delete_vector::DeleteVector;
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::scan::{FileScanTask, FileScanTaskDeleteFile};
use crate::spec::DataContentType;
use crate::{Error, ErrorKind, Result};
use once_cell::sync::OnceCell;

/// Result of loading a positional delete file.
/// Contains delete vectors keyed by data file path.
#[derive(Debug, Default)]
pub(crate) struct PosDelLoadResult {
    #[allow(dead_code)]
    pub del_vecs: HashMap<String, DeleteVector>,
}

/// Result of loading an equality delete file.
/// Contains the predicate that filters deleted rows.
#[derive(Debug)]
pub(crate) struct EqDelLoadResult {
    pub predicate: Predicate,
}

#[derive(Debug, Default)]
struct DeleteFileFilterState {
    /// Merged delete vectors, keyed by data file path.
    /// Multiple positional delete files may contribute to the same data file's delete vector.
    delete_vectors: HashMap<String, Arc<Mutex<DeleteVector>>>,

    /// One OnceCell per positional delete file.
    /// Ensures each file is loaded exactly once, even with concurrent access.
    positional_deletes: HashMap<String, Arc<OnceCell<PosDelLoadResult>>>,

    /// One OnceCell per equality delete file.
    /// Ensures each file is loaded exactly once, even with concurrent access.
    equality_deletes: HashMap<String, Arc<OnceCell<EqDelLoadResult>>>,
}

/// Thread-safe delete filter with OnceCell-based concurrent loading.
///
/// This filter uses `OnceCell` to ensure each delete file is loaded exactly once,
/// even when multiple threads attempt to load the same file concurrently.
/// Other threads will block and wait for the first loader to complete,
/// then reuse the cached result.
#[derive(Clone, Debug, Default)]
pub(crate) struct DeleteFilter {
    state: Arc<RwLock<DeleteFileFilterState>>,
}

impl DeleteFilter {
    /// Retrieve a delete vector for the data file associated with a given file scan task.
    pub(crate) fn get_delete_vector(
        &self,
        file_scan_task: &FileScanTask,
    ) -> Option<Arc<Mutex<DeleteVector>>> {
        self.get_delete_vector_for_path(file_scan_task.data_file_path())
    }

    /// Retrieve a delete vector for a data file by its path.
    pub(crate) fn get_delete_vector_for_path(
        &self,
        data_file_path: &str,
    ) -> Option<Arc<Mutex<DeleteVector>>> {
        self.state
            .read()
            .ok()
            .and_then(|st| st.delete_vectors.get(data_file_path).cloned())
    }

    /// Get or create a OnceCell for a positional delete file.
    /// This ensures we have a single OnceCell instance per file path.
    fn get_or_create_pos_del_cell(
        &self,
        file_path: &str,
    ) -> Arc<OnceCell<PosDelLoadResult>> {
        let mut state = self.state.write().unwrap();
        state
            .positional_deletes
            .entry(file_path.to_string())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    }

    /// Get or create a OnceCell for an equality delete file.
    fn get_or_create_eq_del_cell(
        &self,
        file_path: &str,
    ) -> Arc<OnceCell<EqDelLoadResult>> {
        let mut state = self.state.write().unwrap();
        state
            .equality_deletes
            .entry(file_path.to_string())
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    }

    /// Load a positional delete file using the provided loader function.
    ///
    /// This method is concurrency-safe: if multiple threads call this for the same file,
    /// only one will execute the loader, others will wait and reuse the result.
    ///
    /// The loader function should return a map of data file path -> delete vector.
    pub(crate) fn load_pos_del_file<F>(
        &self,
        file_path: &str,
        loader: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<HashMap<String, DeleteVector>>,
    {
        // Get or create the OnceLock for this file
        let cell = self.get_or_create_pos_del_cell(file_path);

        // get_or_try_init guarantees only one thread executes the loader
        // Other threads will block and wait, then get the cached result
        cell.get_or_try_init(|| {
            let del_vecs = loader()?;

            // Merge the results into the combined delete_vectors map
            for (data_file_path, dv) in del_vecs {
                self.upsert_delete_vector(data_file_path, dv);
            }
            Ok::<_, Error>(PosDelLoadResult {
                del_vecs: HashMap::new(),
            })
        })?;

        Ok(())
    }

    /// Load an equality delete file using the provided loader function.
    ///
    /// This method is concurrency-safe: if multiple threads call this for the same file,
    /// only one will execute the loader, others will wait and reuse the result.
    ///
    /// The loader function should return the predicate for this delete file.
    pub(crate) fn load_eq_del_file<F>(&self, file_path: &str, loader: F) -> Result<()>
    where
        F: FnOnce() -> Result<Predicate>,
    {
        // Get or create the OnceLock for this file
        let cell = self.get_or_create_eq_del_cell(file_path);

        // get_or_try_init guarantees only one thread executes the loader
        cell.get_or_try_init(|| {
            let predicate = loader()?;
            Ok::<_, Error>(EqDelLoadResult { predicate })
        })?;

        Ok(())
    }

    /// Retrieve the equality delete predicate for a given delete file path.
    /// Returns None if the file hasn't been loaded yet.
    pub(crate) fn get_equality_delete_predicate(
        &self,
        file_path: &str,
    ) -> Option<Predicate> {
        let state = self.state.read().ok()?;
        let cell = state.equality_deletes.get(file_path)?;
        cell.get().map(|r| r.predicate.clone())
    }

    /// Builds the combined equality delete predicate for the provided task.
    ///
    /// This method retrieves all cached equality delete predicates for the task's
    /// delete files, combines them with AND, and binds to the task's schema.
    pub(crate) fn build_equality_delete_predicate(
        &self,
        file_scan_task: &FileScanTask,
    ) -> Result<Option<BoundPredicate>> {
        let mut combined_predicate = AlwaysTrue;

        for delete in &file_scan_task.deletes {
            if !is_equality_delete(delete) {
                continue;
            }

            let Some(predicate) =
                self.get_equality_delete_predicate(&delete.file_path)
            else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Missing predicate for equality delete file '{}'. \
                         Ensure the delete file is loaded before calling this method.",
                        delete.file_path
                    ),
                ));
            };

            combined_predicate = combined_predicate.and(predicate);
        }

        if combined_predicate == AlwaysTrue {
            return Ok(None);
        }

        let bound_predicate = combined_predicate
            .bind(file_scan_task.schema.clone(), file_scan_task.case_sensitive)?;
        Ok(Some(bound_predicate))
    }

    /// Upsert (insert or merge) a delete vector for a data file path.
    pub(crate) fn upsert_delete_vector(
        &self,
        data_file_path: String,
        delete_vector: DeleteVector,
    ) {
        let mut state = self.state.write().unwrap();

        let Some(entry) = state.delete_vectors.get_mut(&data_file_path) else {
            state
                .delete_vectors
                .insert(data_file_path, Arc::new(Mutex::new(delete_vector)));
            return;
        };

        *entry.lock().unwrap() |= delete_vector;
    }
}

pub(crate) fn is_equality_delete(f: &FileScanTaskDeleteFile) -> bool {
    matches!(f.file_type, DataContentType::EqualityDeletes)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::Schema as ArrowSchema;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
    use crate::expr::Reference;
    use crate::io::FileIO;
    use crate::spec::{
        DataFileFormat, Datum, NestedField, PrimitiveType, Schema, Type,
    };

    type ArrowSchemaRef = Arc<ArrowSchema>;

    const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
    const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

    #[test]
    fn test_delete_file_filter_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io =
            FileIO::from_path(table_location.as_os_str().to_str().unwrap()).unwrap();

        let delete_file_loader = CachingDeleteFileLoader::new(file_io.clone(), 10);

        let file_scan_tasks = setup(table_location);

        let delete_filter = delete_file_loader
            .load_deletes(
                &file_scan_tasks[0].deletes,
                file_scan_tasks[0].schema_ref(),
            )
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();
        assert_eq!(result.lock().unwrap().len(), 12); // pos dels from pos del file 1 and 2

        let delete_filter = delete_file_loader
            .load_deletes(
                &file_scan_tasks[1].deletes,
                file_scan_tasks[1].schema_ref(),
            )
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[1])
            .unwrap();
        assert_eq!(result.lock().unwrap().len(), 8); // pos dels for file 2
    }

    pub(crate) fn setup(table_location: &Path) -> Vec<FileScanTask> {
        let data_file_schema = Arc::new(Schema::builder().build().unwrap());
        let positional_delete_schema = create_pos_del_schema();

        let file_path_values = [
            vec![format!("{}/1.parquet", table_location.to_str().unwrap()); 8],
            vec![format!("{}/1.parquet", table_location.to_str().unwrap()); 8],
            vec![format!("{}/2.parquet", table_location.to_str().unwrap()); 8],
        ];
        let pos_values = [
            vec![0i64, 1, 3, 5, 6, 8, 1022, 1023],
            vec![0i64, 1, 3, 5, 20, 21, 22, 23],
            vec![0i64, 1, 3, 5, 6, 8, 1022, 1023],
        ];

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for n in 1..=3 {
            let file_path_vals = file_path_values.get(n - 1).unwrap();
            let file_path_col =
                Arc::new(StringArray::from_iter_values(file_path_vals));

            let pos_vals = pos_values.get(n - 1).unwrap();
            let pos_col = Arc::new(Int64Array::from_iter_values(pos_vals.clone()));

            let positional_deletes_to_write = RecordBatch::try_new(
                positional_delete_schema.clone(),
                vec![file_path_col.clone(), pos_col.clone()],
            )
            .unwrap();

            let file = File::create(format!(
                "{}/pos-del-{}.parquet",
                table_location.to_str().unwrap(),
                n
            ))
            .unwrap();
            let mut writer = ArrowWriter::try_new(
                file,
                positional_deletes_to_write.schema(),
                Some(props.clone()),
            )
            .unwrap();

            writer
                .write(&positional_deletes_to_write)
                .expect("Writing batch");

            // writer must be closed to write footer
            writer.close().unwrap();
        }

        let pos_del_1 = FileScanTaskDeleteFile {
            file_size_in_bytes: 0,
            file_path: format!(
                "{}/pos-del-1.parquet",
                table_location.to_str().unwrap()
            ),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: None,
        };

        let pos_del_2 = FileScanTaskDeleteFile {
            file_size_in_bytes: 0,
            file_path: format!(
                "{}/pos-del-2.parquet",
                table_location.to_str().unwrap()
            ),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: None,
        };

        let pos_del_3 = FileScanTaskDeleteFile {
            file_size_in_bytes: 0,
            file_path: format!(
                "{}/pos-del-3.parquet",
                table_location.to_str().unwrap()
            ),
            file_type: DataContentType::PositionDeletes,
            partition_spec_id: 0,
            equality_ids: None,
        };

        let file_scan_tasks = vec![
            FileScanTask {
                file_size_in_bytes: 0,
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!(
                    "{}/1.parquet",
                    table_location.to_str().unwrap()
                ),
                data_file_format: DataFileFormat::Parquet,
                schema: data_file_schema.clone(),
                project_field_ids: vec![],
                predicate: None,
                deletes: vec![pos_del_1, pos_del_2.clone()],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            },
            FileScanTask {
                file_size_in_bytes: 0,
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!(
                    "{}/2.parquet",
                    table_location.to_str().unwrap()
                ),
                data_file_format: DataFileFormat::Parquet,
                schema: data_file_schema.clone(),
                project_field_ids: vec![],
                predicate: None,
                deletes: vec![pos_del_3],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
            },
        ];

        file_scan_tasks
    }

    pub(crate) fn create_pos_del_schema() -> ArrowSchemaRef {
        let fields = vec![
            arrow_schema::Field::new(
                "file_path",
                arrow_schema::DataType::Utf8,
                false,
            )
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
            )])),
            arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
                )])),
        ];
        Arc::new(arrow_schema::Schema::new(fields))
    }

    #[test]
    fn test_build_equality_delete_predicate_case_sensitive() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "Id",
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        // ---------- fake FileScanTask ----------
        let task = FileScanTask {
            file_size_in_bytes: 0,
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: "data.parquet".to_string(),
            data_file_format: crate::spec::DataFileFormat::Parquet,
            schema: schema.clone(),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_size_in_bytes: 0,
                file_path: "eq-del.parquet".to_string(),
                file_type: DataContentType::EqualityDeletes,
                partition_spec_id: 0,
                equality_ids: None,
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        };

        let filter = DeleteFilter::default();

        // ---------- load equality delete predicate using OnceLock ----------
        let pred = Reference::new("id").equal_to(Datum::long(10));
        filter
            .load_eq_del_file("eq-del.parquet", || Ok(pred))
            .unwrap();

        // ---------- should FAIL (case mismatch: "id" vs "Id") ----------
        let result = filter.build_equality_delete_predicate(&task);

        assert!(
            result.is_err(),
            "case_sensitive=true should fail when column case mismatches"
        );
    }

    #[test]
    fn test_oncelock_prevents_duplicate_loading() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let filter = DeleteFilter::default();
        let load_count = Arc::new(AtomicUsize::new(0));
        let filter_clone = filter.clone();
        let load_count_clone = load_count.clone();

        // Spawn multiple threads that try to load the same file
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let filter = filter_clone.clone();
                let load_count = load_count_clone.clone();
                thread::spawn(move || {
                    filter
                        .load_pos_del_file("test-file.parquet", || {
                            // Count how many times the loader is actually called
                            load_count.fetch_add(1, Ordering::SeqCst);
                            // Simulate some work
                            thread::sleep(std::time::Duration::from_millis(10));
                            Ok(HashMap::new())
                        })
                        .unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // The loader should only be called once despite 10 concurrent calls
        assert_eq!(load_count.load(Ordering::SeqCst), 1);
    }
}
