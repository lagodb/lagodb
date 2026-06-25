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

use std::sync::Arc;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::Result;
use crate::arrow::ArrowReader;
use crate::arrow::reader::ParquetReadOptions;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::scan_metrics::ScanMetrics;
use crate::io::FileIO;
use crate::scan::{ArrowRecordBatchIterator, FileScanTaskDeleteFile};
use crate::spec::{Schema, SchemaRef};

/// Delete File Loader
#[allow(unused)]
pub trait DeleteFileLoader {
    /// Read the delete file referred to in the task
    ///
    /// Returns the contents of the delete file as a RecordBatch stream. Applies schema evolution.
    fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef,
    ) -> Result<ArrowRecordBatchIterator>;
}

#[derive(Clone, Debug)]
pub(crate) struct BasicDeleteFileLoader {
    file_io: FileIO,
    scan_metrics: Option<ScanMetrics>,
}

#[allow(unused_variables)]
impl BasicDeleteFileLoader {
    pub fn new(file_io: FileIO) -> Self {
        BasicDeleteFileLoader {
            file_io,
            scan_metrics: None,
        }
    }

    pub(crate) fn with_scan_metrics(mut self, scan_metrics: ScanMetrics) -> Self {
        self.scan_metrics = Some(scan_metrics);
        self
    }

    /// Loads a RecordBatchIterator for a given datafile.
    pub(crate) fn parquet_to_batch_iterator(
        &self,
        data_file_path: &str,
        file_size_in_bytes: u64,
    ) -> Result<ArrowRecordBatchIterator> {
        /*
           Essentially a super-cut-down ArrowReader. We can't use ArrowReader directly
           as that introduces a circular dependency.
        */
        let (parquet_file_reader, arrow_metadata) = ArrowReader::open_parquet_file(
            data_file_path,
            &self.file_io,
            file_size_in_bytes,
            ParquetReadOptions::default(),
            self.scan_metrics.clone(),
        )?;

        let record_batch_reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            parquet_file_reader,
            arrow_metadata,
        )
        .build()?;

        let iterator = record_batch_reader.map(|batch| batch.map_err(|e| e.into()));

        Ok(Box::new(iterator) as ArrowRecordBatchIterator)
    }

    /// Evolves the schema of the RecordBatches from an equality delete file.
    ///
    /// Per the [Iceberg spec](https://iceberg.apache.org/spec/#equality-delete-files),
    /// only evolves the specified `equality_ids` columns, not all table columns.
    pub(crate) fn evolve_schema(
        record_batch_iterator: ArrowRecordBatchIterator,
        target_schema: Arc<Schema>,
        equality_ids: &[i32],
    ) -> Result<ArrowRecordBatchIterator> {
        let mut record_batch_transformer =
            RecordBatchTransformerBuilder::new(target_schema.clone(), equality_ids)
                .build();

        let iterator = record_batch_iterator.map(move |record_batch| {
            record_batch.and_then(|record_batch| {
                record_batch_transformer.process_record_batch(record_batch)
            })
        });

        Ok(Box::new(iterator) as ArrowRecordBatchIterator)
    }
}

impl DeleteFileLoader for BasicDeleteFileLoader {
    fn read_delete_file(
        &self,
        task: &FileScanTaskDeleteFile,
        schema: SchemaRef,
    ) -> Result<ArrowRecordBatchIterator> {
        let raw_batch_iterator =
            self.parquet_to_batch_iterator(&task.file_path, task.file_size_in_bytes)?;

        // For equality deletes, only evolve the equality_ids columns.
        // For positional deletes (equality_ids is None), use all field IDs.
        let field_ids = match &task.equality_ids {
            Some(ids) => ids.clone(),
            None => schema.field_id_to_name_map().keys().cloned().collect(),
        };

        Self::evolve_schema(raw_batch_iterator, schema, &field_ids)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::delete_filter::tests::setup;

    #[test]
    fn test_basic_delete_file_loader_read_delete_file() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io =
            FileIO::from_path(table_location.as_os_str().to_str().unwrap()).unwrap();

        let delete_file_loader = BasicDeleteFileLoader::new(file_io.clone());

        let file_scan_tasks = setup(table_location);

        let result = delete_file_loader
            .read_delete_file(
                &file_scan_tasks[0].deletes[0],
                file_scan_tasks[0].schema_ref(),
            )
            .unwrap();

        let result = result.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(result.len(), 1);
    }
}
