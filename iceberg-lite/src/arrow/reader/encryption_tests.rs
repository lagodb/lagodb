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
use std::fs;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use tempfile::TempDir;

use super::ArrowReaderBuilder;
use crate::arrow::delete_file_loader::{BasicDeleteFileLoader, DeleteFileLoader};
use crate::arrow::delete_filter::tests::create_pos_del_schema;
use crate::arrow::test_utils::write_encrypted_parquet;
use crate::encryption::StandardKeyMetadata;
use crate::io::FileIO;
use crate::metadata_columns::{
    RESERVED_COL_NAME_DELETE_FILE_PATH, RESERVED_COL_NAME_DELETE_FILE_POS,
    RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
};
use crate::scan::{FileScanTask, FileScanTaskDeleteFile};
use crate::spec::{
    DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, Type,
};
use crate::{ErrorKind, Result};

fn iceberg_schema(primitive_type: PrimitiveType) -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(primitive_type))
                    .into(),
            ])
            .build()
            .unwrap(),
    )
}

fn arrow_schema(data_type: DataType) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
    ]))
}

fn data_file_task(
    file_path: String,
    file_size_in_bytes: u64,
    key_metadata: Option<Box<[u8]>>,
) -> FileScanTask {
    FileScanTask::builder()
        .with_file_size_in_bytes(file_size_in_bytes)
        .with_start(0)
        .with_length(0)
        .with_data_file_path(file_path)
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(iceberg_schema(PrimitiveType::Int))
        .with_project_field_ids(vec![1])
        .with_case_sensitive(false)
        .with_key_metadata(key_metadata)
        .build()
}

fn encoded_key_metadata(key: &[u8], aad_prefix: Option<&[u8]>) -> Box<[u8]> {
    let mut metadata = StandardKeyMetadata::try_new(key).unwrap();
    if let Some(aad) = aad_prefix {
        metadata = metadata.with_aad_prefix(aad);
    }
    metadata.encode().unwrap()
}

fn read_data_file(task: FileScanTask, file_io: FileIO) -> Result<Vec<RecordBatch>> {
    ArrowReaderBuilder::new(file_io)
        .build()
        .read(vec![task])?
        .collect()
}

fn assert_encrypted_parquet_roundtrip(encryption_key: &[u8]) {
    let aad_prefix = b"aad_prefix";
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path().to_str().unwrap();
    let file_io = FileIO::from_path(table_location).unwrap();
    let batch = RecordBatch::try_new(
        arrow_schema(DataType::Int32),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef],
    )
    .unwrap();

    let file_path = format!("{table_location}/encrypted.parquet");
    write_encrypted_parquet(&file_path, &batch, encryption_key, Some(aad_prefix));
    let task = data_file_task(
        file_path.clone(),
        fs::metadata(&file_path).unwrap().len(),
        Some(encoded_key_metadata(encryption_key, Some(aad_prefix))),
    );

    let batches = read_data_file(task, file_io).unwrap();
    assert_eq!(batches.len(), 1);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[10, 20, 30]);
}

#[test]
fn test_read_encrypted_parquet_aes_128() {
    assert_encrypted_parquet_roundtrip(b"0123456789abcdef");
}

#[test]
fn test_read_encrypted_parquet_aes_256() {
    assert_encrypted_parquet_roundtrip(b"0123456789abcdef0123456789abcdef");
}

#[test]
fn test_read_encrypted_parquet_without_key_metadata_fails() {
    let encryption_key = b"0123456789abcdef";
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path().to_str().unwrap();
    let file_io = FileIO::from_path(table_location).unwrap();
    let batch = RecordBatch::try_new(
        arrow_schema(DataType::Int32),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let file_path = format!("{table_location}/encrypted-no-key.parquet");
    write_encrypted_parquet(&file_path, &batch, encryption_key, None);
    let task = data_file_task(
        file_path.clone(),
        fs::metadata(&file_path).unwrap().len(),
        None,
    );

    let err = read_data_file(task, file_io).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    let err = err.to_string();
    assert!(err.contains("encrypted footer"), "unexpected error: {err}");
    assert!(
        err.contains("decryption properties were not provided"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_read_encrypted_parquet_with_wrong_key_fails() {
    let encryption_key = b"0123456789abcdef";
    let wrong_key = b"fedcba9876543210";
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path().to_str().unwrap();
    let file_io = FileIO::from_path(table_location).unwrap();
    let batch = RecordBatch::try_new(
        arrow_schema(DataType::Int32),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let file_path = format!("{table_location}/encrypted-wrong-key.parquet");
    write_encrypted_parquet(&file_path, &batch, encryption_key, None);
    let task = data_file_task(
        file_path.clone(),
        fs::metadata(&file_path).unwrap().len(),
        Some(encoded_key_metadata(wrong_key, None)),
    );

    let err = read_data_file(task, file_io).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unexpected);
    assert!(
        err.to_string().contains("unable to decrypt parquet footer"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_read_encrypted_positional_delete_file() {
    let encryption_key = b"0123456789abcdef";
    let aad_prefix = b"aad_prefix";
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path().to_str().unwrap();
    let file_io = FileIO::from_path(table_location).unwrap();
    let batch = RecordBatch::try_new(
        create_pos_del_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(vec!["data.parquet"; 4])),
            Arc::new(Int64Array::from(vec![0_i64, 1, 5, 10])),
        ],
    )
    .unwrap();
    let file_path = format!("{table_location}/encrypted-pos-del.parquet");
    write_encrypted_parquet(&file_path, &batch, encryption_key, Some(aad_prefix));
    let task = FileScanTaskDeleteFile::builder()
        .with_file_path(file_path.clone())
        .with_file_size_in_bytes(fs::metadata(&file_path).unwrap().len())
        .with_file_type(DataContentType::PositionDeletes)
        .with_partition_spec_id(0)
        .with_key_metadata(Some(encoded_key_metadata(
            encryption_key,
            Some(aad_prefix),
        )))
        .build();
    let schema = Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(
                    RESERVED_FIELD_ID_DELETE_FILE_PATH,
                    RESERVED_COL_NAME_DELETE_FILE_PATH,
                    Type::Primitive(PrimitiveType::String),
                )
                .into(),
                NestedField::required(
                    RESERVED_FIELD_ID_DELETE_FILE_POS,
                    RESERVED_COL_NAME_DELETE_FILE_POS,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
            ])
            .build()
            .unwrap(),
    );

    let batches: Vec<_> = BasicDeleteFileLoader::new(file_io)
        .read_delete_file(&task, schema)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 4);
}

#[test]
fn test_read_encrypted_equality_delete_file() {
    let encryption_key = b"0123456789abcdef";
    let aad_prefix = b"my-table-uuid!!";
    let tmp_dir = TempDir::new().unwrap();
    let table_location = tmp_dir.path().to_str().unwrap();
    let file_io = FileIO::from_path(table_location).unwrap();
    let batch = RecordBatch::try_new(
        arrow_schema(DataType::Int64),
        vec![Arc::new(Int64Array::from(vec![100_i64, 200, 300]))],
    )
    .unwrap();
    let file_path = format!("{table_location}/encrypted-eq-del.parquet");
    write_encrypted_parquet(&file_path, &batch, encryption_key, Some(aad_prefix));
    let task = FileScanTaskDeleteFile::builder()
        .with_file_path(file_path.clone())
        .with_file_size_in_bytes(fs::metadata(&file_path).unwrap().len())
        .with_file_type(DataContentType::EqualityDeletes)
        .with_partition_spec_id(0)
        .with_equality_ids(Some(vec![1]))
        .with_key_metadata(Some(encoded_key_metadata(
            encryption_key,
            Some(aad_prefix),
        )))
        .build();

    let batches: Vec<_> = BasicDeleteFileLoader::new(file_io)
        .read_delete_file(&task, iceberg_schema(PrimitiveType::Long))
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);
}
