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

//! ManifestList for Iceberg.

mod _const_schema;
pub(super) mod _serde;
mod manifest_file;
mod reader;
mod writer;

use apache_avro::types::Value;
use apache_avro::{Reader, from_value};
pub use manifest_file::*;
pub use reader::*;
pub use serde_bytes::ByteBuf;
pub use writer::*;

use self::_const_schema::MANIFEST_LIST_AVRO_SCHEMA_V1;
use super::FormatVersion;
use crate::error::Result;

/// Placeholder for sequence number. The field with this value must be replaced with the actual sequence number before it write.
pub const UNASSIGNED_SEQUENCE_NUMBER: i64 = -1;

/// Snapshots are embedded in table metadata, but the list of manifests for a
/// snapshot are stored in a separate manifest list file.
///
/// A new manifest list is written for each attempt to commit a snapshot
/// because the list of manifests always changes to produce a new snapshot.
/// When a manifest list is written, the (optimistic) sequence number of the
/// snapshot is written for all new manifest files tracked by the list.
///
/// A manifest list includes summary metadata that can be used to avoid
/// scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a
/// summary of values for each field of the partition spec used to write the
/// manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestList {
    /// Entries in a manifest list.
    entries: Vec<ManifestFile>,
}

impl ManifestList {
    /// Parse manifest list from bytes.
    pub fn parse_with_version(
        bs: &[u8],
        version: FormatVersion,
    ) -> Result<ManifestList> {
        match version {
            FormatVersion::V1 => {
                let reader = Reader::with_schema(&MANIFEST_LIST_AVRO_SCHEMA_V1, bs)?;
                let values = Value::Array(
                    reader.collect::<std::result::Result<Vec<Value>, _>>()?,
                );
                from_value::<_serde::ManifestListV1>(&values)?.try_into()
            }
            FormatVersion::V2 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(
                    reader.collect::<std::result::Result<Vec<Value>, _>>()?,
                );
                from_value::<_serde::ManifestListV2>(&values)?.try_into()
            }
            FormatVersion::V3 => {
                let reader = Reader::new(bs)?;
                let values = Value::Array(
                    reader.collect::<std::result::Result<Vec<Value>, _>>()?,
                );
                from_value::<_serde::ManifestListV3>(&values)?.try_into()
            }
        }
    }

    /// Get the entries in the manifest list.
    pub fn entries(&self) -> &[ManifestFile] {
        &self.entries
    }

    /// Take ownership of the entries in the manifest list, consuming it
    pub fn consume_entries(self) -> impl IntoIterator<Item = ManifestFile> {
        Box::new(self.entries.into_iter())
    }
}

#[cfg(test)]
mod test {
    use std::fs;

    use apache_avro::{Reader, Schema};
    use tempfile::TempDir;

    use super::_serde::ManifestListV2;
    use crate::encryption::{
        EncryptedInputFile, EncryptedOutputFile, StandardKeyMetadata,
    };
    use crate::io::FileIO;
    use crate::spec::manifest_list::_serde::{ManifestListV1, ManifestListV3};
    use crate::spec::{
        Datum, FieldSummary, ManifestContentType, ManifestFile, ManifestList,
        ManifestListWriter, UNASSIGNED_SEQUENCE_NUMBER,
    };

    fn test_key_metadata() -> StandardKeyMetadata {
        StandardKeyMetadata::new(b"0123456789abcdef")
            .with_aad_prefix(b"manifest-list!!")
    }

    #[test]
    fn test_parse_manifest_list_v1() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                    manifest_length: 5806,
                    partition_spec_id: 0,
                    content: ManifestContentType::Data,
                    sequence_number: 0,
                    min_sequence_number: 0,
                    added_snapshot_id: 1646658105718557341,
                    added_files_count: Some(3),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(vec![]),
                    key_metadata: None,
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIO::local();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v1(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .create_file_writer()
                .unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V1)
                .unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[test]
    fn test_parse_manifest_list_v2() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 1,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: None,
                },
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m1.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 2,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1).to_bytes().unwrap()), upper_bound: Some(Datum::float(2.1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: None,
                }
            ]
        };

        let file_io = FileIO::local();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v1.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v2(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .create_file_writer()
                .unwrap(),
            1646658105718557341,
            Some(1646658105718557341),
            1,
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2)
                .unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[test]
    fn test_parse_manifest_list_v3() {
        let manifest_list = ManifestList {
            entries: vec![
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 1,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: Some(10),
                },
                ManifestFile {
                    manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m1.avro".to_string(),
                    manifest_length: 6926,
                    partition_spec_id: 2,
                    content: ManifestContentType::Data,
                    sequence_number: 1,
                    min_sequence_number: 1,
                    added_snapshot_id: 377075049360453639,
                    added_files_count: Some(1),
                    existing_files_count: Some(0),
                    deleted_files_count: Some(0),
                    added_rows_count: Some(3),
                    existing_rows_count: Some(0),
                    deleted_rows_count: Some(0),
                    partitions: Some(
                        vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::float(1.1).to_bytes().unwrap()), upper_bound: Some(Datum::float(2.1).to_bytes().unwrap())}]
                    ),
                    key_metadata: None,
                    first_row_id: Some(13),
                }
            ]
        };

        let file_io = FileIO::local();

        let tmp_dir = TempDir::new().unwrap();
        let file_name = "simple_manifest_list_v3.avro";
        let full_path = format!("{}/{}", tmp_dir.path().to_str().unwrap(), file_name);

        let mut writer = ManifestListWriter::v3(
            file_io
                .new_output(full_path.clone())
                .unwrap()
                .create_file_writer()
                .unwrap(),
            377075049360453639,
            Some(377075049360453639),
            1,
            Some(10),
        );

        writer
            .add_manifests(manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(full_path).expect("read_file must succeed");

        let parsed_manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3)
                .unwrap();

        assert_eq!(manifest_list, parsed_manifest_list);
    }

    #[test]
    fn test_serialize_manifest_list_v1() {
        let manifest_list:ManifestListV1 = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 0,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: None,
                key_metadata: None,
                first_row_id: None,
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro","manifest_length":5806,"partition_spec_id":0,"added_snapshot_id":1646658105718557341,"added_data_files_count":3,"existing_data_files_count":0,"deleted_data_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":null,"key_metadata":null}]"#
        );
    }

    #[test]
    fn test_serialize_manifest_list_v2() {
        let manifest_list:ManifestListV2 = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 377075049360453639,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro","manifest_length":6926,"partition_spec_id":1,"content":0,"sequence_number":1,"min_sequence_number":1,"added_snapshot_id":377075049360453639,"added_files_count":1,"existing_files_count":0,"deleted_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":[{"contains_null":false,"contains_nan":false,"lower_bound":[1,0,0,0,0,0,0,0],"upper_bound":[1,0,0,0,0,0,0,0]}],"key_metadata":null}]"#
        );
    }

    #[test]
    fn test_serialize_manifest_list_v3() {
        let manifest_list: ManifestListV3 = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 1,
                min_sequence_number: 1,
                added_snapshot_id: 377075049360453639,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: Some(10),
            }]
        }.try_into().unwrap();
        let result = serde_json::to_string(&manifest_list).unwrap();
        assert_eq!(
            result,
            r#"[{"manifest_path":"s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro","manifest_length":6926,"partition_spec_id":1,"content":0,"sequence_number":1,"min_sequence_number":1,"added_snapshot_id":377075049360453639,"added_files_count":1,"existing_files_count":0,"deleted_files_count":0,"added_rows_count":3,"existing_rows_count":0,"deleted_rows_count":0,"partitions":[{"contains_null":false,"contains_nan":false,"lower_bound":[1,0,0,0,0,0,0,0],"upper_bound":[1,0,0,0,0,0,0,0]}],"key_metadata":null,"first_row_id":10}]"#
        );
    }

    #[test]
    fn test_manifest_list_writer_v1() {
        let expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}],
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIO::local();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v1(
            output_file.create_file_writer().unwrap(),
            1646658105718557341,
            Some(0),
        );
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V1)
                .unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_manifest_list_writer_v2() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;
        let mut expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIO::local();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v2(
            output_file.create_file_writer().unwrap(),
            snapshot_id,
            Some(0),
            seq_num,
        );
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(path).unwrap();
        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2)
                .unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_manifest_list_writer_with_encrypted_output() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;
        let mut expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: None,
                key_metadata: None,
                first_row_id: None,
            }],
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("encrypted_manifest_list_v2.avro");
        let io = FileIO::local();
        let key_metadata = test_key_metadata();
        let encrypted_output = EncryptedOutputFile::new(
            io.new_output(path.to_str().unwrap()).unwrap(),
            key_metadata.clone(),
        );

        let mut writer = ManifestListWriter::v2(
            encrypted_output.writer().unwrap(),
            snapshot_id,
            Some(0),
            seq_num,
        );
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let encrypted_input = EncryptedInputFile::new(
            io.new_input(path.to_str().unwrap()).unwrap(),
            key_metadata,
        );
        let bs = encrypted_input.read().unwrap();
        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2)
                .unwrap();

        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_manifest_list_writer_v3() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;
        let mut expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: Some(10),
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIO::local();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v3(
            output_file.create_file_writer().unwrap(),
            snapshot_id,
            Some(0),
            seq_num,
            Some(10),
        );
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(path).unwrap();
        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3)
                .unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        expected_manifest_list.entries[0].first_row_id = Some(10);
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_manifest_list_writer_v1_as_v2() {
        let expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIO::local();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v1(
            output_file.create_file_writer().unwrap(),
            1646658105718557341,
            Some(0),
        );
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V2)
                .unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_manifest_list_writer_v1_as_v3() {
        let expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "/opt/bitnami/spark/warehouse/db/table/metadata/10d28031-9739-484c-92db-cdf2975cead4-m0.avro".to_string(),
                manifest_length: 5806,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 1646658105718557341,
                added_files_count: Some(3),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v1.avro");
        let io = FileIO::local();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v1(
            output_file.create_file_writer().unwrap(),
            1646658105718557341,
            Some(0),
        );
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3)
                .unwrap();
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_manifest_list_writer_v2_as_v3() {
        let snapshot_id = 377075049360453639;
        let seq_num = 1;
        let mut expected_manifest_list = ManifestList {
            entries: vec![ManifestFile {
                manifest_path: "s3a://icebergdata/demo/s1/t1/metadata/05ffe08b-810f-49b3-a8f4-e88fc99b254a-m0.avro".to_string(),
                manifest_length: 6926,
                partition_spec_id: 1,
                content: ManifestContentType::Data,
                sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                min_sequence_number: UNASSIGNED_SEQUENCE_NUMBER,
                added_snapshot_id: snapshot_id,
                added_files_count: Some(1),
                existing_files_count: Some(0),
                deleted_files_count: Some(0),
                added_rows_count: Some(3),
                existing_rows_count: Some(0),
                deleted_rows_count: Some(0),
                partitions: Some(
                    vec![FieldSummary { contains_null: false, contains_nan: Some(false), lower_bound: Some(Datum::long(1).to_bytes().unwrap()), upper_bound: Some(Datum::long(1).to_bytes().unwrap())}]
                ),
                key_metadata: None,
                first_row_id: None,
            }]
        };

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("manifest_list_v2.avro");
        let io = FileIO::local();
        let output_file = io.new_output(path.to_str().unwrap()).unwrap();

        let mut writer = ManifestListWriter::v2(
            output_file.create_file_writer().unwrap(),
            snapshot_id,
            Some(0),
            seq_num,
        );
        writer
            .add_manifests(expected_manifest_list.entries.clone().into_iter())
            .unwrap();
        writer.close().unwrap();

        let bs = fs::read(path).unwrap();

        let manifest_list =
            ManifestList::parse_with_version(&bs, crate::spec::FormatVersion::V3)
                .unwrap();
        expected_manifest_list.entries[0].sequence_number = seq_num;
        expected_manifest_list.entries[0].min_sequence_number = seq_num;
        assert_eq!(manifest_list, expected_manifest_list);

        temp_dir.close().unwrap();
    }

    #[test]
    fn test_manifest_list_v2_deserializer_aliases() {
        // reading avro manifest file generated by iceberg 1.4.0
        let avro_1_path = "testdata/manifests_lists/manifest-list-v2-1.avro";
        let bs_1 = fs::read(avro_1_path).unwrap();
        let avro_1_fields = read_avro_schema_fields_as_str(bs_1.clone());
        assert_eq!(
            avro_1_fields,
            "manifest_path, manifest_length, partition_spec_id, content, sequence_number, min_sequence_number, added_snapshot_id, added_data_files_count, existing_data_files_count, deleted_data_files_count, added_rows_count, existing_rows_count, deleted_rows_count, partitions"
        );
        // reading avro manifest file generated by iceberg 1.5.0
        let avro_2_path = "testdata/manifests_lists/manifest-list-v2-2.avro";
        let bs_2 = fs::read(avro_2_path).unwrap();
        let avro_2_fields = read_avro_schema_fields_as_str(bs_2.clone());
        assert_eq!(
            avro_2_fields,
            "manifest_path, manifest_length, partition_spec_id, content, sequence_number, min_sequence_number, added_snapshot_id, added_files_count, existing_files_count, deleted_files_count, added_rows_count, existing_rows_count, deleted_rows_count, partitions"
        );
        // deserializing both files to ManifestList struct
        let _manifest_list_1 =
            ManifestList::parse_with_version(&bs_1, crate::spec::FormatVersion::V2)
                .unwrap();
        let _manifest_list_2 =
            ManifestList::parse_with_version(&bs_2, crate::spec::FormatVersion::V2)
                .unwrap();
    }

    fn read_avro_schema_fields_as_str(bs: Vec<u8>) -> String {
        let reader = Reader::new(&bs[..]).unwrap();
        let schema = reader.writer_schema();
        let fields: String = match schema {
            Schema::Record(record) => record
                .fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<String>>()
                .join(", "),
            _ => "".to_string(),
        };
        fields
    }

    #[test]
    fn test_manifest_content_type_default() {
        assert_eq!(ManifestContentType::default(), ManifestContentType::Data);
    }

    #[test]
    fn test_manifest_content_type_default_value() {
        assert_eq!(ManifestContentType::default() as i32, 0);
    }

    #[test]
    fn test_manifest_file_v1_to_v2_projection() {
        use crate::spec::manifest_list::_serde::ManifestFileV1;

        // Create a V1 manifest file object (without V2 fields)
        let v1_manifest = ManifestFileV1 {
            manifest_path: "/test/manifest.avro".to_string(),
            manifest_length: 5806,
            partition_spec_id: 0,
            added_snapshot_id: 1646658105718557341,
            added_data_files_count: Some(3),
            existing_data_files_count: Some(0),
            deleted_data_files_count: Some(0),
            added_rows_count: Some(3),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
        };

        // Convert V1 to V2 - this should apply defaults for missing V2 fields
        let v2_manifest: ManifestFile = v1_manifest.try_into().unwrap();

        // Verify V1→V2 projection defaults are applied correctly
        assert_eq!(
            v2_manifest.content,
            ManifestContentType::Data,
            "V1 manifest content should default to Data (0)"
        );
        assert_eq!(
            v2_manifest.sequence_number, 0,
            "V1 manifest sequence_number should default to 0"
        );
        assert_eq!(
            v2_manifest.min_sequence_number, 0,
            "V1 manifest min_sequence_number should default to 0"
        );

        // Verify other fields are preserved correctly
        assert_eq!(v2_manifest.manifest_path, "/test/manifest.avro");
        assert_eq!(v2_manifest.manifest_length, 5806);
        assert_eq!(v2_manifest.partition_spec_id, 0);
        assert_eq!(v2_manifest.added_snapshot_id, 1646658105718557341);
        assert_eq!(v2_manifest.added_files_count, Some(3));
        assert_eq!(v2_manifest.existing_files_count, Some(0));
        assert_eq!(v2_manifest.deleted_files_count, Some(0));
        assert_eq!(v2_manifest.added_rows_count, Some(3));
        assert_eq!(v2_manifest.existing_rows_count, Some(0));
        assert_eq!(v2_manifest.deleted_rows_count, Some(0));
        assert_eq!(v2_manifest.partitions, None);
        assert_eq!(v2_manifest.key_metadata, None);
    }
}
