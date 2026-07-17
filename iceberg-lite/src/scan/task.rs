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

use serde::{Deserialize, Serialize, Serializer};
use typed_builder::TypedBuilder;

use crate::expr::BoundPredicate;
use crate::spec::{
    DataContentType, DataFileFormat, ManifestEntryRef, NameMapping, PartitionSpec,
    Schema, SchemaRef, Struct,
};

/// Serialization helper that always returns NotImplementedError.
/// Used for fields that should not be serialized but we want to be explicit about it.
fn serialize_not_implemented<S, T>(
    _: &T,
    _: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    Err(serde::ser::Error::custom(
        "Serialization not implemented for this field",
    ))
}

/// Deserialization helper that always returns NotImplementedError.
/// Used for fields that should not be deserialized but we want to be explicit about it.
fn deserialize_not_implemented<'de, D, T>(_: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Err(serde::de::Error::custom(
        "Deserialization not implemented for this field",
    ))
}

fn default_delete_file_format() -> DataFileFormat {
    DataFileFormat::Parquet
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct FileScanTask {
    /// The total size of the data file in bytes, from the manifest entry.
    /// Used to avoid an additional metadata lookup when reading Parquet footers.
    pub file_size_in_bytes: u64,
    /// The start offset of the file to scan.
    pub start: u64,
    /// The length of the file to scan.
    pub length: u64,
    /// The number of records in the file to scan.
    ///
    /// This is an optional field, and only available if we are
    /// reading the entire data file.
    #[builder(default)]
    pub record_count: Option<u64>,

    /// Effective first row id for this data file in format v3 tables.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub first_row_id: Option<u64>,

    /// Data sequence inherited by rows when the v3 last-updated lineage field
    /// is not physically present in the source file.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub last_updated_sequence_number: Option<i64>,

    /// The data file path corresponding to the task.
    pub data_file_path: String,

    /// The format of the file to scan.
    pub data_file_format: DataFileFormat,

    /// ID of the partition spec used to encode this data file's partition.
    #[serde(default)]
    #[builder(default)]
    pub partition_spec_id: i32,

    /// The schema of the file to scan.
    pub schema: SchemaRef,
    /// The field ids to project.
    pub project_field_ids: Vec<i32>,
    /// The predicate to filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub predicate: Option<BoundPredicate>,

    /// The list of delete files that may need to be applied to this data file
    #[builder(default)]
    pub deletes: Vec<FileScanTaskDeleteFile>,

    /// Partition data from the manifest entry, used to identify which columns can use
    /// constant values from partition metadata vs. reading from the data file.
    /// Per the Iceberg spec, only identity-transformed partition fields should use constants.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    pub partition: Option<Struct>,

    /// The partition spec for this file, used to distinguish identity transforms
    /// (which use partition metadata constants) from non-identity transforms like
    /// bucket/truncate (which must read source columns from the data file).
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    pub partition_spec: Option<Arc<PartitionSpec>>,

    /// Name mapping from table metadata (property: schema.name-mapping.default),
    /// used to resolve field IDs from column names when Parquet files lack field IDs
    /// or have field ID conflicts.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_not_implemented")]
    #[serde(deserialize_with = "deserialize_not_implemented")]
    #[builder(default)]
    pub name_mapping: Option<Arc<NameMapping>>,

    /// Whether this scan task should treat column names as case-sensitive when binding predicates.
    pub case_sensitive: bool,
}

impl FileScanTask {
    /// Returns the data file path of this file scan task.
    pub fn data_file_path(&self) -> &str {
        &self.data_file_path
    }

    /// Returns the project field id of this file scan task.
    pub fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }

    /// Returns the predicate of this file scan task.
    pub fn predicate(&self) -> Option<&BoundPredicate> {
        self.predicate.as_ref()
    }

    /// Replace the predicate used by the reader for this task.
    ///
    /// File planning and row filtering can intentionally use different
    /// predicates: planning needs a stable pruning predicate, while repeated
    /// reads of the same planned task list may need the current runtime row
    /// predicate.
    pub fn set_predicate(&mut self, predicate: Option<BoundPredicate>) {
        self.predicate = predicate;
    }

    /// Returns the schema of this file scan task as a reference
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns the schema of this file scan task as a SchemaRef
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the partition spec id for this file scan task.
    pub fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }
}

#[derive(Debug)]
pub(crate) struct DeleteFileContext {
    pub(crate) manifest_entry: ManifestEntryRef,
    pub(crate) partition_spec_id: i32,
}

impl From<&DeleteFileContext> for FileScanTaskDeleteFile {
    fn from(ctx: &DeleteFileContext) -> Self {
        let data_file = &ctx.manifest_entry.data_file;
        let referenced_data_file = if data_file.is_deletion_vector() {
            data_file.referenced_data_file_path()
        } else {
            data_file.position_delete_target_data_file_path()
        };
        FileScanTaskDeleteFile {
            file_path: ctx.manifest_entry.file_path().to_string(),
            file_size_in_bytes: ctx.manifest_entry.file_size_in_bytes(),
            file_type: ctx.manifest_entry.content_type(),
            file_format: ctx.manifest_entry.file_format(),
            partition_spec_id: ctx.partition_spec_id,
            equality_ids: ctx.manifest_entry.data_file.equality_ids.clone(),
            referenced_data_file: referenced_data_file.map(str::to_owned),
            content_offset: data_file.content_offset(),
            content_size_in_bytes: data_file.content_size_in_bytes(),
            record_count: data_file.record_count(),
        }
    }
}

/// A task to scan part of file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct FileScanTaskDeleteFile {
    /// The delete file path
    pub file_path: String,

    /// The total size of the delete file in bytes, from the manifest entry.
    pub file_size_in_bytes: u64,

    /// delete file type
    pub file_type: DataContentType,

    /// delete file format
    #[serde(default = "default_delete_file_format")]
    #[builder(default = DataFileFormat::Parquet)]
    pub file_format: DataFileFormat,

    /// partition id
    pub partition_spec_id: i32,

    /// equality ids for equality deletes (null for anything other than equality-deletes)
    #[serde(default)]
    #[builder(default)]
    pub equality_ids: Option<Vec<i32>>,

    /// Effective data file path referenced by file-scoped position deletes or
    /// deletion vectors.
    ///
    /// For legacy Parquet position deletes this may be inferred from
    /// `file_path` lower/upper bounds in the delete manifest entry, not only
    /// from the manifest's explicit `referenced_data_file` field.
    #[serde(default)]
    #[builder(default)]
    pub referenced_data_file: Option<String>,

    /// Offset of a referenced Puffin blob for deletion vectors.
    #[serde(default)]
    #[builder(default)]
    pub content_offset: Option<i64>,

    /// Size of a referenced Puffin blob for deletion vectors.
    #[serde(default)]
    #[builder(default)]
    pub content_size_in_bytes: Option<i64>,

    /// Delete record count, or deletion-vector cardinality for Puffin DVs.
    #[serde(default)]
    #[builder(default)]
    pub record_count: u64,
}

impl FileScanTaskDeleteFile {
    /// Returns true when this scan task delete file is a position delete file.
    pub fn is_position_delete(&self) -> bool {
        self.file_type == DataContentType::PositionDeletes
    }

    /// Returns true when this scan task delete file is an Iceberg deletion
    /// vector.
    pub fn is_deletion_vector(&self) -> bool {
        self.is_position_delete() && self.file_format == DataFileFormat::Puffin
    }

    /// Returns the effective target data file path for a file-scoped position
    /// delete or deletion vector.
    pub fn referenced_data_file_path(&self) -> Option<&str> {
        self.referenced_data_file.as_deref()
    }

    /// Returns true when this is a legacy position delete that may contain
    /// deletes for more than one data file.
    pub fn is_broad_scoped_position_delete(&self) -> bool {
        self.is_position_delete()
            && !self.is_deletion_vector()
            && self.referenced_data_file.is_none()
    }

    /// Returns true when a DV rewrite that has merged this effective
    /// file-scoped delete can remove the old delete file from table metadata.
    pub fn can_remove_after_dv_rewrite(&self) -> bool {
        self.is_deletion_vector()
            || (self.is_position_delete() && self.referenced_data_file.is_some())
    }
}
