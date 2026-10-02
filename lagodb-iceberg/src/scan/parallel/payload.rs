//! Binary Iceberg worker inventory.
//!
//! File-level data is stored once in the DSM-backed byte image, byte ranges are
//! fixed-size records, and groups contain only range identifiers. Worker entry
//! validates the section layout and decodes shared task metadata; file records
//! become owned `FileScanTask`s only when that worker claims their group.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use bincode::Options;
use iceberg_lite::expr::BoundPredicate;
use iceberg_lite::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg_lite::spec::{
    DataFileFormat, Literal, NameMapping, PartitionSpec, PrimitiveLiteral,
    SchemaRef as IcebergSchemaRef, Struct, StructType,
};
use serde::{Deserialize, Serialize};

use super::super::ScanError;
use super::grouping::{GroupedTaskRanges, TaskRange};
use crate::error::IcebergError;

mod flat;
mod metadata;

pub(super) use flat::DecodedTaskInventory;
use metadata::WorkerTaskMetadataCodec;

pub(super) struct WorkerSourcePayload;

#[derive(PartialEq, Serialize, Deserialize)]
struct WorkerFileRecord {
    metadata_id: u32,
    file_size_in_bytes: u64,
    first_row_id: Option<u64>,
    data_sequence_number: Option<i64>,
    data_file_path: String,
    data_file_format: DataFileFormat,
    partition_spec_id: i32,
    key_metadata: Option<Box<[u8]>>,
    deletes: Vec<FileScanTaskDeleteFile>,
    partition: Option<Vec<Option<WorkerPartitionValue>>>,
}

// Iceberg metadata uses self-describing serde representations such as
// `flatten`, `untagged`, and `deserialize_any`. Its codec must also preserve
// byte strings because decimal, binary, fixed, and UUID literals use them.
#[derive(Serialize, Deserialize)]
struct WorkerTaskMetadata {
    schema: IcebergSchemaRef,
    project_field_ids: Vec<i32>,
    predicate: Option<BoundPredicate>,
    partition_spec: Option<Arc<PartitionSpec>>,
    unified_partition_type: Option<Arc<StructType>>,
    name_mapping: Option<Arc<NameMapping>>,
    case_sensitive: bool,
}

impl WorkerTaskMetadata {
    fn from_task(task: &FileScanTask) -> Self {
        Self {
            schema: Arc::clone(&task.schema),
            project_field_ids: task.project_field_ids.clone(),
            predicate: task.predicate.clone(),
            partition_spec: task.partition_spec.as_ref().map(Arc::clone),
            unified_partition_type: task
                .unified_partition_type
                .as_ref()
                .map(Arc::clone),
            name_mapping: task.name_mapping.as_ref().map(Arc::clone),
            case_sensitive: task.case_sensitive,
        }
    }

    fn matches(&self, task: &FileScanTask) -> bool {
        Arc::ptr_eq(&self.schema, &task.schema)
            && self.project_field_ids == task.project_field_ids
            && self.predicate == task.predicate
            && self.partition_spec.as_ref().map(Arc::as_ptr)
                == task.partition_spec.as_ref().map(Arc::as_ptr)
            && self.unified_partition_type.as_ref().map(Arc::as_ptr)
                == task.unified_partition_type.as_ref().map(Arc::as_ptr)
            && self.name_mapping.as_ref().map(Arc::as_ptr)
                == task.name_mapping.as_ref().map(Arc::as_ptr)
            && self.case_sensitive == task.case_sensitive
    }
}

impl WorkerSourcePayload {
    pub(super) fn encode(
        prefix: &[u8],
        schema: &SchemaRef,
        tasks: &[FileScanTask],
        mut grouped: GroupedTaskRanges,
    ) -> Result<Box<[u8]>, ScanError> {
        let mut metadata = Vec::<WorkerTaskMetadata>::new();
        let mut files = Vec::<WorkerFileRecord>::new();
        let mut file_ids = HashMap::<&str, u32>::new();
        let mut task_file_ids = Vec::with_capacity(tasks.len());
        for task in tasks {
            let metadata_id =
                match metadata.iter().position(|entry| entry.matches(task)) {
                    Some(index) => index,
                    None => {
                        let index = metadata.len();
                        metadata.push(WorkerTaskMetadata::from_task(task));
                        index
                    }
                };
            let metadata_id = u32::try_from(metadata_id).map_err(|_| {
                ScanError::WorkerPayload(
                    "Iceberg metadata inventory exceeds u32".to_owned(),
                )
            })?;
            let record = WorkerFileRecord::encode(task, metadata_id)?;
            let file_id = match file_ids.get(task.data_file_path.as_str()) {
                Some(file_id) => {
                    if files[*file_id as usize] != record {
                        return Err(ScanError::WorkerPayload(format!(
                            "data file {} has inconsistent file-level task metadata",
                            task.data_file_path,
                        )));
                    }
                    *file_id
                }
                None => {
                    let file_id = u32::try_from(files.len()).map_err(|_| {
                        ScanError::WorkerPayload(
                            "Iceberg file inventory exceeds u32".to_owned(),
                        )
                    })?;
                    files.push(record);
                    file_ids.insert(task.data_file_path.as_str(), file_id);
                    file_id
                }
            };
            task_file_ids.push(file_id);
        }
        for range in grouped.ranges.iter_mut() {
            range.file_id =
                *task_file_ids.get(range.file_id as usize).ok_or_else(|| {
                    ScanError::WorkerPayload(
                        "grouping references a missing source task".to_owned(),
                    )
                })?;
        }
        flat::FlatPayloadEncoder::encode(
            prefix,
            schema,
            &metadata,
            &files,
            &grouped.ranges,
            &grouped.groups,
        )
    }

    pub(super) unsafe fn decode_shared(
        data: &[u8],
    ) -> Result<DecodedTaskInventory, ScanError> {
        unsafe { DecodedTaskInventory::decode_shared(data) }
    }
}

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

fn codec_error(error: bincode::Error) -> ScanError {
    ScanError::from(IcebergError::from(error))
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
enum WorkerPartitionValue {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    String(String),
    Binary(Vec<u8>),
    Int128(i128),
    UInt128(u128),
    AboveMax,
    BelowMin,
}

impl WorkerPartitionValue {
    fn encode(value: &Literal) -> Result<Self, ScanError> {
        let Literal::Primitive(value) = value else {
            return Err(ScanError::WorkerPayload(
                "partition data contains a non-primitive value".to_owned(),
            ));
        };
        Ok(match value {
            PrimitiveLiteral::Boolean(value) => Self::Boolean(*value),
            PrimitiveLiteral::Int(value) => Self::Int(*value),
            PrimitiveLiteral::Long(value) => Self::Long(*value),
            PrimitiveLiteral::Float(value) => {
                Self::Float(value.into_inner().to_bits())
            }
            PrimitiveLiteral::Double(value) => {
                Self::Double(value.into_inner().to_bits())
            }
            PrimitiveLiteral::String(value) => Self::String(value.clone()),
            PrimitiveLiteral::Binary(value) => Self::Binary(value.clone()),
            PrimitiveLiteral::Int128(value) => Self::Int128(*value),
            PrimitiveLiteral::UInt128(value) => Self::UInt128(*value),
            PrimitiveLiteral::AboveMax => Self::AboveMax,
            PrimitiveLiteral::BelowMin => Self::BelowMin,
        })
    }

    fn into_literal(self) -> Literal {
        Literal::Primitive(match self {
            Self::Boolean(value) => PrimitiveLiteral::Boolean(value),
            Self::Int(value) => PrimitiveLiteral::Int(value),
            Self::Long(value) => PrimitiveLiteral::Long(value),
            Self::Float(bits) => PrimitiveLiteral::Float(f32::from_bits(bits).into()),
            Self::Double(bits) => {
                PrimitiveLiteral::Double(f64::from_bits(bits).into())
            }
            Self::String(value) => PrimitiveLiteral::String(value),
            Self::Binary(value) => PrimitiveLiteral::Binary(value),
            Self::Int128(value) => PrimitiveLiteral::Int128(value),
            Self::UInt128(value) => PrimitiveLiteral::UInt128(value),
            Self::AboveMax => PrimitiveLiteral::AboveMax,
            Self::BelowMin => PrimitiveLiteral::BelowMin,
        })
    }
}

impl WorkerFileRecord {
    fn encode(task: &FileScanTask, metadata_id: u32) -> Result<Self, ScanError> {
        let partition = task
            .partition
            .as_ref()
            .map(|partition| {
                partition
                    .iter()
                    .map(|value| value.map(WorkerPartitionValue::encode).transpose())
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        Ok(Self {
            metadata_id,
            file_size_in_bytes: task.file_size_in_bytes,
            first_row_id: task.first_row_id,
            data_sequence_number: task.data_sequence_number,
            data_file_path: task.data_file_path.clone(),
            data_file_format: task.data_file_format,
            partition_spec_id: task.partition_spec_id,
            key_metadata: task.key_metadata.clone(),
            deletes: task.deletes.clone(),
            partition,
        })
    }

    fn into_task(
        self,
        range: TaskRange,
        metadata: &WorkerTaskMetadata,
    ) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: self.file_size_in_bytes,
            start: range.start,
            length: range.length,
            record_count: range.record_count,
            first_row_id: self.first_row_id,
            data_sequence_number: self.data_sequence_number,
            data_file_path: self.data_file_path,
            data_file_format: self.data_file_format,
            partition_spec_id: self.partition_spec_id,
            schema: Arc::clone(&metadata.schema),
            project_field_ids: metadata.project_field_ids.clone(),
            predicate: metadata.predicate.clone(),
            deletes: self.deletes,
            partition: self.partition.map(|values| {
                values
                    .into_iter()
                    .map(|value| value.map(WorkerPartitionValue::into_literal))
                    .collect::<Struct>()
            }),
            partition_spec: metadata.partition_spec.as_ref().map(Arc::clone),
            unified_partition_type: metadata
                .unified_partition_type
                .as_ref()
                .map(Arc::clone),
            // Sort metadata is not consumed by the worker reader. Keep it out
            // of DSM until execution has semantics that require it.
            sort_order_id: None,
            sort_order: None,
            name_mapping: metadata.name_mapping.as_ref().map(Arc::clone),
            case_sensitive: metadata.case_sensitive,
            key_metadata: self.key_metadata,
        }
    }
}
