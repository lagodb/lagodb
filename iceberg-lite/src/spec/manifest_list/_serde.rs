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

pub use serde_bytes::ByteBuf;
use serde_derive::{Deserialize, Serialize};

use super::ManifestFile;
use crate::Error;
use crate::error::Result;
use crate::spec::FieldSummary;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct ManifestListV3 {
    entries: Vec<ManifestFileV3>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct ManifestListV2 {
    entries: Vec<ManifestFileV2>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct ManifestListV1 {
    entries: Vec<ManifestFileV1>,
}

impl ManifestListV3 {
    /// Converts the [ManifestListV3] into a [ManifestList].
    pub fn try_into(self) -> Result<super::ManifestList> {
        Ok(super::ManifestList {
            entries: self
                .entries
                .into_iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl TryFrom<super::ManifestList> for ManifestListV3 {
    type Error = Error;

    fn try_from(
        value: super::ManifestList,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            entries: value
                .entries
                .into_iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?,
        })
    }
}

impl ManifestListV2 {
    /// Converts the [ManifestListV2] into a [ManifestList].
    pub fn try_into(self) -> Result<super::ManifestList> {
        Ok(super::ManifestList {
            entries: self
                .entries
                .into_iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl TryFrom<super::ManifestList> for ManifestListV2 {
    type Error = Error;

    fn try_from(
        value: super::ManifestList,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            entries: value
                .entries
                .into_iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?,
        })
    }
}

impl ManifestListV1 {
    /// Converts the [ManifestListV1] into a [ManifestList].
    pub fn try_into(self) -> Result<super::ManifestList> {
        Ok(super::ManifestList {
            entries: self
                .entries
                .into_iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl TryFrom<super::ManifestList> for ManifestListV1 {
    type Error = Error;

    fn try_from(
        value: super::ManifestList,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            entries: value
                .entries
                .into_iter()
                .map(|v| v.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ManifestFileV1 {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    pub added_snapshot_id: i64,
    pub added_data_files_count: Option<i32>,
    pub existing_data_files_count: Option<i32>,
    pub deleted_data_files_count: Option<i32>,
    pub added_rows_count: Option<i64>,
    pub existing_rows_count: Option<i64>,
    pub deleted_rows_count: Option<i64>,
    pub partitions: Option<Vec<FieldSummary>>,
    pub key_metadata: Option<ByteBuf>,
}

// Aliases were added to fields that were renamed in Iceberg  1.5.0 (https://github.com/apache/iceberg/pull/5338), in order to support both conventions/versions.
// In the current implementation deserialization is done using field names, and therefore these fields may appear as either.
// see issue that raised this here: https://github.com/apache/iceberg-rust/issues/338
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ManifestFileV2 {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    #[serde(default = "v2_default_content_for_v1")]
    pub content: i32,
    #[serde(default = "v2_default_sequence_number_for_v1")]
    pub sequence_number: i64,
    #[serde(default = "v2_default_min_sequence_number_for_v1")]
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    #[serde(alias = "added_data_files_count", alias = "added_files_count")]
    pub added_files_count: i32,
    #[serde(alias = "existing_data_files_count", alias = "existing_files_count")]
    pub existing_files_count: i32,
    #[serde(alias = "deleted_data_files_count", alias = "deleted_files_count")]
    pub deleted_files_count: i32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
    pub partitions: Option<Vec<FieldSummary>>,
    pub key_metadata: Option<ByteBuf>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ManifestFileV3 {
    pub manifest_path: String,
    pub manifest_length: i64,
    pub partition_spec_id: i32,
    #[serde(default = "v2_default_content_for_v1")]
    pub content: i32,
    #[serde(default = "v2_default_sequence_number_for_v1")]
    pub sequence_number: i64,
    #[serde(default = "v2_default_min_sequence_number_for_v1")]
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    #[serde(alias = "added_data_files_count", alias = "added_files_count")]
    pub added_files_count: i32,
    #[serde(alias = "existing_data_files_count", alias = "existing_files_count")]
    pub existing_files_count: i32,
    #[serde(alias = "deleted_data_files_count", alias = "deleted_files_count")]
    pub deleted_files_count: i32,
    pub added_rows_count: i64,
    pub existing_rows_count: i64,
    pub deleted_rows_count: i64,
    pub partitions: Option<Vec<FieldSummary>>,
    pub key_metadata: Option<ByteBuf>,
    pub first_row_id: Option<u64>,
}

impl ManifestFileV3 {
    /// Converts the [ManifestFileV3] into a [ManifestFile].
    pub fn try_into(self) -> Result<ManifestFile> {
        let manifest_file = ManifestFile {
            manifest_path: self.manifest_path,
            manifest_length: self.manifest_length,
            partition_spec_id: self.partition_spec_id,
            content: self.content.try_into()?,
            sequence_number: self.sequence_number,
            min_sequence_number: self.min_sequence_number,
            added_snapshot_id: self.added_snapshot_id,
            added_files_count: Some(self.added_files_count.try_into()?),
            existing_files_count: Some(self.existing_files_count.try_into()?),
            deleted_files_count: Some(self.deleted_files_count.try_into()?),
            added_rows_count: Some(self.added_rows_count.try_into()?),
            existing_rows_count: Some(self.existing_rows_count.try_into()?),
            deleted_rows_count: Some(self.deleted_rows_count.try_into()?),
            partitions: self.partitions,
            key_metadata: self.key_metadata.map(|b| b.into_vec()),
            first_row_id: self.first_row_id,
        };

        Ok(manifest_file)
    }
}

impl ManifestFileV2 {
    /// Converts the [ManifestFileV2] into a [ManifestFile].
    pub fn try_into(self) -> Result<ManifestFile> {
        Ok(ManifestFile {
            manifest_path: self.manifest_path,
            manifest_length: self.manifest_length,
            partition_spec_id: self.partition_spec_id,
            content: self.content.try_into()?,
            sequence_number: self.sequence_number,
            min_sequence_number: self.min_sequence_number,
            added_snapshot_id: self.added_snapshot_id,
            added_files_count: Some(self.added_files_count.try_into()?),
            existing_files_count: Some(self.existing_files_count.try_into()?),
            deleted_files_count: Some(self.deleted_files_count.try_into()?),
            added_rows_count: Some(self.added_rows_count.try_into()?),
            existing_rows_count: Some(self.existing_rows_count.try_into()?),
            deleted_rows_count: Some(self.deleted_rows_count.try_into()?),
            partitions: self.partitions,
            key_metadata: self.key_metadata.map(|b| b.into_vec()),
            first_row_id: None,
        })
    }
}

fn v2_default_content_for_v1() -> i32 {
    super::ManifestContentType::Data as i32
}

fn v2_default_sequence_number_for_v1() -> i64 {
    0
}

fn v2_default_min_sequence_number_for_v1() -> i64 {
    0
}

impl ManifestFileV1 {
    /// Converts the [ManifestFileV1] into a [ManifestFile].
    pub fn try_into(self) -> Result<ManifestFile> {
        Ok(ManifestFile {
            manifest_path: self.manifest_path,
            manifest_length: self.manifest_length,
            partition_spec_id: self.partition_spec_id,
            added_snapshot_id: self.added_snapshot_id,
            added_files_count: self
                .added_data_files_count
                .map(TryInto::try_into)
                .transpose()?,
            existing_files_count: self
                .existing_data_files_count
                .map(TryInto::try_into)
                .transpose()?,
            deleted_files_count: self
                .deleted_data_files_count
                .map(TryInto::try_into)
                .transpose()?,
            added_rows_count: self
                .added_rows_count
                .map(TryInto::try_into)
                .transpose()?,
            existing_rows_count: self
                .existing_rows_count
                .map(TryInto::try_into)
                .transpose()?,
            deleted_rows_count: self
                .deleted_rows_count
                .map(TryInto::try_into)
                .transpose()?,
            partitions: self.partitions,
            key_metadata: self.key_metadata.map(|b| b.into_vec()),
            // as ref: https://iceberg.apache.org/spec/#partitioning
            // use 0 when reading v1 manifest lists
            content: super::ManifestContentType::Data,
            sequence_number: 0,
            min_sequence_number: 0,
            first_row_id: None,
        })
    }
}

fn convert_to_serde_key_metadata(key_metadata: Option<Vec<u8>>) -> Option<ByteBuf> {
    match key_metadata {
        Some(metadata) if !metadata.is_empty() => Some(ByteBuf::from(metadata)),
        _ => None,
    }
}

impl TryFrom<ManifestFile> for ManifestFileV3 {
    type Error = Error;

    fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
        let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
        Ok(Self {
            manifest_path: value.manifest_path,
            manifest_length: value.manifest_length,
            partition_spec_id: value.partition_spec_id,
            content: value.content as i32,
            sequence_number: value.sequence_number,
            min_sequence_number: value.min_sequence_number,
            added_snapshot_id: value.added_snapshot_id,
            added_files_count: value
                .added_files_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "added_data_files_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            existing_files_count: value
                .existing_files_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "existing_data_files_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            deleted_files_count: value
                .deleted_files_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "deleted_data_files_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            added_rows_count: value
                .added_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "added_rows_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            existing_rows_count: value
                .existing_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "existing_rows_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            deleted_rows_count: value
                .deleted_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "deleted_rows_count in ManifestFileV3 is required",
                    )
                })?
                .try_into()?,
            partitions: value.partitions,
            key_metadata,
            first_row_id: value.first_row_id,
        })
    }
}

impl TryFrom<ManifestFile> for ManifestFileV2 {
    type Error = Error;

    fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
        let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
        Ok(Self {
            manifest_path: value.manifest_path,
            manifest_length: value.manifest_length,
            partition_spec_id: value.partition_spec_id,
            content: value.content as i32,
            sequence_number: value.sequence_number,
            min_sequence_number: value.min_sequence_number,
            added_snapshot_id: value.added_snapshot_id,
            added_files_count: value
                .added_files_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "added_data_files_count in ManifestFileV2 should be require",
                    )
                })?
                .try_into()?,
            existing_files_count: value
                .existing_files_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "existing_data_files_count in ManifestFileV2 should be require",
                    )
                })?
                .try_into()?,
            deleted_files_count: value
                .deleted_files_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "deleted_data_files_count in ManifestFileV2 should be require",
                    )
                })?
                .try_into()?,
            added_rows_count: value
                .added_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "added_rows_count in ManifestFileV2 should be require",
                    )
                })?
                .try_into()?,
            existing_rows_count: value
                .existing_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "existing_rows_count in ManifestFileV2 should be require",
                    )
                })?
                .try_into()?,
            deleted_rows_count: value
                .deleted_rows_count
                .ok_or_else(|| {
                    Error::new(
                        crate::ErrorKind::DataInvalid,
                        "deleted_rows_count in ManifestFileV2 should be require",
                    )
                })?
                .try_into()?,
            partitions: value.partitions,
            key_metadata,
        })
    }
}

impl TryFrom<ManifestFile> for ManifestFileV1 {
    type Error = Error;

    fn try_from(value: ManifestFile) -> std::result::Result<Self, Self::Error> {
        let key_metadata = convert_to_serde_key_metadata(value.key_metadata);
        Ok(Self {
            manifest_path: value.manifest_path,
            manifest_length: value.manifest_length,
            partition_spec_id: value.partition_spec_id,
            added_snapshot_id: value.added_snapshot_id,
            added_data_files_count: value
                .added_files_count
                .map(TryInto::try_into)
                .transpose()?,
            existing_data_files_count: value
                .existing_files_count
                .map(TryInto::try_into)
                .transpose()?,
            deleted_data_files_count: value
                .deleted_files_count
                .map(TryInto::try_into)
                .transpose()?,
            added_rows_count: value
                .added_rows_count
                .map(TryInto::try_into)
                .transpose()?,
            existing_rows_count: value
                .existing_rows_count
                .map(TryInto::try_into)
                .transpose()?,
            deleted_rows_count: value
                .deleted_rows_count
                .map(TryInto::try_into)
                .transpose()?,
            partitions: value.partitions,
            key_metadata,
        })
    }
}
