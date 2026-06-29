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

//! Physical Parquet row-read requests.

use std::sync::Arc;

use crate::spec::{
    DEFAULT_SCHEMA_NAME_MAPPING, NameMapping, SchemaRef, TableMetadata,
};
use crate::{Error, ErrorKind, Result};

/// Table-level context shared by physical row reads.
#[derive(Clone, Debug)]
pub struct PhysicalRowReadContext {
    schema: SchemaRef,
    name_mapping: Option<Arc<NameMapping>>,
}

impl PhysicalRowReadContext {
    /// Creates physical-read context from table metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the table's default name mapping is invalid.
    pub fn try_new(table_metadata: &TableMetadata) -> Result<Self> {
        let name_mapping = table_metadata
            .properties()
            .get(DEFAULT_SCHEMA_NAME_MAPPING)
            .map(|raw| {
                serde_json::from_str::<NameMapping>(raw).map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Failed to parse table property {DEFAULT_SCHEMA_NAME_MAPPING} as a NameMapping"
                        ),
                    )
                    .with_source(error)
                })
            })
            .transpose()?
            .map(Arc::new);

        Ok(Self {
            schema: table_metadata.current_schema().clone(),
            name_mapping,
        })
    }

    /// Returns the Iceberg schema used to project physical rows.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Creates a request for one physical row.
    ///
    /// # Errors
    ///
    /// Returns an error if `position` exceeds Iceberg's signed long range.
    pub fn read_row(
        &self,
        data_file_path: impl AsRef<str>,
        position: u64,
        projected_field_ids: Vec<i32>,
    ) -> Result<PhysicalRowReadRequest> {
        let position = i64::try_from(position).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                "row position does not fit Iceberg long metadata column",
            )
        })?;

        Ok(PhysicalRowReadRequest {
            data_file_path: data_file_path.as_ref().to_owned(),
            position,
            schema: Arc::clone(&self.schema),
            projected_field_ids,
            name_mapping: self.name_mapping.as_ref().map(Arc::clone),
        })
    }
}

/// A request for the physical row at one original Parquet file position.
///
/// Physical reads do not apply Iceberg predicates or delete files. They are
/// intended for callers, such as PostgreSQL's `SnapshotAny` tuple fetch, that
/// have already resolved row identity and require the exact stored row.
#[derive(Debug)]
pub struct PhysicalRowReadRequest {
    pub(super) data_file_path: String,
    pub(super) position: i64,
    pub(super) schema: SchemaRef,
    pub(super) projected_field_ids: Vec<i32>,
    pub(super) name_mapping: Option<Arc<NameMapping>>,
}
