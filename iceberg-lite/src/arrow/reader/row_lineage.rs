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

//! Resolution of physically stored Iceberg v3 row-lineage columns.

use std::collections::HashMap;

use arrow_schema::SchemaRef;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};

use super::projection::build_field_id_map;
use crate::metadata_columns::{
    RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_FIELD_ID_ROW_ID,
};
use crate::{Error, ErrorKind, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhysicalLineageSource {
    Absent,
    Embedded { leaf_index: usize },
    UnresolvableById,
}

impl PhysicalLineageSource {
    fn resolve(
        field_id_map: Option<&HashMap<i32, usize>>,
        present_by_id: bool,
        present_by_name: bool,
        field_id: i32,
    ) -> Self {
        if let Some(leaf_index) =
            field_id_map.and_then(|field_ids| field_ids.get(&field_id).copied())
        {
            return Self::Embedded { leaf_index };
        }

        if present_by_id || present_by_name {
            Self::UnresolvableById
        } else {
            Self::Absent
        }
    }

    fn append_projected_leaf(
        self,
        project: bool,
        field_name: &str,
        data_file_path: &str,
        leaf_indices: &mut Vec<usize>,
    ) -> Result<()> {
        if !project {
            return Ok(());
        }

        match self {
            Self::Absent => Ok(()),
            Self::Embedded { leaf_index } => {
                leaf_indices.push(leaf_index);
                Ok(())
            }
            Self::UnresolvableById => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "Reading physically stored {field_name} from data file \
                     {data_file_path} without an embedded field ID is not supported"
                ),
            )),
        }
    }
}

/// One-time resolution of physical lineage sources from the original file schema.
///
/// The resolution is deliberately performed before name mapping or fallback IDs are
/// applied. A physical metadata column is readable only when the complete Parquet leaf
/// schema has embedded IDs and its reserved ID resolves to a leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RowLineageResolution {
    row_id: PhysicalLineageSource,
    last_updated_sequence_number: PhysicalLineageSource,
}

impl RowLineageResolution {
    pub(super) fn try_new(
        parquet_schema: &SchemaDescriptor,
        original_arrow_schema: &SchemaRef,
        data_file_path: &str,
    ) -> Result<Self> {
        let field_id_map = build_field_id_map(parquet_schema)?;
        let field_id_map = field_id_map.as_ref();
        let mut row_id_present_by_id = false;
        let mut row_id_present_by_name = false;
        let mut last_updated_present_by_id = false;
        let mut last_updated_present_by_name = false;
        for field in original_arrow_schema.fields() {
            row_id_present_by_name |= field.name() == RESERVED_COL_NAME_ROW_ID;
            last_updated_present_by_name |=
                field.name() == RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER;
            if let Some(value) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
                let field_id = value.parse::<i32>().map_err(|source| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Data file {data_file_path} contains an invalid Parquet field ID \
                             {value:?} on column {}",
                            field.name()
                        ),
                    )
                    .with_source(source)
                })?;
                row_id_present_by_id |= field_id == RESERVED_FIELD_ID_ROW_ID;
                last_updated_present_by_id |=
                    field_id == RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER;
            }
        }
        for column in parquet_schema.columns() {
            if let ParquetType::PrimitiveType { basic_info, .. } = column.self_type()
                && basic_info.has_id()
            {
                row_id_present_by_id |= basic_info.id() == RESERVED_FIELD_ID_ROW_ID;
                last_updated_present_by_id |=
                    basic_info.id() == RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER;
            }
        }

        Ok(Self {
            row_id: PhysicalLineageSource::resolve(
                field_id_map,
                row_id_present_by_id,
                row_id_present_by_name,
                RESERVED_FIELD_ID_ROW_ID,
            ),
            last_updated_sequence_number: PhysicalLineageSource::resolve(
                field_id_map,
                last_updated_present_by_id,
                last_updated_present_by_name,
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            ),
        })
    }

    pub(super) fn projection_mask(
        self,
        parquet_schema: &SchemaDescriptor,
        project_row_id: bool,
        project_last_updated_sequence_number: bool,
        data_file_path: &str,
    ) -> Result<Option<ProjectionMask>> {
        let mut leaf_indices = Vec::with_capacity(2);
        self.row_id.append_projected_leaf(
            project_row_id,
            RESERVED_COL_NAME_ROW_ID,
            data_file_path,
            &mut leaf_indices,
        )?;
        self.last_updated_sequence_number.append_projected_leaf(
            project_last_updated_sequence_number,
            RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
            data_file_path,
            &mut leaf_indices,
        )?;

        if leaf_indices.is_empty() {
            Ok(None)
        } else {
            Ok(Some(ProjectionMask::leaves(parquet_schema, leaf_indices)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use parquet::schema::parser::parse_message_type;

    use super::*;

    fn parquet_schema(message: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    fn field_with_id(name: &str, field_id: i32) -> Field {
        Field::new(name, DataType::Int64, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            field_id.to_string(),
        )]))
    }

    #[test]
    fn resolves_both_embedded_lineage_leaves() {
        let parquet_schema = parquet_schema(&format!(
            "message schema {{
                required int32 id = 1;
                optional int64 {RESERVED_COL_NAME_ROW_ID} = {RESERVED_FIELD_ID_ROW_ID};
                optional int64 {RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER} = \
                    {RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER};
            }}"
        ));
        let arrow_schema = Arc::new(Schema::new(vec![
            field_with_id("id", 1),
            field_with_id(RESERVED_COL_NAME_ROW_ID, RESERVED_FIELD_ID_ROW_ID),
            field_with_id(
                RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
                RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
            ),
        ]));

        let resolution = RowLineageResolution::try_new(
            &parquet_schema,
            &arrow_schema,
            "data.parquet",
        )
        .unwrap();

        assert_eq!(
            resolution.row_id,
            PhysicalLineageSource::Embedded { leaf_index: 1 }
        );
        assert_eq!(
            resolution.last_updated_sequence_number,
            PhysicalLineageSource::Embedded { leaf_index: 2 }
        );
    }

    #[test]
    fn rejects_lineage_when_the_leaf_id_map_is_incomplete() {
        let parquet_schema = parquet_schema(&format!(
            "message schema {{
                required int32 id;
                optional int64 {RESERVED_COL_NAME_ROW_ID} = {RESERVED_FIELD_ID_ROW_ID};
            }}"
        ));
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            field_with_id(RESERVED_COL_NAME_ROW_ID, RESERVED_FIELD_ID_ROW_ID),
        ]));
        let resolution = RowLineageResolution::try_new(
            &parquet_schema,
            &arrow_schema,
            "mixed-ids.parquet",
        )
        .unwrap();

        let error = resolution
            .projection_mask(&parquet_schema, true, false, "mixed-ids.parquet")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(error.to_string().contains("mixed-ids.parquet"));
    }

    #[test]
    fn rejects_name_only_lineage_without_silently_overwriting_it() {
        let parquet_schema = parquet_schema(&format!(
            "message schema {{
                required int32 id;
                optional int64 {RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER};
            }}"
        ));
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER,
                DataType::Int64,
                true,
            ),
        ]));
        let resolution = RowLineageResolution::try_new(
            &parquet_schema,
            &arrow_schema,
            "name-only.parquet",
        )
        .unwrap();

        let error = resolution
            .projection_mask(&parquet_schema, false, true, "name-only.parquet")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert!(error.to_string().contains("name-only.parquet"));
    }

    #[test]
    fn malformed_arrow_field_id_is_invalid_data() {
        let parquet_schema = parquet_schema(
            "message schema { required int32 id; optional int64 _row_id; }",
        );
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(RESERVED_COL_NAME_ROW_ID, DataType::Int64, true)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_owned(),
                    "not-an-i32".to_owned(),
                )])),
        ]));

        let error = RowLineageResolution::try_new(
            &parquet_schema,
            &arrow_schema,
            "malformed.parquet",
        )
        .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.to_string().contains("malformed.parquet"));
    }
}
