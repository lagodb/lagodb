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

//! Predicate-driven row filtering for `ArrowReader`: constructing Arrow
//! `RowFilter`s from Iceberg predicates, row-group selection based on column
//! statistics, row-selection via the Parquet page index, and byte-range
//! row-group filtering used for file splitting.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::Schema as ArrowSchema;
use arrow_select::filter::filter_record_batch;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelection};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;

use super::predicate_visitor::PredicateResult;
use super::{ArrowReader, PredicateConverter};
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::expr::visitors::page_index_evaluator::PageIndexEvaluator;
use crate::expr::visitors::row_group_metrics_evaluator::RowGroupMetricsEvaluator;
use crate::spec::Schema;
use crate::{Error, ErrorKind};

pub(super) struct TransformedRecordBatchFilter {
    predicate: BoundPredicate,
    predicate_fn: Option<Box<PredicateResult>>,
}

impl TransformedRecordBatchFilter {
    pub(super) fn new(predicate: BoundPredicate) -> Self {
        Self {
            predicate,
            predicate_fn: None,
        }
    }

    pub(super) fn filter(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.predicate_fn.is_none() {
            let field_id_to_batch_index =
                ArrowReader::build_record_batch_field_id_map(batch.schema_ref())?;
            let mut converter =
                PredicateConverter::for_record_batch(&field_id_to_batch_index);
            self.predicate_fn = Some(visit(&mut converter, &self.predicate)?);
        }

        let Some(predicate_fn) = self.predicate_fn.as_mut() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "transformed record batch predicate was not initialized",
            ));
        };
        let filter = predicate_fn(batch.clone()).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to evaluate predicate against transformed record batch",
            )
            .with_source(err)
        })?;

        Ok(filter_record_batch(&batch, &filter).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to filter transformed record batch",
            )
            .with_source(err)
        })?)
    }
}

impl ArrowReader {
    pub(super) fn get_row_filter(
        predicates: &BoundPredicate,
        parquet_schema: &SchemaDescriptor,
        iceberg_field_ids: &HashSet<i32>,
        field_id_map: &HashMap<i32, usize>,
    ) -> Result<RowFilter> {
        // Collect Parquet column indices from field ids.
        // If the field id is not found in Parquet schema, it will be ignored due to schema evolution.
        let mut column_indices = iceberg_field_ids
            .iter()
            .filter_map(|field_id| field_id_map.get(field_id).cloned())
            .collect::<Vec<_>>();
        column_indices.sort();

        // The converter that converts `BoundPredicates` to `ArrowPredicates`
        let mut converter = PredicateConverter::for_parquet(
            parquet_schema,
            field_id_map,
            &column_indices,
        );

        // After collecting required leaf column indices used in the predicate,
        // creates the projection mask for the Arrow predicates.
        let projection_mask =
            ProjectionMask::leaves(parquet_schema, column_indices.clone());
        let predicate_func = visit(&mut converter, predicates)?;
        let arrow_predicate = ArrowPredicateFn::new(projection_mask, predicate_func);
        Ok(RowFilter::new(vec![Box::new(arrow_predicate)]))
    }

    pub(crate) fn project_record_batch_by_field_ids(
        batch: RecordBatch,
        projected_field_ids: &[i32],
    ) -> Result<RecordBatch> {
        let schema = batch.schema();
        let field_id_to_batch_index = Self::build_record_batch_field_id_map(&schema)?;

        if batch.num_columns() == projected_field_ids.len() {
            let already_projected =
                projected_field_ids
                    .iter()
                    .enumerate()
                    .all(|(idx, field_id)| {
                        field_id_to_batch_index
                            .get(field_id)
                            .is_some_and(|batch_idx| *batch_idx == idx)
                    });
            if already_projected {
                return Ok(batch);
            }
        }

        let mut fields = Vec::with_capacity(projected_field_ids.len());
        let mut columns = Vec::with_capacity(projected_field_ids.len());

        for field_id in projected_field_ids {
            let Some(batch_idx) = field_id_to_batch_index.get(field_id).copied()
            else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "field id {field_id} not found in transformed record batch"
                    ),
                ));
            };
            fields.push(schema.fields()[batch_idx].clone());
            columns.push(batch.column(batch_idx).clone());
        }

        let options = RecordBatchOptions::default()
            .with_match_field_names(false)
            .with_row_count(Some(batch.num_rows()));
        Ok(RecordBatch::try_new_with_options(
            Arc::new(ArrowSchema::new(fields)),
            columns,
            &options,
        )?)
    }

    fn build_record_batch_field_id_map(
        schema: &Arc<ArrowSchema>,
    ) -> Result<HashMap<i32, usize>> {
        let mut field_id_to_batch_index = HashMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            if let Some(field_id) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
                let field_id = field_id.parse().map_err(|err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("field id not parseable as an i32: {err}"),
                    )
                })?;
                field_id_to_batch_index.insert(field_id, idx);
            }
        }
        Ok(field_id_to_batch_index)
    }

    pub(super) fn get_selected_row_group_indices(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<Vec<usize>> {
        let row_groups_metadata = parquet_metadata.row_groups();
        let mut results = Vec::with_capacity(row_groups_metadata.len());

        for (idx, row_group_metadata) in row_groups_metadata.iter().enumerate() {
            if RowGroupMetricsEvaluator::eval(
                predicate,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )? {
                results.push(idx);
            }
        }

        Ok(results)
    }

    pub(super) fn get_row_selection_for_filter_predicate(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        selected_row_groups: &Option<Vec<usize>>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<Option<RowSelection>> {
        let (Some(column_index), Some(offset_index)) = (
            parquet_metadata.column_index(),
            parquet_metadata.offset_index(),
        ) else {
            return Ok(None);
        };
        if column_index.len() != parquet_metadata.num_row_groups()
            || offset_index.len() != parquet_metadata.num_row_groups()
        {
            return Ok(None);
        }

        // If all row groups were filtered out, return an empty RowSelection (select no rows)
        if let Some(selected_row_groups) = selected_row_groups {
            if selected_row_groups.is_empty() {
                return Ok(Some(RowSelection::from(Vec::new())));
            }
        }

        let mut selected_row_groups_idx = 0;

        let page_index = column_index
            .iter()
            .enumerate()
            .zip(offset_index)
            .zip(parquet_metadata.row_groups());

        let mut results = Vec::new();
        for (((idx, column_index), offset_index), row_group_metadata) in page_index {
            if let Some(selected_row_groups) = selected_row_groups {
                // skip row groups that aren't present in selected_row_groups
                if idx == selected_row_groups[selected_row_groups_idx] {
                    selected_row_groups_idx += 1;
                } else {
                    continue;
                }
            }

            let selections_for_page = PageIndexEvaluator::eval(
                predicate,
                column_index,
                offset_index,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )?;

            results.push(selections_for_page);

            if let Some(selected_row_groups) = selected_row_groups {
                if selected_row_groups_idx == selected_row_groups.len() {
                    break;
                }
            }
        }

        Ok(Some(
            results.into_iter().flatten().collect::<Vec<_>>().into(),
        ))
    }

    /// Filters row groups by byte range to support Iceberg's file splitting.
    ///
    /// Engines split a data file into scan tasks, each covering `[start, start+length)`.
    /// A row group must be read by exactly one task, otherwise rows are duplicated
    /// when externally planned ranges bisect row groups. Assign ownership by the row
    /// group's midpoint, matching parquet-mr's `BlockMetaData` midpoint semantics.
    pub(super) fn filter_row_groups_by_byte_range(
        parquet_metadata: &Arc<ParquetMetaData>,
        start: u64,
        length: u64,
    ) -> Result<Vec<usize>> {
        let row_groups = parquet_metadata.row_groups();
        let mut selected = Vec::new();
        let end = start + length;

        // Row groups are stored sequentially after the 4-byte magic header.
        let mut current_byte_offset = 4u64;

        for (idx, row_group) in row_groups.iter().enumerate() {
            let row_group_size = row_group.compressed_size() as u64;
            let row_group_midpoint = current_byte_offset + row_group_size / 2;

            if start <= row_group_midpoint && row_group_midpoint < end {
                selected.push(idx);
            }

            current_byte_offset += row_group_size;
        }

        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        ArrayRef, Int32Array, LargeStringArray, RecordBatch, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::expr::accessor::StructAccessor;
    use crate::expr::{
        BinaryExpression, Bind, BoundPredicate, BoundReference, Predicate,
        PredicateOperator, Reference,
    };
    use crate::io::FileIO;
    use crate::metadata_columns::{RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_POS};
    use crate::scan::FileScanTask;
    use crate::spec::{
        DataFileFormat, Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type,
    };

    #[test]
    fn mixed_physical_and_position_predicate_preserves_file_row_number() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ])),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("mixed_predicate.parquet");
        let mut writer = ArrowWriter::try_new(
            File::create(&file_path).unwrap(),
            arrow_schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let physical_predicate = Reference::new("id")
            .greater_than_or_equal_to(Datum::int(5))
            .bind(schema.clone(), true)
            .unwrap();
        let position_field = Arc::new(NestedField::required(
            RESERVED_FIELD_ID_POS,
            RESERVED_COL_NAME_POS,
            Type::Primitive(PrimitiveType::Long),
        ));
        let position_reference = BoundReference::new(
            RESERVED_COL_NAME_POS,
            position_field,
            Arc::new(StructAccessor::new(1, PrimitiveType::Long)),
        );
        let position_predicate = BoundPredicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            position_reference,
            Datum::long(7),
        ));
        let task = FileScanTask {
            file_size_in_bytes: 0,
            start: 0,
            length: 0,
            record_count: Some(10),
            first_row_id: None,
            data_file_path: file_path.to_string_lossy().into_owned(),
            data_file_format: DataFileFormat::Parquet,
            partition_spec_id: 0,
            schema,
            project_field_ids: vec![1, RESERVED_FIELD_ID_POS],
            predicate: Some(physical_predicate.and(position_predicate)),
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        };
        let file_io = FileIO::from_path(temp_dir.path().to_str().unwrap()).unwrap();
        let batches = ArrowReaderBuilder::new(file_io)
            .build()
            .read(vec![task])
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();

        let mut ids = Vec::new();
        let mut positions = Vec::new();
        for batch in &batches {
            ids.extend_from_slice(
                batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values(),
            );
            positions.extend_from_slice(
                batch
                    .column(1)
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .values(),
            );
        }

        assert_eq!(ids, vec![7]);
        assert_eq!(positions, vec![7]);
    }

    #[test]
    fn test_kleene_logic_or_behaviour() {
        // a IS NULL OR a = 'foo'
        let predicate = Reference::new("a")
            .is_null()
            .or(Reference::new("a").equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a =
            vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: [NULL, "foo"].
        let expected = vec![None, Some("foo".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        let result_data =
            test_perform_read(predicate, schema, table_location, reader);

        assert_eq!(result_data, expected);
    }

    #[test]
    fn test_kleene_logic_and_behaviour() {
        // a IS NOT NULL AND a != 'foo'
        let predicate = Reference::new("a")
            .is_not_null()
            .and(Reference::new("a").not_equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a =
            vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: ["bar"].
        let expected = vec![Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        let result_data =
            test_perform_read(predicate, schema, table_location, reader);

        assert_eq!(result_data, expected);
    }

    #[test]
    fn test_predicate_cast_literal() {
        let predicates = vec![
            // a == 'foo'
            (
                Reference::new("a").equal_to(Datum::string("foo")),
                vec![Some("foo".to_string())],
            ),
            // a != 'foo'
            (
                Reference::new("a").not_equal_to(Datum::string("foo")),
                vec![Some("bar".to_string())],
            ),
            // STARTS_WITH(a, 'foo')
            (
                Reference::new("a").starts_with(Datum::string("f")),
                vec![Some("foo".to_string())],
            ),
            // NOT STARTS_WITH(a, 'foo')
            (
                Reference::new("a").not_starts_with(Datum::string("f")),
                vec![Some("bar".to_string())],
            ),
            // a < 'foo'
            (
                Reference::new("a").less_than(Datum::string("foo")),
                vec![Some("bar".to_string())],
            ),
            // a <= 'foo'
            (
                Reference::new("a").less_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string()), Some("bar".to_string())],
            ),
            // a > 'foo'
            (
                Reference::new("a").greater_than(Datum::string("bar")),
                vec![Some("foo".to_string())],
            ),
            // a >= 'foo'
            (
                Reference::new("a").greater_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string())],
            ),
            // a IN ('foo', 'bar')
            (
                Reference::new("a")
                    .is_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("foo".to_string())],
            ),
            // a NOT IN ('foo', 'bar')
            (
                Reference::new("a")
                    .is_not_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("bar".to_string())],
            ),
        ];

        // Table data: ["foo", "bar"]
        let data_for_col_a = vec![Some("foo".to_string()), Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::LargeUtf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        for (predicate, expected) in predicates {
            println!("testing predicate {predicate}");
            let result_data = test_perform_read(
                predicate.clone(),
                schema.clone(),
                table_location.clone(),
                reader.clone(),
            );

            assert_eq!(result_data, expected, "predicate={predicate}");
        }
    }

    fn test_perform_read(
        predicate: Predicate,
        schema: SchemaRef,
        table_location: String,
        reader: ArrowReader,
    ) -> Vec<Option<String>> {
        let tasks = vec![FileScanTask {
            file_size_in_bytes: 0,
            start: 0,
            length: 0,
            record_count: None,
            first_row_id: None,
            data_file_path: format!("{table_location}/1.parquet"),
            data_file_format: DataFileFormat::Parquet,
            partition_spec_id: 0,
            schema: schema.clone(),
            project_field_ids: vec![1],
            predicate: Some(predicate.bind(schema, true).unwrap()),
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        }];

        let result = reader
            .read(tasks)
            .unwrap()
            .collect::<Result<Vec<RecordBatch>, _>>()
            .unwrap();

        result[0].columns()[0]
            .as_string_opt::<i32>()
            .unwrap()
            .iter()
            .map(|v| v.map(ToOwned::to_owned))
            .collect::<Vec<_>>()
    }

    fn setup_kleene_logic(
        data_for_col_a: Vec<Option<String>>,
        col_a_type: DataType,
    ) -> (FileIO, SchemaRef, String, TempDir) {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "a",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", col_a_type.clone(), true).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        let file_io = FileIO::from_path(&table_location).unwrap();

        let col = match col_a_type {
            DataType::Utf8 => Arc::new(StringArray::from(data_for_col_a)) as ArrayRef,
            DataType::LargeUtf8 => {
                Arc::new(LargeStringArray::from(data_for_col_a)) as ArrayRef
            }
            _ => panic!("unexpected col_a_type"),
        };

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col]).unwrap();

        // Write the Parquet files
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, to_write.schema(), Some(props.clone()))
                .unwrap();

        writer.write(&to_write).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        (file_io, schema, table_location, tmp_dir)
    }

    /// Verifies that file splits respect byte ranges and only read specific row groups.
    #[test]
    fn test_file_splits_respect_byte_ranges() {
        use arrow_array::Int32Array;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string()),
            ])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/multi_row_group.parquet");

        // Force each batch into its own row group for testing byte range filtering.
        let batch1 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>()))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from((100..200).collect::<Vec<i32>>()))],
        )
        .unwrap();
        let batch3 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from((200..300).collect::<Vec<i32>>()))],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_row_count(Some(100))
            .build();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.write(&batch3).expect("Writing batch 3");
        writer.close().unwrap();

        // Read the file metadata to get row group byte positions
        let file = File::open(&file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        println!("File has {} row groups", metadata.num_row_groups());
        assert_eq!(metadata.num_row_groups(), 3, "Expected 3 row groups");

        // Get byte positions for each row group
        let row_group_0 = metadata.row_group(0);
        let row_group_1 = metadata.row_group(1);
        let row_group_2 = metadata.row_group(2);

        let rg0_start = 4u64; // Parquet files start with 4-byte magic "PAR1"
        let rg1_start = rg0_start + row_group_0.compressed_size() as u64;
        let rg2_start = rg1_start + row_group_1.compressed_size() as u64;
        let file_end = rg2_start + row_group_2.compressed_size() as u64;

        println!(
            "Row group 0: {} rows, starts at byte {}, {} bytes compressed",
            row_group_0.num_rows(),
            rg0_start,
            row_group_0.compressed_size()
        );
        println!(
            "Row group 1: {} rows, starts at byte {}, {} bytes compressed",
            row_group_1.num_rows(),
            rg1_start,
            row_group_1.compressed_size()
        );
        println!(
            "Row group 2: {} rows, starts at byte {}, {} bytes compressed",
            row_group_2.num_rows(),
            rg2_start,
            row_group_2.compressed_size()
        );

        let file_io = FileIO::from_path(&table_location).unwrap();
        let reader = ArrowReaderBuilder::new(file_io).build();

        // Task 1: read only the first row group
        let task1 = FileScanTask {
            file_size_in_bytes: 0,
            start: rg0_start,
            length: row_group_0.compressed_size() as u64,
            record_count: Some(100),
            first_row_id: None,
            data_file_path: file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            partition_spec_id: 0,
            schema: schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };

        // Task 2: read the second and third row groups
        let task2 = FileScanTask {
            file_size_in_bytes: 0,
            start: rg1_start,
            length: file_end - rg1_start,
            record_count: Some(200),
            first_row_id: None,
            data_file_path: file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            partition_spec_id: 0,
            schema: schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };

        let tasks1 = vec![task1];
        let result1 = reader
            .clone()
            .read(tasks1)
            .unwrap()
            .collect::<Result<Vec<RecordBatch>, _>>()
            .unwrap();

        let total_rows_task1: usize = result1.iter().map(|b| b.num_rows()).sum();
        println!(
            "Task 1 (bytes {}-{}) returned {} rows",
            rg0_start,
            rg0_start + row_group_0.compressed_size() as u64,
            total_rows_task1
        );

        let tasks2 = vec![task2];
        let result2 = reader
            .read(tasks2)
            .unwrap()
            .collect::<Result<Vec<RecordBatch>, _>>()
            .unwrap();

        let total_rows_task2: usize = result2.iter().map(|b| b.num_rows()).sum();
        println!(
            "Task 2 (bytes {rg1_start}-{file_end}) returned {total_rows_task2} rows"
        );

        assert_eq!(
            total_rows_task1, 100,
            "Task 1 should read only the first row group (100 rows), but got {total_rows_task1} rows"
        );

        assert_eq!(
            total_rows_task2, 200,
            "Task 2 should read only the second+third row groups (200 rows), but got {total_rows_task2} rows"
        );

        // Verify the actual data values are correct (not just the row count)
        if total_rows_task1 > 0 {
            let first_batch = &result1[0];
            let id_col = first_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let first_val = id_col.value(0);
            let last_val = id_col.value(id_col.len() - 1);
            println!("Task 1 data range: {first_val} to {last_val}");

            assert_eq!(first_val, 0, "Task 1 should start with id=0");
            assert_eq!(last_val, 99, "Task 1 should end with id=99");
        }

        if total_rows_task2 > 0 {
            let first_batch = &result2[0];
            let id_col = first_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let first_val = id_col.value(0);
            println!("Task 2 first value: {first_val}");

            assert_eq!(first_val, 100, "Task 2 should start with id=100, not id=0");
        }
    }
}
