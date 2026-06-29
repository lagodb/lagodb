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

//! The main `ArrowReader` pipeline: reading `FileScanTask`s, opening
//! Parquet files and resolving schemas, then wiring projection, predicates,
//! row-group / row selection, and delete handling into transformed Arrow
//! `RecordBatch` iterators.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, RowNumber};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

use super::predicate_plan::FilePredicatePlan;
use super::row_filter::TransformedRecordBatchFilter;
use super::row_position::RowPositionSelection;
use super::{
    ArrowFileReader, ArrowReader, ParquetReadOptions, PhysicalRowReadRequest,
    add_fallback_field_ids_to_arrow_schema, apply_name_mapping_to_arrow_schema,
};
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::delete_filter::is_equality_delete;
use crate::arrow::int96::coerce_int96_timestamps;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::scan_metrics::{CountingFileRead, ScanMetrics, ScanResult};
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::io::{FileIO, FileMetadata, FileRead};
use crate::metadata_columns::{
    RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_FILE, RESERVED_FIELD_ID_POS,
    RESERVED_FIELD_ID_ROW_ID, is_metadata_field,
};
use crate::scan::{ArrowRecordBatchIterator, FileScanTask};
use crate::spec::{Datum, NameMapping, PartitionSpec, SchemaRef, Struct};
use crate::{Error, ErrorKind};

/// Preserve the upstream scan task as a domain object until scan-only delete
/// handling is complete; physical reads have different visibility semantics.
/// Keeping the scan variant inline avoids one heap allocation per data file.
#[allow(clippy::large_enum_variant)]
enum FileReadRequest {
    Scan(FileScanTask),
    Physical(PhysicalRowReadRequest),
}

/// Borrowed inputs shared by the Parquet execution core after request-specific
/// behavior has been selected.
struct FileReadPlan<'a> {
    file_size_in_bytes: u64,
    start: u64,
    length: u64,
    first_row_id: Option<u64>,
    data_file_path: &'a str,
    schema: &'a SchemaRef,
    project_field_ids: &'a [i32],
    predicate: Option<&'a BoundPredicate>,
    partition: Option<&'a Struct>,
    partition_spec: Option<&'a Arc<PartitionSpec>>,
    name_mapping: Option<&'a Arc<NameMapping>>,
    has_deletes: bool,
    has_equality_deletes: bool,
    row_position: Option<i64>,
}

impl FileReadRequest {
    fn plan(&self) -> FileReadPlan<'_> {
        match self {
            Self::Scan(task) => FileReadPlan {
                file_size_in_bytes: task.file_size_in_bytes,
                start: task.start,
                length: task.length,
                first_row_id: task.first_row_id,
                data_file_path: &task.data_file_path,
                schema: &task.schema,
                project_field_ids: &task.project_field_ids,
                predicate: task.predicate.as_ref(),
                partition: task.partition.as_ref(),
                partition_spec: task.partition_spec.as_ref(),
                name_mapping: task.name_mapping.as_ref(),
                has_deletes: !task.deletes.is_empty(),
                has_equality_deletes: task.deletes.iter().any(is_equality_delete),
                row_position: None,
            },
            Self::Physical(request) => FileReadPlan {
                file_size_in_bytes: 0,
                start: 0,
                length: 0,
                first_row_id: None,
                data_file_path: &request.data_file_path,
                schema: &request.schema,
                project_field_ids: &request.projected_field_ids,
                predicate: None,
                partition: None,
                partition_spec: None,
                name_mapping: request.name_mapping.as_ref(),
                has_deletes: false,
                has_equality_deletes: false,
                row_position: Some(request.position),
            },
        }
    }
}

/// Synchronous file-scan pipeline wrapper.
///
/// Upstream iceberg-rust executes this stage with async tasks on its runtime.
/// iceberg-lite keeps a named pipeline entry point, but execution is
/// deterministic and sequential through the synchronous reader.
pub struct SyncFileScanPipeline {
    reader: ArrowReader,
    tasks: Vec<FileScanTask>,
}

impl SyncFileScanPipeline {
    pub fn new(reader: ArrowReader, tasks: Vec<FileScanTask>) -> Self {
        Self { reader, tasks }
    }

    pub fn execute(self) -> Result<ScanResult> {
        self.reader.read_with_metrics(self.tasks)
    }
}

impl ArrowReader {
    /// Creates a synchronous pipeline for future callers that need an explicit
    /// pipeline object instead of calling `read_with_metrics` directly.
    pub fn sync_pipeline(self, tasks: Vec<FileScanTask>) -> SyncFileScanPipeline {
        SyncFileScanPipeline::new(self, tasks)
    }
}

impl ArrowReader {
    /// Take a list of FileScanTasks and reads all the files.
    /// Returns an iterator of Arrow RecordBatches containing the data from the files
    pub fn read(self, tasks: Vec<FileScanTask>) -> Result<ArrowRecordBatchIterator> {
        Ok(self.read_with_metrics(tasks)?.stream())
    }

    /// Take a list of FileScanTasks and read all the files.
    ///
    /// Returns a [`ScanResult`] containing the record batch iterator and scan metrics.
    pub fn read_with_metrics(self, tasks: Vec<FileScanTask>) -> Result<ScanResult> {
        self.read_requests_with_metrics(tasks.into_iter().map(FileReadRequest::Scan))
    }

    /// Reads the exact stored row at one original, zero-based file position.
    ///
    /// This physical read does not apply Iceberg predicates or delete files.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or transformed according to
    /// the request schema.
    pub fn read_physical_row(
        self,
        request: PhysicalRowReadRequest,
    ) -> Result<Option<RecordBatch>> {
        let batches = self
            .read_requests_with_metrics(std::iter::once(FileReadRequest::Physical(
                request,
            )))?
            .stream();
        for batch in batches {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            if batch.num_rows() != 1 {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "position-constrained reader returned more than one row",
                ));
            }
            return Ok(Some(batch));
        }
        Ok(None)
    }

    fn read_requests_with_metrics<I>(self, requests: I) -> Result<ScanResult>
    where
        I: IntoIterator<Item = FileReadRequest>,
        I::IntoIter: Send + 'static,
    {
        let file_io = self.file_io;
        let batch_size = self.batch_size;
        let row_group_filtering_enabled = self.row_group_filtering_enabled;
        let row_selection_enabled = self.row_selection_enabled;
        let parquet_read_options = self.parquet_read_options;
        let scan_metrics = ScanMetrics::new();
        let scan_metrics_for_result = scan_metrics.clone();
        let delete_file_loader = self
            .delete_file_loader
            .with_scan_metrics(scan_metrics.clone());

        let iterator = requests.into_iter().flat_map(move |request| {
            let file_io = file_io.clone();
            let delete_file_loader = delete_file_loader.clone();
            let scan_metrics = scan_metrics.clone();

            match Self::process_file_read_request(
                request,
                batch_size,
                file_io,
                delete_file_loader,
                row_group_filtering_enabled,
                row_selection_enabled,
                parquet_read_options,
                scan_metrics,
            ) {
                Ok(iter) => iter,
                Err(e) => {
                    let err =
                        Error::new(ErrorKind::Unexpected, "file read request failed")
                            .with_source(e);
                    Box::new(std::iter::once(Err(err))) as ArrowRecordBatchIterator
                }
            }
        });

        Ok(ScanResult::new(Box::new(iterator), scan_metrics_for_result))
    }

    #[allow(clippy::too_many_arguments)]
    fn process_file_read_request(
        request: FileReadRequest,
        batch_size: Option<usize>,
        file_io: FileIO,
        delete_file_loader: CachingDeleteFileLoader,
        row_group_filtering_enabled: bool,
        row_selection_enabled: bool,
        parquet_read_options: ParquetReadOptions,
        scan_metrics: ScanMetrics,
    ) -> Result<ArrowRecordBatchIterator> {
        let plan = request.plan();
        let predicate_page_pruning = row_selection_enabled
            && (plan.predicate.is_some() || plan.has_equality_deletes);
        let column_index_policy = if predicate_page_pruning {
            PageIndexPolicy::Optional
        } else {
            PageIndexPolicy::Skip
        };
        let offset_index_policy = if plan.row_position.is_some()
            || predicate_page_pruning
            || plan.has_deletes
        {
            PageIndexPolicy::Optional
        } else {
            PageIndexPolicy::Skip
        };
        let parquet_read_options = parquet_read_options
            .with_index_policies(column_index_policy, offset_index_policy);

        let (delete_predicate, positional_delete_indexes) = match &request {
            FileReadRequest::Scan(file_scan_task) => {
                let delete_filter = delete_file_loader.load_deletes(
                    &file_scan_task.deletes,
                    file_scan_task.schema_ref(),
                )?;
                let delete_predicate =
                    delete_filter.build_equality_delete_predicate(file_scan_task)?;
                let positional_delete_indexes =
                    delete_filter.get_delete_vector(file_scan_task);
                (delete_predicate, positional_delete_indexes)
            }
            // SnapshotAny physical fetches return the exact stored row and must
            // not apply the table's logical delete files.
            FileReadRequest::Physical(_) => (None, None),
        };

        let (parquet_file_reader, arrow_metadata) = Self::open_parquet_file(
            plan.data_file_path,
            &file_io,
            plan.file_size_in_bytes,
            parquet_read_options,
            Some(scan_metrics),
        )?;

        // Check if Parquet file has embedded field IDs
        // Corresponds to Java's ParquetSchemaUtil.hasIds()
        // Reference: parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java:118
        let missing_field_ids = arrow_metadata
            .schema()
            .fields()
            .iter()
            .next()
            .is_some_and(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none());

        let use_position_fallback = missing_field_ids && plan.name_mapping.is_none();

        // Three-branch schema resolution strategy matching Java's ReadConf constructor
        //
        // Per Iceberg spec Column Projection rules:
        // "Columns in Iceberg data files are selected by field id. The table schema's column
        //  names and order may change after a data file is written, and projection must be done
        //  using field ids."
        // https://iceberg.apache.org/spec/#column-projection
        //
        // When Parquet files lack field IDs (e.g., Hive/Spark migrations via add_files),
        // we must assign field IDs BEFORE reading data to enable correct projection.
        //
        // Java's ReadConf determines field ID strategy:
        // - Branch 1: hasIds(fileSchema) → trust embedded field IDs, use pruneColumns()
        // - Branch 2: nameMapping present → applyNameMapping(), then pruneColumns()
        // - Branch 3: fallback → addFallbackIds(), then pruneColumnsFallback()
        let arrow_metadata = if missing_field_ids {
            // Parquet file lacks field IDs - must assign them before reading
            let arrow_schema = if let Some(name_mapping) = plan.name_mapping {
                // Branch 2: Apply name mapping to assign correct Iceberg field IDs
                // Per spec rule #2: "Use schema.name-mapping.default metadata to map field id
                // to columns without field id"
                // Corresponds to Java's ParquetSchemaUtil.applyNameMapping()
                apply_name_mapping_to_arrow_schema(
                    Arc::clone(arrow_metadata.schema()),
                    name_mapping,
                )?
            } else {
                // Branch 3: No name mapping - use position-based fallback IDs
                // Corresponds to Java's ParquetSchemaUtil.addFallbackIds()
                add_fallback_field_ids_to_arrow_schema(arrow_metadata.schema())
            };

            let options = ArrowReaderOptions::new().with_schema(arrow_schema);
            ArrowReaderMetadata::try_new(
                Arc::clone(arrow_metadata.metadata()),
                options,
            )
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to create ArrowReaderMetadata with field ID schema",
                )
                .with_source(err)
            })?
        } else {
            // Branch 1: File has embedded field IDs - trust them
            arrow_metadata
        };

        let arrow_metadata = if let Some(coerced_schema) =
            coerce_int96_timestamps(arrow_metadata.schema(), plan.schema)
        {
            let options = ArrowReaderOptions::new().with_schema(coerced_schema);
            ArrowReaderMetadata::try_new(
                Arc::clone(arrow_metadata.metadata()),
                options,
            )
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to create ArrowReaderMetadata with INT96-coerced schema",
                )
                .with_source(err)
            })?
        } else {
            arrow_metadata
        };

        // In addition to the optional predicate supplied in the `FileScanTask`,
        // we also have an optional predicate resulting from equality delete files.
        // If both are present, we logical-AND them together to form a single filter.
        let final_predicate = match (plan.predicate, delete_predicate) {
            (None, None) => None,
            (Some(predicate), None) => Some(predicate.clone()),
            (None, Some(ref predicate)) => Some(predicate.clone()),
            (Some(filter_predicate), Some(delete_predicate)) => {
                Some(filter_predicate.clone().and(delete_predicate))
            }
        };

        let predicate_plan = FilePredicatePlan::try_new(final_predicate)?;
        let requested_project_field_ids = plan.project_field_ids.to_vec();
        let post_transform_field_ids = predicate_plan.post_transform_field_ids();
        let needs_position_column = requested_project_field_ids
            .contains(&RESERVED_FIELD_ID_POS)
            || post_transform_field_ids.contains(&RESERVED_FIELD_ID_POS);
        let needs_row_id_column = requested_project_field_ids
            .contains(&RESERVED_FIELD_ID_ROW_ID)
            || post_transform_field_ids.contains(&RESERVED_FIELD_ID_ROW_ID);
        let needs_row_number_column = needs_position_column || needs_row_id_column;
        let mut effective_project_field_ids = requested_project_field_ids.clone();
        let mut ordered_post_transform_field_ids =
            post_transform_field_ids.iter().copied().collect::<Vec<_>>();
        ordered_post_transform_field_ids.sort_unstable();
        for field_id in ordered_post_transform_field_ids {
            if !effective_project_field_ids.contains(&field_id) {
                effective_project_field_ids.push(field_id);
            }
        }
        if needs_row_id_column
            && !effective_project_field_ids.contains(&RESERVED_FIELD_ID_POS)
        {
            effective_project_field_ids.push(RESERVED_FIELD_ID_POS);
        }
        let first_row_id = if needs_row_id_column {
            Some(plan.first_row_id.ok_or_else(|| {
                Error::new(
                    ErrorKind::FeatureUnsupported,
                    "_row_id requires Iceberg format v3 row lineage",
                )
            })?)
        } else {
            None
        };

        let arrow_metadata = if needs_row_number_column {
            Self::with_row_number_column(arrow_metadata)?
        } else {
            arrow_metadata
        };

        let mut record_batch_reader_builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(
                parquet_file_reader,
                arrow_metadata,
            );

        // Filter out generated metadata fields for Parquet projection. `_pos`
        // is the exception: arrow-rs exposes it as a native virtual RowNumber
        // column, so it can be projected and row-filtered like a file column.
        let project_field_ids_without_metadata: Vec<i32> =
            effective_project_field_ids
                .iter()
                .filter(|&&id| {
                    !is_metadata_field(id)
                        || (id == RESERVED_FIELD_ID_POS && needs_row_number_column)
                })
                .copied()
                .collect();

        // Create projection mask based on field IDs
        // - If file has embedded IDs: field-ID-based projection.
        // - If name mapping applied: field-ID-based projection using mapped IDs.
        // - Otherwise: position-based fallback projection.
        let projection_mask = Self::get_arrow_projection_mask(
            &project_field_ids_without_metadata,
            plan.schema,
            record_batch_reader_builder.parquet_schema(),
            record_batch_reader_builder.schema(),
            use_position_fallback,
        )?;

        record_batch_reader_builder =
            record_batch_reader_builder.with_projection(projection_mask.clone());

        // RecordBatchTransformer performs any transformations required on the RecordBatches
        // that come back from the file, such as type promotion, default column insertion,
        // column re-ordering, partition constants, and virtual field addition (like _file)
        let mut record_batch_transformer_builder = RecordBatchTransformerBuilder::new(
            Arc::clone(plan.schema),
            &effective_project_field_ids,
        );

        // Add the _file metadata column if it's in the projected fields
        if effective_project_field_ids.contains(&RESERVED_FIELD_ID_FILE) {
            let file_datum = Datum::string(plan.data_file_path.to_owned());
            record_batch_transformer_builder = record_batch_transformer_builder
                .with_constant(RESERVED_FIELD_ID_FILE, file_datum);
        }

        if needs_row_number_column {
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_position_column();
        }

        if let Some(first_row_id) = first_row_id {
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_row_id_column(first_row_id);
        }

        if let (Some(partition_spec), Some(partition_data)) =
            (plan.partition_spec.cloned(), plan.partition.cloned())
        {
            record_batch_transformer_builder = record_batch_transformer_builder
                .with_partition(partition_spec, partition_data)?;
        }

        if let Some(batch_size) = batch_size {
            record_batch_reader_builder =
                record_batch_reader_builder.with_batch_size(batch_size);
        }

        // There are three possible sources for potential lists of selected RowGroup indices,
        // and two for `RowSelection`s.
        // Selected RowGroup index lists can come from three sources:
        //   * When plan.start and plan.length specify a byte range (file splitting);
        //   * When there are equality delete files that are applicable;
        //   * When there is a scan predicate and row_group_filtering_enabled = true.
        // `RowSelection`s can be created in either or both of the following cases:
        //   * When there are positional delete files that are applicable;
        //   * When there is a scan predicate and row_selection_enabled = true
        // Note that row group filtering from predicates only happens when
        // there is a scan predicate AND row_group_filtering_enabled = true,
        // but we perform row selection filtering if there are applicable
        // equality delete files OR (there is a scan predicate AND row_selection_enabled),
        // since the only implemented method of applying positional deletes is
        // by using a `RowSelection`.
        let mut selected_row_group_indices = None;
        let mut row_selection = None;

        // Filter row groups based on byte range from plan.start and plan.length.
        // If both start and length are 0, read the entire file (backwards compatibility).
        if plan.start != 0 || plan.length != 0 {
            let byte_range_filtered_row_groups =
                Self::filter_row_groups_by_byte_range(
                    record_batch_reader_builder.metadata(),
                    plan.start,
                    plan.length,
                )?;
            selected_row_group_indices = Some(byte_range_filtered_row_groups);
        }

        let position_selection = plan
            .row_position
            .map(|position| {
                RowPositionSelection::try_new(
                    record_batch_reader_builder.metadata(),
                    position,
                )
            })
            .transpose()?;
        if let Some(position_selection) = &position_selection {
            position_selection.restrict_row_groups(&mut selected_row_group_indices);
        }

        // The planner removes these physical conjuncts from the post-transform
        // residual, so installing the exact Arrow RowFilter is mandatory. The
        // row-group and page-index evaluators below are conservative reuse of
        // the same predicate, not substitutes for this row-level filter.
        if let Some(predicate) = predicate_plan.parquet_filter_predicate() {
            let (iceberg_field_ids, field_id_map) = Self::build_field_id_set_and_map(
                record_batch_reader_builder.parquet_schema(),
                record_batch_reader_builder.schema(),
                predicate,
                use_position_fallback,
            )?;
            let row_filter = Self::get_row_filter(
                predicate,
                record_batch_reader_builder.parquet_schema(),
                &iceberg_field_ids,
                &field_id_map,
            )?;
            record_batch_reader_builder =
                record_batch_reader_builder.with_row_filter(row_filter);

            if row_group_filtering_enabled {
                let predicate_filtered_row_groups =
                    Self::get_selected_row_group_indices(
                        predicate,
                        record_batch_reader_builder.metadata(),
                        &field_id_map,
                        plan.schema,
                    )?;

                // Merge predicate-based filtering with byte range filtering (if present)
                // by taking the intersection of both filters
                selected_row_group_indices = match selected_row_group_indices {
                    Some(byte_range_filtered) => {
                        // Keep only row groups that are in both filters
                        let intersection: Vec<usize> = byte_range_filtered
                            .into_iter()
                            .filter(|idx| predicate_filtered_row_groups.contains(idx))
                            .collect();
                        Some(intersection)
                    }
                    None => Some(predicate_filtered_row_groups),
                };
            }

            if row_selection_enabled {
                row_selection = Self::get_row_selection_for_filter_predicate(
                    predicate,
                    record_batch_reader_builder.metadata(),
                    &selected_row_group_indices,
                    &field_id_map,
                    plan.schema,
                )?;
            }
        }

        if let Some(position_selection) = position_selection {
            position_selection.merge_row_selection(&mut row_selection);
        }

        if let Some(positional_delete_indexes) = positional_delete_indexes {
            let delete_row_selection = {
                let positional_delete_indexes =
                    positional_delete_indexes.lock().unwrap();

                Self::build_deletes_row_selection(
                    record_batch_reader_builder.metadata().row_groups(),
                    &selected_row_group_indices,
                    &positional_delete_indexes,
                )
            }?;

            // merge the row selection from the delete files with the row selection
            // from the filter predicate, if there is one from the filter predicate
            row_selection = match row_selection {
                None => Some(delete_row_selection),
                Some(filter_row_selection) => {
                    Some(filter_row_selection.intersection(&delete_row_selection))
                }
            };
        }

        let mut record_batch_transformer = record_batch_transformer_builder.build();

        if let Some(row_selection) = row_selection {
            record_batch_reader_builder =
                record_batch_reader_builder.with_row_selection(row_selection);
        }

        if let Some(selected_row_group_indices) = selected_row_group_indices {
            record_batch_reader_builder = record_batch_reader_builder
                .with_row_groups(selected_row_group_indices);
        }

        let mut post_transform_filter = predicate_plan
            .into_post_transform_residual()
            .map(TransformedRecordBatchFilter::new);
        let should_prune_internal_projection =
            effective_project_field_ids != requested_project_field_ids;

        // Build the batch stream and send all the RecordBatches that it generates
        // to the requester.
        let record_batch_reader = record_batch_reader_builder.build()?;
        let iterator = record_batch_reader.map(move |batch| match batch {
            Ok(batch) => {
                // Process the record batch (type promotion, column reordering, virtual fields, etc.)
                let mut batch =
                    record_batch_transformer.process_record_batch(batch)?;
                if let Some(filter) = &mut post_transform_filter {
                    batch = filter.filter(batch)?;
                }
                if should_prune_internal_projection {
                    batch = Self::project_record_batch_by_field_ids(
                        batch,
                        &requested_project_field_ids,
                    )?;
                }
                Ok(batch)
            }
            Err(err) => Err(err.into()),
        });

        Ok(Box::new(iterator))
    }

    fn with_row_number_column(
        arrow_metadata: ArrowReaderMetadata,
    ) -> Result<ArrowReaderMetadata> {
        let options = ArrowReaderOptions::new()
            .with_schema(Arc::clone(arrow_metadata.schema()))
            .with_virtual_columns(vec![Self::row_number_field()])
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to configure Parquet row-number metadata column",
                )
                .with_source(err)
            })?;

        ArrowReaderMetadata::try_new(Arc::clone(arrow_metadata.metadata()), options)
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to create ArrowReaderMetadata with row-number column",
                )
                .with_source(err)
            })
    }

    fn row_number_field() -> FieldRef {
        Arc::new(
            Field::new(RESERVED_COL_NAME_POS, DataType::Int64, false)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    RESERVED_FIELD_ID_POS.to_string(),
                )]))
                .with_extension_type(RowNumber),
        )
    }

    pub(crate) fn open_parquet_file(
        data_file_path: &str,
        file_io: &FileIO,
        file_size_in_bytes: u64,
        parquet_read_options: ParquetReadOptions,
        scan_metrics: Option<ScanMetrics>,
    ) -> Result<(ArrowFileReader<Box<dyn FileRead>>, ArrowReaderMetadata)> {
        let parquet_file = file_io.new_input(data_file_path)?;
        let opened_file = parquet_file.open_reader()?;
        let metadata = if file_size_in_bytes == 0 {
            opened_file.metadata
        } else {
            FileMetadata {
                size: file_size_in_bytes,
            }
        };
        let reader = match scan_metrics {
            Some(metrics) => Box::new(CountingFileRead::new(
                opened_file.reader,
                Arc::clone(metrics.bytes_read_counter()),
            )) as Box<dyn FileRead>,
            None => opened_file.reader,
        };
        let parquet_file_reader = ArrowFileReader::new(metadata, reader);

        let options =
            parquet_read_options.apply_to_options(ArrowReaderOptions::default());
        let metadata_options = options.metadata_options().clone();
        let decryption_properties = options.file_decryption_properties().cloned();
        let parquet_metadata = parquet_read_options
            .apply_to_metadata_reader(ParquetMetaDataReader::new())
            .with_prefetch_hint(parquet_read_options.metadata_size_hint)
            .with_metadata_options(Some(metadata_options))
            .with_decryption_properties(decryption_properties)
            .parse_and_finish(&parquet_file_reader)
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Failed to load Parquet metadata")
                    .with_source(err)
            })?;
        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::new(parquet_metadata), options)
                .map_err(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to create Arrow reader metadata",
                    )
                    .with_source(err)
                })?;

        Ok((parquet_file_reader, arrow_metadata))
    }
}
