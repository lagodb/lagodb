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

//! Table scan api.

mod cache;
use cache::*;
mod context;
use context::*;
mod task;

use std::sync::Arc;

use arrow_array::RecordBatch;
pub use task::*;

use crate::arrow::{ArrowReaderBuilder, SelectedRowsReadRequest};
use crate::delete_file_index::DeleteFileIndexBuilder;
use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::io::FileIO;
use crate::metadata_columns::{
    get_metadata_field_id, is_metadata_column_name, is_metadata_field,
};
use crate::overlay::{SnapshotDelta, SnapshotDeltaRemovalLookup};

use crate::spec::{
    DEFAULT_SCHEMA_NAME_MAPPING, DataContentType, ManifestContentType, NameMapping,
    SnapshotRef,
};
use crate::table::Table;
use crate::util::available_parallelism;
use crate::{Error, ErrorKind, Result};

/// A synchronous iterator over Arrow [`RecordBatch`]es.
pub type ArrowRecordBatchIterator =
    Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;

/// Builder to create table scan.
pub struct TableScanBuilder<'a> {
    table: &'a Table,
    projection: ScanProjection,
    snapshot_id: Option<i64>,
    batch_size: Option<usize>,
    case_sensitive: bool,
    filter: Option<Predicate>,
    concurrency_limit_data_files: usize,
    concurrency_limit_manifest_entries: usize,
    concurrency_limit_manifest_files: usize,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    delta: Option<Arc<SnapshotDelta>>,
}

#[derive(Debug, Clone, Default)]
enum ScanProjection {
    #[default]
    All,
    Names(Vec<String>),
    FieldIds(Vec<i32>),
}

impl<'a> TableScanBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        let num_cpus = available_parallelism().get();

        Self {
            table,
            projection: ScanProjection::All,
            snapshot_id: None,
            batch_size: None,
            case_sensitive: true,
            filter: None,
            concurrency_limit_data_files: num_cpus,
            concurrency_limit_manifest_entries: num_cpus,
            concurrency_limit_manifest_files: num_cpus,
            row_group_filtering_enabled: true,
            row_selection_enabled: false,
            delta: None,
        }
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the scan's case sensitivity
    pub fn with_case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Specifies a predicate to use as a filter
    pub fn with_filter(mut self, predicate: Predicate) -> Self {
        // calls rewrite_not to remove Not nodes, which must be absent
        // when applying the manifest evaluator
        self.filter = Some(predicate.rewrite_not());
        self
    }

    /// Select all columns.
    pub fn select_all(mut self) -> Self {
        self.projection = ScanProjection::All;
        self
    }

    /// Select empty columns.
    pub fn select_empty(mut self) -> Self {
        self.projection = ScanProjection::Names(Vec::new());
        self
    }

    /// Select some columns of the table.
    pub fn select(
        mut self,
        column_names: impl IntoIterator<Item = impl ToString>,
    ) -> Self {
        self.projection = ScanProjection::Names(
            column_names
                .into_iter()
                .map(|item| item.to_string())
                .collect(),
        );
        self
    }

    /// Select top-level table columns by Iceberg field id.
    ///
    /// This is equivalent to [`Self::select`] after name-to-id resolution, but
    /// lets PostgreSQL integrations bind catalog attributes to Iceberg field
    /// ids once and avoid using column names as execution identity.
    pub fn select_field_ids(
        mut self,
        field_ids: impl IntoIterator<Item = i32>,
    ) -> Self {
        self.projection = ScanProjection::FieldIds(field_ids.into_iter().collect());
        self
    }

    /// Set the snapshot to scan. When not set, it uses current snapshot.
    pub fn snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Sets the concurrency limit for both manifest files and manifest
    /// entries for this scan
    pub fn with_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_files = limit;
        self.concurrency_limit_manifest_entries = limit;
        self.concurrency_limit_data_files = limit;
        self
    }

    /// Sets the data file concurrency limit for this scan
    pub fn with_data_file_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit_data_files = limit;
        self
    }

    /// Sets the manifest entry concurrency limit for this scan
    pub fn with_manifest_entry_concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit_manifest_entries = limit;
        self
    }

    /// Determines whether to enable row group filtering.
    /// When enabled, if a read is performed with a filter predicate,
    /// then the metadata for each row group in the parquet file is
    /// evaluated against the filter predicate and row groups
    /// that cant contain matching rows will be skipped entirely.
    ///
    /// Defaults to enabled, as it generally improves performance or
    /// keeps it the same, with performance degradation unlikely.
    pub fn with_row_group_filtering_enabled(
        mut self,
        row_group_filtering_enabled: bool,
    ) -> Self {
        self.row_group_filtering_enabled = row_group_filtering_enabled;
        self
    }

    /// Determines whether to enable row selection.
    /// When enabled, if a read is performed with a filter predicate,
    /// then (for row groups that have not been skipped) the page index
    /// for each row group in a parquet file is parsed and evaluated
    /// against the filter predicate to determine if ranges of rows
    /// within a row group can be skipped, based upon the page-level
    /// statistics for each column.
    ///
    /// Defaults to being disabled. Enabling requires parsing the parquet page
    /// index, which can be slow enough that parsing the page index outweighs any
    /// gains from the reduced number of rows that need scanning.
    /// It is recommended to experiment with partitioning, sorting, row group size,
    /// page size, and page row limit Iceberg settings on the table being scanned in
    /// order to get the best performance from using row selection.
    pub fn with_row_selection_enabled(mut self, row_selection_enabled: bool) -> Self {
        self.row_selection_enabled = row_selection_enabled;
        self
    }

    /// Layer a transaction-local snapshot delta over the committed table state.
    pub fn with_delta(mut self, delta: Arc<SnapshotDelta>) -> Self {
        self.delta = Some(delta);
        self
    }

    /// Build the table scan.
    pub fn build(self) -> Result<TableScan> {
        let use_current_schema = self.snapshot_id.is_none();
        let snapshot = match self.snapshot_id {
            Some(snapshot_id) => self
                .table
                .metadata()
                .snapshot_by_id(snapshot_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Snapshot with id {snapshot_id} not found"),
                    )
                })?
                .clone()
                .into(),
            None => {
                let current_snapshot =
                    self.table.metadata().current_snapshot().cloned();
                if current_snapshot.is_none()
                    && self.delta.as_ref().is_none_or(|delta| delta.is_empty())
                {
                    return Ok(TableScan {
                        batch_size: self.batch_size,
                        column_names: self.projection.column_names(),
                        file_io: self.table.file_io().clone(),
                        plan_context: None,
                        delta: None,
                        concurrency_limit_data_files: self
                            .concurrency_limit_data_files,
                        concurrency_limit_manifest_entries: self
                            .concurrency_limit_manifest_entries,
                        concurrency_limit_manifest_files: self
                            .concurrency_limit_manifest_files,
                        row_group_filtering_enabled: self.row_group_filtering_enabled,
                        row_selection_enabled: self.row_selection_enabled,
                    });
                }
                current_snapshot
            }
        };

        let schema = if use_current_schema {
            // A current table scan projects the table's current schema, not the
            // schema that happened to write the current snapshot. Schema-only
            // evolution can advance current-schema-id without creating a new
            // snapshot; newly-added fields must still project as NULL for old
            // files.
            self.table.metadata().current_schema().clone()
        } else {
            match snapshot.as_ref() {
                Some(snapshot) => snapshot.schema(self.table.metadata())?,
                None => self.table.metadata().current_schema().clone(),
            }
        };

        let field_ids = self.projection.field_ids(&schema)?;

        // TODO(metadata-column predicate binding): `Predicate::bind` below is
        // intentionally a table-schema binder, so `_pos`, `_file`, and
        // `_row_id` cannot be added here by merely teaching `Reference::bind`
        // about reserved names. Manifest, partition, and data-file metrics do
        // not contain row metadata and must never receive such a bound
        // predicate.
        //
        // The preferred PostgreSQL integration exposes metadata through an
        // explicit function or table-function API. The PG layer must resolve
        // that SQL using ordinary catalog semantics, associate every metadata
        // reference with its source relation, and normalize it to the reserved
        // Iceberg field ID before constructing this scan. This crate remains
        // independent of PostgreSQL syntax and parser hooks.
        //
        // Introduce one metadata-aware scan binder/planner that produces both:
        //   1. a conservative physical pruning predicate (`id = 2`) bound to
        //      the table schema for manifest/partition/file-metrics pruning;
        //   2. the full row predicate (`_pos = 3 AND id = 2`) bound to an
        //      evaluation schema containing the table fields plus reserved
        //      metadata fields, and attach that predicate to each FileScanTask.
        // The Arrow reader's FilePredicatePlan will then install `id = 2` as
        // an exact Parquet RowFilter and evaluate only `_pos = 3` after row
        // metadata has been generated. Mixed OR/NOT expressions must remain
        // conservative; never extract one branch unless it is a necessary
        // condition of the complete predicate. If bare `_pos` syntax is added
        // later, its PG core parser integration must normalize to this same
        // binder input rather than creating a second execution path.
        let snapshot_bound_predicate = if let Some(ref predicates) = self.filter {
            Some(predicates.bind(schema.clone(), true)?)
        } else {
            None
        };

        let name_mapping = self
            .table
            .metadata()
            .properties()
            .get(DEFAULT_SCHEMA_NAME_MAPPING)
            .map(|raw| {
                serde_json::from_str::<NameMapping>(raw).map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Failed to parse table property {DEFAULT_SCHEMA_NAME_MAPPING} as a NameMapping"
                        ),
                    )
                    .with_source(e)
                })
            })
            .transpose()?
            .map(Arc::new);

        let plan_context = PlanContext {
            snapshot,
            table_metadata: self.table.metadata_ref(),
            snapshot_schema: schema,
            case_sensitive: self.case_sensitive,
            predicate: self.filter.map(Arc::new),
            snapshot_bound_predicate: snapshot_bound_predicate.map(Arc::new),
            object_cache: self.table.object_cache(),
            field_ids: Arc::new(field_ids),
            name_mapping,
            partition_filter_cache: Arc::new(PartitionFilterCache::new()),
            manifest_evaluator_cache: Arc::new(ManifestEvaluatorCache::new()),
            expression_evaluator_cache: Arc::new(ExpressionEvaluatorCache::new()),
        };

        Ok(TableScan {
            batch_size: self.batch_size,
            column_names: self.projection.column_names(),
            file_io: self.table.file_io().clone(),
            plan_context: Some(plan_context),
            delta: self.delta,
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            concurrency_limit_manifest_entries: self
                .concurrency_limit_manifest_entries,
            concurrency_limit_manifest_files: self.concurrency_limit_manifest_files,
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
        })
    }
}

impl ScanProjection {
    fn column_names(self) -> Option<Vec<String>> {
        match self {
            Self::All | Self::FieldIds(_) => None,
            Self::Names(names) => Some(names),
        }
    }

    fn field_ids(&self, schema: &crate::spec::Schema) -> Result<Vec<i32>> {
        match self {
            Self::All => Ok(schema
                .as_struct()
                .fields()
                .iter()
                .map(|field| field.id)
                .collect()),
            Self::Names(column_names) => {
                let mut field_ids = Vec::with_capacity(column_names.len());
                for column_name in column_names {
                    if is_metadata_column_name(column_name) {
                        field_ids.push(get_metadata_field_id(column_name)?);
                        continue;
                    }

                    let field_id =
                        schema.field_id_by_name(column_name).ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "Column {column_name} not found in table. Schema: {schema}"
                                ),
                            )
                        })?;
                    Self::ensure_top_level_field(schema, field_id, column_name)?;
                    field_ids.push(field_id);
                }
                Ok(field_ids)
            }
            Self::FieldIds(field_ids) => {
                for field_id in field_ids {
                    if is_metadata_field(*field_id) {
                        continue;
                    }
                    Self::ensure_top_level_field(
                        schema,
                        *field_id,
                        &field_id.to_string(),
                    )?;
                }
                Ok(field_ids.clone())
            }
        }
    }

    fn ensure_top_level_field(
        schema: &crate::spec::Schema,
        field_id: i32,
        label: &str,
    ) -> Result<()> {
        schema.as_struct().field_by_id(field_id).ok_or_else(|| {
            if schema.field_by_id(field_id).is_some() {
                Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!(
                        "Column {label} is not a direct child of schema but a nested field, which is not supported now. Schema: {schema}"
                    ),
                )
            } else {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Column {label} not found in table. Schema: {schema}"),
                )
            }
        })?;
        Ok(())
    }
}

/// Table scan.
#[derive(Debug)]
pub struct TableScan {
    /// A [PlanContext], if this table has a committed snapshot or an overlay
    /// delta, otherwise None.
    ///
    /// If this is None, then the scan contains no rows.
    plan_context: Option<PlanContext>,
    delta: Option<Arc<SnapshotDelta>>,
    batch_size: Option<usize>,
    file_io: FileIO,
    column_names: Option<Vec<String>>,
    /// The maximum number of manifest files that will be
    /// retrieved from [`FileIO`] concurrently
    #[allow(dead_code)]
    concurrency_limit_manifest_files: usize,

    /// The maximum number of [`ManifestEntry`]s that will
    /// be processed in parallel
    #[allow(dead_code)]
    concurrency_limit_manifest_entries: usize,

    /// The maximum number of [`ManifestEntry`]s that will
    /// be processed in parallel
    concurrency_limit_data_files: usize,

    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
}

enum FilePlanAccumulator {
    Tasks(Vec<FileScanTask>),
    Maintenance(Vec<PlannedFileScanTask>),
}

impl FilePlanAccumulator {
    fn new(retain_data_files: bool) -> Self {
        if retain_data_files {
            Self::Maintenance(Vec::new())
        } else {
            Self::Tasks(Vec::new())
        }
    }

    fn push(&mut self, entry: ManifestEntryContext) -> Result<()> {
        match self {
            Self::Tasks(tasks) => tasks.push(entry.into_file_scan_task()?),
            Self::Maintenance(files) => {
                files.push(entry.into_planned_file_scan_task()?)
            }
        }
        Ok(())
    }
}

impl TableScan {
    /// Returns a list of [`FileScanTask`]s.
    pub fn plan_files(&self) -> Result<Vec<FileScanTask>> {
        let (files, _) = self.plan_files_internal(false)?;
        match files {
            FilePlanAccumulator::Tasks(tasks) => Ok(tasks),
            FilePlanAccumulator::Maintenance(_) => unreachable!(),
        }
    }

    /// Plans the current snapshot once while retaining the exact source
    /// [`crate::spec::DataFile`] values required by metadata-maintenance actions.
    pub fn plan_files_with_data_files(&self) -> Result<FileScanPlan> {
        let (files, manifest_count) = self.plan_files_internal(true)?;
        let FilePlanAccumulator::Maintenance(files) = files else {
            unreachable!()
        };
        Ok(FileScanPlan {
            files,
            manifest_count,
        })
    }

    fn plan_files_internal(
        &self,
        retain_data_files: bool,
    ) -> Result<(FilePlanAccumulator, usize)> {
        let Some(plan_context) = self.plan_context.as_ref() else {
            return Ok((FilePlanAccumulator::new(retain_data_files), 0));
        };

        let resolved_delta = self.delta.as_ref().map(|delta| delta.resolve());
        let manifest_list = plan_context.get_manifest_list()?;
        let manifest_count = manifest_list
            .as_ref()
            .map_or(0, |manifest_list| manifest_list.entries().len());

        // 0. Build manifest contexts
        let (mut data_manifest_contexts, delete_manifest_contexts) =
            match manifest_list {
                Some(manifest_list) => {
                    plan_context.build_manifest_file_contexts(manifest_list)?
                }
                None => (Vec::new(), Vec::new()),
            };

        // 1. Collect all delete files using builder
        let builder = DeleteFileIndexBuilder::new();
        for ctx in delete_manifest_contexts {
            let entries = ctx.fetch_manifest_entries()?;
            for entry in entries {
                if let Some(delete_ctx) = Self::process_delete_manifest_entry(entry)?
                {
                    if resolved_delta.as_ref().is_some_and(|delta| {
                        !delta.retains_manifest_entry(
                            ManifestContentType::Deletes,
                            delete_ctx.manifest_entry.as_ref(),
                        )
                    }) {
                        continue;
                    }
                    builder.insert(delete_ctx)?;
                }
            }
        }
        if let Some(delta) = resolved_delta.as_ref() {
            let base_sequence_number = plan_context.base_sequence_number();
            for delete_ctx in delta.delete_file_contexts(base_sequence_number)? {
                builder.insert(delete_ctx)?;
            }
        }

        // 2. Build the immutable index and distribute to data manifest contexts
        let delete_file_index = builder.build();
        let mut file_scan_tasks = FilePlanAccumulator::new(retain_data_files);

        for ctx in &mut data_manifest_contexts {
            // Clone is cheap: only increments Arc reference count
            ctx.delete_file_index = Some(delete_file_index.clone());

            let entries = ctx.fetch_manifest_entries()?;
            for entry in entries {
                if resolved_delta.as_ref().is_some_and(|delta| {
                    !delta.retains_manifest_entry(
                        ManifestContentType::Data,
                        entry.manifest_entry.as_ref(),
                    )
                }) {
                    continue;
                }
                if let Some(entry) = Self::process_data_manifest_entry(entry)? {
                    file_scan_tasks.push(entry)?;
                }
            }
        }

        if let Some(delta) = resolved_delta.as_ref() {
            let base_sequence_number = plan_context.base_sequence_number();
            for manifest_entry in delta.data_manifest_entries(base_sequence_number)? {
                let partition_spec_id = manifest_entry.data_file().partition_spec_id;
                let entry_context = plan_context
                    .create_delta_manifest_entry_context(
                        manifest_entry,
                        partition_spec_id,
                        Some(delete_file_index.clone()),
                    )?;
                if let Some(entry) = Self::process_data_manifest_entry(entry_context)?
                {
                    file_scan_tasks.push(entry)?;
                }
            }
        }

        Ok((file_scan_tasks, manifest_count))
    }

    /// Returns an [`ArrowRecordBatchIterator`].
    pub fn to_arrow(&self) -> Result<ArrowRecordBatchIterator> {
        let tasks = self.plan_files()?;
        self.to_arrow_with_tasks(tasks)
    }

    /// Returns an [`ArrowRecordBatchIterator`] for a pre-planned task list.
    pub fn to_arrow_with_tasks(
        &self,
        tasks: Vec<FileScanTask>,
    ) -> Result<ArrowRecordBatchIterator> {
        let mut arrow_reader_builder = ArrowReaderBuilder::new(self.file_io.clone())
            .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
            .with_row_group_filtering_enabled(self.row_group_filtering_enabled)
            .with_row_selection_enabled(self.row_selection_enabled);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        arrow_reader_builder.build().read(tasks)
    }

    /// Read pre-planned tasks and validate every manifest record count against
    /// the corresponding Parquet footer before yielding rows.
    ///
    /// The check belongs to the file-reader boundary: delete filtering may
    /// change logical visibility, but it must not explain a mismatch in the
    /// file's physical cardinality.
    pub fn to_arrow_with_validated_tasks(
        &self,
        tasks: Vec<FileScanTask>,
    ) -> Result<ArrowRecordBatchIterator> {
        let mut arrow_reader_builder = ArrowReaderBuilder::new(self.file_io.clone())
            .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
            .with_row_group_filtering_enabled(self.row_group_filtering_enabled)
            .with_row_selection_enabled(self.row_selection_enabled);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        arrow_reader_builder
            .build()
            .read_with_record_count_validation(tasks)
    }

    /// Returns an [`ArrowRecordBatchIterator`] for a pre-planned task list,
    /// replacing each task's reader predicate with `filter`.
    ///
    /// This lets callers cache file planning based on a stable pruning
    /// predicate and later apply the current row predicate without rebuilding
    /// manifests or delete indexes.
    pub fn to_arrow_with_tasks_and_filter(
        &self,
        mut tasks: Vec<FileScanTask>,
        filter: Option<Predicate>,
    ) -> Result<ArrowRecordBatchIterator> {
        let bound_filter = match (filter, self.plan_context.as_ref()) {
            (Some(filter), Some(plan_context)) => Some(
                filter
                    .rewrite_not()
                    .bind(plan_context.snapshot_schema.clone(), true)?,
            ),
            (Some(_), None) | (None, _) => None,
        };

        for task in &mut tasks {
            task.set_predicate(bound_filter.clone());
        }

        self.to_arrow_with_tasks(tasks)
    }

    /// Returns an [`ArrowRecordBatchIterator`] for a shared pre-planned task
    /// list, replacing each task's reader predicate with `filter` without
    /// cloning or mutating the cached tasks.
    pub fn to_arrow_with_shared_tasks_and_filter(
        &self,
        tasks: Arc<[FileScanTask]>,
        filter: Option<Predicate>,
    ) -> Result<ArrowRecordBatchIterator> {
        let bound_filter = match (filter, self.plan_context.as_ref()) {
            (Some(filter), Some(plan_context)) => Some(
                filter
                    .rewrite_not()
                    .bind(plan_context.snapshot_schema.clone(), true)?,
            ),
            (Some(_), None) | (None, _) => None,
        };

        let mut arrow_reader_builder = ArrowReaderBuilder::new(self.file_io.clone())
            .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
            .with_row_group_filtering_enabled(self.row_group_filtering_enabled)
            .with_row_selection_enabled(self.row_selection_enabled);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        arrow_reader_builder
            .build()
            .read_shared_with_filter(tasks, bound_filter)
    }

    /// Read validated physical-position requests with this scan's reader
    /// configuration. Requests must have been built from tasks planned by the
    /// same scan so schema, projection, predicates, deletes, and transaction
    /// overlay remain aligned.
    pub fn to_arrow_with_selected_rows(
        &self,
        requests: Vec<SelectedRowsReadRequest>,
    ) -> Result<ArrowRecordBatchIterator> {
        let mut arrow_reader_builder = ArrowReaderBuilder::new(self.file_io.clone())
            .with_data_file_concurrency_limit(self.concurrency_limit_data_files)
            .with_row_group_filtering_enabled(self.row_group_filtering_enabled)
            .with_row_selection_enabled(true);

        if let Some(batch_size) = self.batch_size {
            arrow_reader_builder = arrow_reader_builder.with_batch_size(batch_size);
        }

        arrow_reader_builder.build().read_selected_rows(requests)
    }

    /// Returns a reference to the column names of the table scan.
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    /// Returns a reference to the snapshot of the table scan.
    pub fn snapshot(&self) -> Option<&SnapshotRef> {
        self.plan_context.as_ref().and_then(|x| x.snapshot.as_ref())
    }

    fn process_data_manifest_entry(
        manifest_entry_context: ManifestEntryContext,
    ) -> Result<Option<ManifestEntryContext>> {
        // skip processing this manifest entry if it has been marked as deleted
        if !manifest_entry_context.manifest_entry.is_alive() {
            return Ok(None);
        }

        // abort the plan if we encounter a manifest entry for a delete file
        if manifest_entry_context.manifest_entry.content_type()
            != DataContentType::Data
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a delete file in a data file manifest",
            ));
        }

        if let Some(ref bound_predicates) = manifest_entry_context.bound_predicates {
            let BoundPredicates {
                snapshot_bound_predicate,
                partition_bound_predicate,
            } = bound_predicates.as_ref();

            let expression_evaluator_cache =
                manifest_entry_context.expression_evaluator_cache.as_ref();

            let expression_evaluator = expression_evaluator_cache.get(
                manifest_entry_context.partition_spec_id,
                partition_bound_predicate,
            )?;

            // skip any data file whose partition data indicates that it can't contain
            // any data that matches this scan's filter
            if !expression_evaluator
                .eval(manifest_entry_context.manifest_entry.data_file())?
            {
                return Ok(None);
            }

            // skip any data file whose metrics don't match this scan's filter
            if !InclusiveMetricsEvaluator::eval(
                snapshot_bound_predicate,
                manifest_entry_context.manifest_entry.data_file(),
                false,
            )? {
                return Ok(None);
            }
        }

        Ok(Some(manifest_entry_context))
    }

    fn process_delete_manifest_entry(
        manifest_entry_context: ManifestEntryContext,
    ) -> Result<Option<DeleteFileContext>> {
        // skip processing this manifest entry if it has been marked as deleted
        if !manifest_entry_context.manifest_entry.is_alive() {
            return Ok(None);
        }

        // abort the plan if we encounter a manifest entry that is not for a delete file
        if manifest_entry_context.manifest_entry.content_type()
            == DataContentType::Data
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Encountered an entry for a data file in a delete manifest",
            ));
        }

        if let Some(ref bound_predicates) = manifest_entry_context.bound_predicates {
            let expression_evaluator_cache =
                manifest_entry_context.expression_evaluator_cache.as_ref();

            let expression_evaluator = expression_evaluator_cache.get(
                manifest_entry_context.partition_spec_id,
                &bound_predicates.partition_bound_predicate,
            )?;

            // skip any data file whose partition data indicates that it can't contain
            // any data that matches this scan's filter
            if !expression_evaluator
                .eval(manifest_entry_context.manifest_entry.data_file())?
            {
                return Ok(None);
            }
        }

        Ok(Some(DeleteFileContext {
            manifest_entry: manifest_entry_context.manifest_entry.clone(),
            partition_spec_id: manifest_entry_context.partition_spec_id,
        }))
    }
}

pub(crate) struct BoundPredicates {
    partition_bound_predicate: BoundPredicate,
    snapshot_bound_predicate: BoundPredicate,
}

#[cfg(test)]
pub mod tests {
    //! shared tests for the table scan API
    #![allow(missing_docs)]

    use std::collections::HashMap;
    use std::fs;
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array,
        RecordBatch, StringArray,
    };

    use minijinja::value::Value;
    use minijinja::{AutoEscape, Environment, context};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::Result;
    use crate::TableIdent;
    use crate::arrow::{
        ArrowReaderBuilder, PhysicalRowReadContext, SelectedRowsReadRequest,
    };
    use crate::expr::{BoundPredicate, Reference};
    use crate::io::{FileIO, OutputFile};
    use crate::metadata_columns::{
        RESERVED_COL_NAME_FILE, RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_POS,
    };
    use crate::overlay::SnapshotDelta;
    use crate::scan::FileScanTask;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal,
        ManifestEntry, ManifestListWriter, ManifestStatus, ManifestWriterBuilder,
        NestedField, PartitionSpec, PrimitiveType, Schema, Struct, StructType,
        TableMetadata, Type,
    };
    use crate::table::Table;
    use crate::transaction::{AddColumn, Transaction};

    fn render_template(template: &str, ctx: Value) -> String {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.render_str(template, ctx).unwrap()
    }

    pub struct TableTestFixture {
        pub table_location: String,
        pub table: Table,
    }

    impl TableTestFixture {
        #[allow(clippy::new_without_default)]
        pub fn new() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location =
                table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location =
                table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io =
                FileIO::from_path(table_location.as_os_str().to_str().unwrap())
                    .unwrap();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let metadata_json = render_template(
                    &template_json_str,
                    context! {
                        table_location => &table_location,
                        manifest_list_1_location => &manifest_list1_location,
                        manifest_list_2_location => &manifest_list2_location,
                        table_metadata_1_location => &table_metadata1_location,
                    },
                );
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(
                    table_metadata1_location.as_os_str().to_str().unwrap(),
                )
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        #[allow(clippy::new_without_default)]
        pub fn new_empty() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io =
                FileIO::from_path(table_location.as_os_str().to_str().unwrap())
                    .unwrap();

            let table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_empty_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let metadata_json = render_template(
                    &template_json_str,
                    context! {
                        table_location => &table_location,
                        table_metadata_1_location => &table_metadata1_location,
                    },
                );
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(
                    table_metadata1_location.as_os_str().to_str().unwrap(),
                )
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        pub fn new_unpartitioned() -> Self {
            let tmp_dir = TempDir::new().unwrap();
            let table_location = tmp_dir.path().join("table1");
            let manifest_list1_location =
                table_location.join("metadata/manifests_list_1.avro");
            let manifest_list2_location =
                table_location.join("metadata/manifests_list_2.avro");
            let table_metadata1_location = table_location.join("metadata/v1.json");

            let file_io =
                FileIO::from_path(table_location.to_str().unwrap()).unwrap();

            let mut table_metadata = {
                let template_json_str = fs::read_to_string(format!(
                    "{}/testdata/example_table_metadata_v2.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                let metadata_json = render_template(
                    &template_json_str,
                    context! {
                        table_location => &table_location,
                        manifest_list_1_location => &manifest_list1_location,
                        manifest_list_2_location => &manifest_list2_location,
                        table_metadata_1_location => &table_metadata1_location,
                    },
                );
                serde_json::from_str::<TableMetadata>(&metadata_json).unwrap()
            };

            table_metadata.default_spec = Arc::new(PartitionSpec::unpartition_spec());
            table_metadata.partition_specs.clear();
            table_metadata.default_partition_type = StructType::new(vec![]);
            table_metadata
                .partition_specs
                .insert(0, table_metadata.default_spec.clone());

            let table = Table::builder()
                .metadata(table_metadata)
                .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
                .file_io(file_io.clone())
                .metadata_location(table_metadata1_location.to_str().unwrap())
                .build()
                .unwrap();

            Self {
                table_location: table_location.to_str().unwrap().to_string(),
                table,
            }
        }

        fn next_manifest_file(&self) -> OutputFile {
            self.table
                .file_io()
                .new_output(format!(
                    "{}/metadata/manifest_{}.avro",
                    self.table_location,
                    Uuid::new_v4()
                ))
                .unwrap()
        }

        pub fn setup_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let parent_snapshot = current_snapshot
                .parent_snapshot(self.table.metadata())
                .unwrap();
            let current_schema =
                current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec =
                self.table.metadata().default_partition_spec();

            // Write data files
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                None,
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();
            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!(
                                    "{}/1.parquet",
                                    &self.table_location
                                ))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(0)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(
                                    100,
                                ))]))
                                .key_metadata(None)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            writer
                .add_delete_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!(
                                    "{}/2.parquet",
                                    &self.table_location
                                ))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(0)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(
                                    200,
                                ))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            writer
                .add_existing_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!(
                                    "{}/3.parquet",
                                    &self.table_location
                                ))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(0)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(
                                    300,
                                ))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            let data_file_manifest = writer.write_manifest_file().unwrap();

            // Write to manifest list
            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap()
                    .create_file_writer()
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().unwrap();

            // prepare data
            let schema = {
                let fields = vec![
                    arrow_schema::Field::new(
                        "x",
                        arrow_schema::DataType::Int64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "1".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "y",
                        arrow_schema::DataType::Int64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "z",
                        arrow_schema::DataType::Int64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "3".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "a",
                        arrow_schema::DataType::Utf8,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "4".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "dbl",
                        arrow_schema::DataType::Float64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "5".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "i32",
                        arrow_schema::DataType::Int32,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "6".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "i64",
                        arrow_schema::DataType::Int64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "7".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "bool",
                        arrow_schema::DataType::Boolean,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "8".to_string(),
                    )])),
                ];
                Arc::new(arrow_schema::Schema::new(fields))
            };
            // x: [1, 1, 1, 1, ...]
            let col1 =
                Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;

            let mut values = vec![2; 512];
            values.append(vec![3; 200].as_mut());
            values.append(vec![4; 300].as_mut());
            values.append(vec![5; 12].as_mut());

            // y: [2, 2, 2, 2, ..., 3, 3, 3, 3, ..., 4, 4, 4, 4, ..., 5, 5, 5, 5]
            let col2 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![3; 512];
            values.append(vec![4; 512].as_mut());

            // z: [3, 3, 3, 3, ..., 4, 4, 4, 4]
            let col3 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            // a: ["Apache", "Apache", "Apache", ..., "Iceberg", "Iceberg", "Iceberg"]
            let mut values = vec!["Apache"; 512];
            values.append(vec!["Iceberg"; 512].as_mut());
            let col4 = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;

            // dbl:
            let mut values = vec![100.0f64; 512];
            values.append(vec![150.0f64; 12].as_mut());
            values.append(vec![200.0f64; 500].as_mut());
            let col5 = Arc::new(Float64Array::from_iter_values(values)) as ArrayRef;

            // i32:
            let mut values = vec![100i32; 512];
            values.append(vec![150i32; 12].as_mut());
            values.append(vec![200i32; 500].as_mut());
            let col6 = Arc::new(Int32Array::from_iter_values(values)) as ArrayRef;

            // i64:
            let mut values = vec![100i64; 512];
            values.append(vec![150i64; 12].as_mut());
            values.append(vec![200i64; 500].as_mut());
            let col7 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            // bool:
            let mut values = vec![false; 512];
            values.append(vec![true; 512].as_mut());
            let values: BooleanArray = values.into();
            let col8 = Arc::new(values) as ArrayRef;

            let to_write = RecordBatch::try_new(
                schema.clone(),
                vec![col1, col2, col3, col4, col5, col6, col7, col8],
            )
            .unwrap();

            // Write the Parquet files
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            for n in 1..=3 {
                let file =
                    File::create(format!("{}/{}.parquet", &self.table_location, n))
                        .unwrap();
                let mut writer = ArrowWriter::try_new(
                    file,
                    to_write.schema(),
                    Some(props.clone()),
                )
                .unwrap();

                writer.write(&to_write).expect("Writing batch");

                // writer must be closed to write footer
                writer.close().unwrap();
            }
        }

        pub fn setup_unpartitioned_manifest_files(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let parent_snapshot = current_snapshot
                .parent_snapshot(self.table.metadata())
                .unwrap();
            let current_schema =
                current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec = Arc::new(PartitionSpec::unpartition_spec());

            // Write data files using an empty partition for unpartitioned tables.
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                None,
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();

            // Create an empty partition value.
            let empty_partition = Struct::empty();

            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!(
                                    "{}/1.parquet",
                                    &self.table_location
                                ))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(0)
                                .record_count(1)
                                .partition(empty_partition.clone())
                                .key_metadata(None)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();

            writer
                .add_delete_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Deleted)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!(
                                    "{}/2.parquet",
                                    &self.table_location
                                ))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(0)
                                .record_count(1)
                                .partition(empty_partition.clone())
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();

            writer
                .add_existing_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Existing)
                        .snapshot_id(parent_snapshot.snapshot_id())
                        .sequence_number(parent_snapshot.sequence_number())
                        .file_sequence_number(parent_snapshot.sequence_number())
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::Data)
                                .file_path(format!(
                                    "{}/3.parquet",
                                    &self.table_location
                                ))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(0)
                                .record_count(1)
                                .partition(empty_partition.clone())
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();

            let data_file_manifest = writer.write_manifest_file().unwrap();

            // Write to manifest list
            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap()
                    .create_file_writer()
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_file_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().unwrap();

            // prepare data for parquet files
            let schema = {
                let fields = vec![
                    arrow_schema::Field::new(
                        "x",
                        arrow_schema::DataType::Int64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "1".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "y",
                        arrow_schema::DataType::Int64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "2".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "z",
                        arrow_schema::DataType::Int64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "3".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "a",
                        arrow_schema::DataType::Utf8,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "4".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "dbl",
                        arrow_schema::DataType::Float64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "5".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "i32",
                        arrow_schema::DataType::Int32,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "6".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "i64",
                        arrow_schema::DataType::Int64,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "7".to_string(),
                    )])),
                    arrow_schema::Field::new(
                        "bool",
                        arrow_schema::DataType::Boolean,
                        false,
                    )
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        "8".to_string(),
                    )])),
                ];
                Arc::new(arrow_schema::Schema::new(fields))
            };

            // Build the arrays for the RecordBatch
            let col1 =
                Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;

            let mut values = vec![2; 512];
            values.append(vec![3; 200].as_mut());
            values.append(vec![4; 300].as_mut());
            values.append(vec![5; 12].as_mut());
            let col2 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![3; 512];
            values.append(vec![4; 512].as_mut());
            let col3 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec!["Apache"; 512];
            values.append(vec!["Iceberg"; 512].as_mut());
            let col4 = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;

            let mut values = vec![100.0f64; 512];
            values.append(vec![150.0f64; 12].as_mut());
            values.append(vec![200.0f64; 500].as_mut());
            let col5 = Arc::new(Float64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![100i32; 512];
            values.append(vec![150i32; 12].as_mut());
            values.append(vec![200i32; 500].as_mut());
            let col6 = Arc::new(Int32Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![100i64; 512];
            values.append(vec![150i64; 12].as_mut());
            values.append(vec![200i64; 500].as_mut());
            let col7 = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;

            let mut values = vec![false; 512];
            values.append(vec![true; 512].as_mut());
            let values: BooleanArray = values.into();
            let col8 = Arc::new(values) as ArrayRef;

            let to_write = RecordBatch::try_new(
                schema.clone(),
                vec![col1, col2, col3, col4, col5, col6, col7, col8],
            )
            .unwrap();

            // Write the Parquet files
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            for n in 1..=3 {
                let file =
                    File::create(format!("{}/{}.parquet", &self.table_location, n))
                        .unwrap();
                let mut writer = ArrowWriter::try_new(
                    file,
                    to_write.schema(),
                    Some(props.clone()),
                )
                .unwrap();

                writer.write(&to_write).expect("Writing batch");

                // writer must be closed to write footer
                writer.close().unwrap();
            }
        }

        pub fn setup_deadlock_manifests(&mut self) {
            let current_snapshot = self.table.metadata().current_snapshot().unwrap();
            let _parent_snapshot = current_snapshot
                .parent_snapshot(self.table.metadata())
                .unwrap();
            let current_schema =
                current_snapshot.schema(self.table.metadata()).unwrap();
            let current_partition_spec =
                self.table.metadata().default_partition_spec();

            // 1. Write DATA manifest with MULTIPLE entries to fill buffer
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                None,
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_data();

            // Add 10 data entries
            for i in 0..10 {
                writer
                    .add_entry(
                        ManifestEntry::builder()
                            .status(ManifestStatus::Added)
                            .data_file(
                                DataFileBuilder::default()
                                    .partition_spec_id(0)
                                    .content(DataContentType::Data)
                                    .file_path(format!(
                                        "{}/{}.parquet",
                                        &self.table_location, i
                                    ))
                                    .file_format(DataFileFormat::Parquet)
                                    .file_size_in_bytes(0)
                                    .record_count(1)
                                    .partition(Struct::from_iter([Some(
                                        Literal::long(100),
                                    )]))
                                    .key_metadata(None)
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                    )
                    .unwrap();
            }
            let data_manifest = writer.write_manifest_file().unwrap();

            // 2. Write DELETE manifest
            let mut writer = ManifestWriterBuilder::new(
                self.next_manifest_file(),
                Some(current_snapshot.snapshot_id()),
                None,
                current_schema.clone(),
                current_partition_spec.as_ref().clone(),
            )
            .build_v2_deletes();

            writer
                .add_entry(
                    ManifestEntry::builder()
                        .status(ManifestStatus::Added)
                        .data_file(
                            DataFileBuilder::default()
                                .partition_spec_id(0)
                                .content(DataContentType::PositionDeletes)
                                .file_path(format!(
                                    "{}/del.parquet",
                                    &self.table_location
                                ))
                                .file_format(DataFileFormat::Parquet)
                                .file_size_in_bytes(0)
                                .record_count(1)
                                .partition(Struct::from_iter([Some(Literal::long(
                                    100,
                                ))]))
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .unwrap();
            let delete_manifest = writer.write_manifest_file().unwrap();

            // Write to manifest list - DATA FIRST then DELETE
            // This order is crucial for reproduction
            let mut manifest_list_write = ManifestListWriter::v2(
                self.table
                    .file_io()
                    .new_output(current_snapshot.manifest_list())
                    .unwrap()
                    .create_file_writer()
                    .unwrap(),
                current_snapshot.snapshot_id(),
                current_snapshot.parent_snapshot_id(),
                current_snapshot.sequence_number(),
            );
            manifest_list_write
                .add_manifests(vec![data_manifest, delete_manifest].into_iter())
                .unwrap();
            manifest_list_write.close().unwrap();
        }
    }

    #[test]
    fn test_table_scan_columns() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select(["x", "y"]).build().unwrap();
        assert_eq!(
            Some(vec!["x".to_string(), "y".to_string()]),
            table_scan.column_names
        );

        let table_scan = table
            .scan()
            .select(["x", "y"])
            .select(["z"])
            .build()
            .unwrap();
        assert_eq!(Some(vec!["z".to_string()]), table_scan.column_names);
    }

    #[test]
    fn test_select_all() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select_all().build().unwrap();
        assert!(table_scan.column_names.is_none());
    }

    #[test]
    fn test_select_no_exist_column() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().select(["x", "y", "z", "a", "b"]).build();
        assert!(table_scan.is_err());
    }

    #[test]
    fn test_table_scan_default_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().build().unwrap();
        assert_eq!(
            table.metadata().current_snapshot().unwrap().snapshot_id(),
            table_scan.snapshot().unwrap().snapshot_id()
        );
    }

    #[test]
    fn test_table_scan_non_exist_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table.scan().snapshot_id(1024).build();
        assert!(table_scan.is_err());
    }

    #[test]
    fn test_table_scan_with_snapshot_id() {
        let table = TableTestFixture::new().table;

        let table_scan = table
            .scan()
            .snapshot_id(3051729675574597004)
            .with_row_selection_enabled(true)
            .build()
            .unwrap();
        assert_eq!(
            table_scan.snapshot().unwrap().snapshot_id(),
            3051729675574597004
        );
    }

    #[test]
    fn test_plan_files_on_table_without_any_snapshots() {
        let table = TableTestFixture::new_empty().table;
        let batch_iterator = table.scan().build().unwrap().to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_plan_files_on_empty_table_with_delta() {
        let fixture = TableTestFixture::new_empty();
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(
                DataFileBuilder::default()
                    .partition_spec_id(0)
                    .content(DataContentType::Data)
                    .file_path(format!("{}/pending.parquet", fixture.table_location))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(0)
                    .record_count(1)
                    .partition(Struct::empty())
                    .build()
                    .unwrap(),
            )
            .unwrap();

        let tasks = fixture
            .table
            .scan()
            .with_delta(Arc::new(delta))
            .build()
            .unwrap()
            .plan_files()
            .unwrap();

        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].data_file_path,
            format!("{}/pending.parquet", fixture.table_location)
        );
    }

    #[test]
    fn test_plan_files_delta_prunes_by_file_metrics() {
        let fixture = TableTestFixture::new_empty();
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(delta_data_file_with_y_bounds(
                &fixture,
                "pending-pruned.parquet",
                100,
                10,
                20,
            ))
            .unwrap();
        delta
            .add_data_file(delta_data_file_with_y_bounds(
                &fixture,
                "pending-kept.parquet",
                100,
                1,
                2,
            ))
            .unwrap();

        let predicate = Reference::new("y").less_than(Datum::long(3));
        let tasks = fixture
            .table
            .scan()
            .with_delta(Arc::new(delta))
            .with_filter(predicate)
            .build()
            .unwrap()
            .plan_files()
            .unwrap();

        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].data_file_path.ends_with("pending-kept.parquet"));
        assert!(tasks[0].predicate.is_some());
    }

    #[test]
    fn test_plan_files_delta_prunes_by_partition() {
        let fixture = TableTestFixture::new_empty();
        let mut delta = SnapshotDelta::new();
        delta
            .add_data_file(delta_data_file_with_y_bounds(
                &fixture,
                "pending-wrong-partition.parquet",
                100,
                1,
                2,
            ))
            .unwrap();

        let predicate = Reference::new("x").equal_to(Datum::long(200));
        let tasks = fixture
            .table
            .scan()
            .with_delta(Arc::new(delta))
            .with_filter(predicate)
            .build()
            .unwrap()
            .plan_files()
            .unwrap();

        assert!(tasks.is_empty());
    }

    #[test]
    fn test_plan_files_no_deletions() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Create table scan for current snapshot and plan files
        let table_scan = fixture
            .table
            .scan()
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let mut tasks = table_scan.plan_files().unwrap();

        assert_eq!(tasks.len(), 2);

        tasks.sort_by_key(|t| t.data_file_path.to_string());

        // Check first task is added data file
        assert_eq!(
            tasks[0].data_file_path,
            format!("{}/1.parquet", &fixture.table_location)
        );

        // Check second task is existing data file
        assert_eq!(
            tasks[1].data_file_path,
            format!("{}/3.parquet", &fixture.table_location)
        );
    }

    #[test]
    fn test_open_parquet_no_deletions() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Create table scan for current snapshot and plan files
        let table_scan = fixture
            .table
            .scan()
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        let col = batches[0].column_by_name("x").unwrap();

        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);
    }

    #[test]
    fn test_physical_row_read_returns_requested_row() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();
        let data_file_path = format!("{}/1.parquet", fixture.table_location);
        let read_context =
            PhysicalRowReadContext::try_new(fixture.table.metadata()).unwrap();
        let request = read_context
            .read_row(&data_file_path, 700, vec![2])
            .unwrap();

        let batch = fixture
            .table
            .reader_builder()
            .build()
            .read_physical_row(request)
            .unwrap()
            .unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 3);

        let out_of_bounds = read_context
            .read_row(&data_file_path, 1024, vec![2])
            .unwrap();
        assert!(
            fixture
                .table
                .reader_builder()
                .build()
                .read_physical_row(out_of_bounds)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_selected_rows_are_batched_with_position_metadata() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();
        let data_file_path = format!("{}/1.parquet", fixture.table_location);
        let table_scan = fixture.table.scan().build().unwrap();
        let mut task = table_scan
            .plan_files()
            .unwrap()
            .into_iter()
            .find(|task| task.data_file_path == data_file_path)
            .unwrap();
        task.project_field_ids = vec![2, RESERVED_FIELD_ID_POS];
        // This legacy fixture uses a manifest count of one while writing
        // 1,024 physical rows. Selected-row requests intentionally validate
        // that count against the footer, so make this request internally
        // consistent instead of weakening production validation.
        task.record_count = Some(1_024);
        let record_count = task.record_count.unwrap();
        let requested_positions = vec![0, record_count / 2, record_count - 1];
        let request =
            SelectedRowsReadRequest::try_new(task, requested_positions.clone())
                .unwrap();

        let batches = fixture
            .table
            .reader_builder()
            .with_row_selection_enabled(true)
            .build()
            .read_selected_rows(vec![request])
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let positions = batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column_by_name(RESERVED_COL_NAME_POS)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                values.values().iter().copied().collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(rows, 3);
        assert_eq!(
            positions,
            requested_positions
                .into_iter()
                .map(|position| i64::try_from(position).unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_open_parquet_no_deletions_by_separate_reader() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Create table scan for current snapshot and plan files
        let table_scan = fixture
            .table
            .scan()
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let mut plan_task: Vec<_> = table_scan.plan_files().unwrap();
        assert_eq!(plan_task.len(), 2);

        let reader = ArrowReaderBuilder::new(fixture.table.file_io().clone()).build();
        let batch_iterator = reader.clone().read(vec![plan_task.remove(0)]).unwrap();
        let batch_1: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        let reader = ArrowReaderBuilder::new(fixture.table.file_io().clone()).build();
        let batch_iterator = reader.read(vec![plan_task.remove(0)]).unwrap();
        let batch_2: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batch_1, batch_2);
    }

    #[test]
    fn test_open_parquet_with_projection() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Create table scan for current snapshot and plan files
        let table_scan = fixture
            .table
            .scan()
            .select(["x", "z"])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_columns(), 2);

        let col1 = batches[0].column_by_name("x").unwrap();
        let int64_arr = col1.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);

        let col2 = batches[0].column_by_name("z").unwrap();
        let int64_arr = col2.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 3);

        // test empty scan
        let table_scan = fixture.table.scan().select_empty().build().unwrap();
        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(batches[0].num_rows(), 1024);
    }

    fn delta_data_file_with_y_bounds(
        fixture: &TableTestFixture,
        file_name: &str,
        partition_x: i64,
        lower_y: i64,
        upper_y: i64,
    ) -> crate::spec::DataFile {
        DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path(format!("{}/{}", fixture.table_location, file_name))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(0)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(partition_x))]))
            .value_counts(HashMap::from([(2, 1)]))
            .null_value_counts(HashMap::from([(2, 0)]))
            .lower_bounds(HashMap::from([(2, Datum::long(lower_y))]))
            .upper_bounds(HashMap::from([(2, Datum::long(upper_y))]))
            .build()
            .unwrap()
    }

    #[test]
    fn test_filter_on_arrow_lt() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Filter: y < 3
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y").less_than(Datum::long(3));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("x").unwrap();
        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);

        let col = batches[0].column_by_name("y").unwrap();
        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 2);
    }

    #[test]
    fn test_filter_on_arrow_gt_eq() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Filter: y >= 5
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y").greater_than_or_equal_to(Datum::long(5));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_rows(), 12);

        let col = batches[0].column_by_name("x").unwrap();
        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 1);

        let col = batches[0].column_by_name("y").unwrap();
        let int64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int64_arr.value(0), 5);
    }

    #[test]
    fn test_filter_double_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: dbl == 150.0
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("dbl").equal_to(Datum::double(150.0f64));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 12);

        let col = batches[0].column_by_name("dbl").unwrap();
        let f64_arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(f64_arr.value(1), 150.0f64);
    }

    #[test]
    fn test_filter_int_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: i32 == 150
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("i32").equal_to(Datum::int(150i32));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 12);

        let col = batches[0].column_by_name("i32").unwrap();
        let i32_arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(i32_arr.value(1), 150i32);
    }

    #[test]
    fn test_filter_long_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: i64 == 150
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("i64").equal_to(Datum::long(150i64));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 12);

        let col = batches[0].column_by_name("i64").unwrap();
        let i64_arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(i64_arr.value(1), 150i64);
    }

    #[test]
    fn test_filter_bool_eq() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: bool == true
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("bool").equal_to(Datum::bool(true));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("bool").unwrap();
        let bool_arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bool_arr.value(1));
    }

    #[test]
    fn test_filter_on_arrow_is_null() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: y is null
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y").is_null();
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches.len(), 0);
    }

    #[test]
    fn test_filter_on_arrow_is_not_null() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: y is not null
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y").is_not_null();
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches[0].num_rows(), 1024);
    }

    #[test]
    fn test_filter_on_arrow_lt_and_gt() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Filter: y < 5 AND z >= 4
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y")
            .less_than(Datum::long(5))
            .and(Reference::new("z").greater_than_or_equal_to(Datum::long(4)));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches[0].num_rows(), 500);

        let col = batches[0].column_by_name("x").unwrap();
        let expected_x =
            Arc::new(Int64Array::from_iter_values(vec![1; 500])) as ArrayRef;
        assert_eq!(col, &expected_x);

        let col = batches[0].column_by_name("y").unwrap();
        let mut values = vec![];
        values.append(vec![3; 200].as_mut());
        values.append(vec![4; 300].as_mut());
        let expected_y = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;
        assert_eq!(col, &expected_y);

        let col = batches[0].column_by_name("z").unwrap();
        let expected_z =
            Arc::new(Int64Array::from_iter_values(vec![4; 500])) as ArrayRef;
        assert_eq!(col, &expected_z);
    }

    #[test]
    fn test_filter_on_arrow_lt_or_gt() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Filter: y < 5 AND z >= 4
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("y")
            .less_than(Datum::long(5))
            .or(Reference::new("z").greater_than_or_equal_to(Datum::long(4)));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(batches[0].num_rows(), 1024);

        let col = batches[0].column_by_name("x").unwrap();
        let expected_x =
            Arc::new(Int64Array::from_iter_values(vec![1; 1024])) as ArrayRef;
        assert_eq!(col, &expected_x);

        let col = batches[0].column_by_name("y").unwrap();
        let mut values = vec![2; 512];
        values.append(vec![3; 200].as_mut());
        values.append(vec![4; 300].as_mut());
        values.append(vec![5; 12].as_mut());
        let expected_y = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;
        assert_eq!(col, &expected_y);

        let col = batches[0].column_by_name("z").unwrap();
        let mut values = vec![3; 512];
        values.append(vec![4; 512].as_mut());
        let expected_z = Arc::new(Int64Array::from_iter_values(values)) as ArrayRef;
        assert_eq!(col, &expected_z);
    }

    #[test]
    fn test_filter_on_arrow_startswith() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: a STARTSWITH "Ice"
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("a").starts_with(Datum::string("Ice"));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("a").unwrap();
        let string_arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_arr.value(0), "Iceberg");
    }

    #[test]
    fn test_filter_on_arrow_not_startswith() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: a NOT STARTSWITH "Ice"
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("a").not_starts_with(Datum::string("Ice"));
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("a").unwrap();
        let string_arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_arr.value(0), "Apache");
    }

    #[test]
    fn test_filter_on_arrow_in() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: a IN ("Sioux", "Iceberg")
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("a")
            .is_in([Datum::string("Sioux"), Datum::string("Iceberg")]);
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("a").unwrap();
        let string_arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_arr.value(0), "Iceberg");
    }

    #[test]
    fn test_filter_on_arrow_not_in() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Filter: a NOT IN ("Sioux", "Iceberg")
        let mut builder = fixture.table.scan();
        let predicate = Reference::new("a")
            .is_not_in([Datum::string("Sioux"), Datum::string("Iceberg")]);
        builder = builder
            .with_filter(predicate)
            .with_row_selection_enabled(true);
        let table_scan = builder.build().unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();

        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_rows(), 512);

        let col = batches[0].column_by_name("a").unwrap();
        let string_arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_arr.value(0), "Apache");
    }

    #[test]
    fn test_file_scan_task_serialize_deserialize() {
        let test_fn = |task: FileScanTask| {
            let serialized = serde_json::to_string(&task).unwrap();
            let deserialized: FileScanTask =
                serde_json::from_str(&serialized).unwrap();

            assert_eq!(task.data_file_path, deserialized.data_file_path);
            assert_eq!(task.start, deserialized.start);
            assert_eq!(task.length, deserialized.length);
            assert_eq!(task.project_field_ids, deserialized.project_field_ids);
            assert_eq!(task.predicate, deserialized.predicate);
            assert_eq!(task.schema, deserialized.schema);
        };

        // without predicate
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "x",
                    Type::Primitive(PrimitiveType::Binary),
                ))])
                .build()
                .unwrap(),
        );
        let task = FileScanTask {
            file_size_in_bytes: 0,
            data_file_path: "data_file_path".to_string(),
            start: 0,
            length: 100,
            project_field_ids: vec![1, 2, 3],
            predicate: None,
            schema: schema.clone(),
            record_count: Some(100),
            first_row_id: None,
            last_updated_sequence_number: None,
            data_file_format: DataFileFormat::Parquet,
            partition_spec_id: 0,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };
        test_fn(task);

        // with predicate
        let task = FileScanTask {
            file_size_in_bytes: 0,
            data_file_path: "data_file_path".to_string(),
            start: 0,
            length: 100,
            project_field_ids: vec![1, 2, 3],
            predicate: Some(BoundPredicate::AlwaysTrue),
            schema,
            record_count: None,
            first_row_id: None,
            last_updated_sequence_number: None,
            data_file_format: DataFileFormat::Avro,
            partition_spec_id: 0,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        };
        test_fn(task);
    }

    #[test]
    fn test_select_with_file_column() {
        use arrow_array::cast::AsArray;

        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Select regular columns plus the _file column
        let table_scan = fixture
            .table
            .scan()
            .select(["x", RESERVED_COL_NAME_FILE])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        // Verify we have 2 columns: x and _file
        assert_eq!(batches[0].num_columns(), 2);

        // Verify the x column exists and has correct data
        let x_col = batches[0].column_by_name("x").unwrap();
        let x_arr = x_col.as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(x_arr.value(0), 1);

        // Verify the _file column exists
        let file_col = batches[0].column_by_name(RESERVED_COL_NAME_FILE);
        assert!(
            file_col.is_some(),
            "_file column should be present in the batch"
        );

        // Verify the _file column contains a file path
        let file_col = file_col.unwrap();
        assert!(
            matches!(
                file_col.data_type(),
                arrow_schema::DataType::RunEndEncoded(_, _)
            ),
            "_file column should use RunEndEncoded type"
        );

        // Decode the RunArray to verify it contains the file path
        let run_array = file_col
            .as_any()
            .downcast_ref::<arrow_array::RunArray<arrow_array::types::Int32Type>>()
            .expect("_file column should be a RunArray");

        let values = run_array.values();
        let string_values = values.as_string::<i32>();
        assert_eq!(string_values.len(), 1, "Should have a single file path");

        let file_path = string_values.value(0);
        assert!(
            file_path.ends_with(".parquet"),
            "File path should end with .parquet, got: {file_path}"
        );
    }

    #[test]
    fn test_select_file_column_position() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Select columns in specific order: x, _file, z
        let table_scan = fixture
            .table
            .scan()
            .select(["x", RESERVED_COL_NAME_FILE, "z"])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_columns(), 3);

        // Verify column order: x at position 0, _file at position 1, z at position 2
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "x");
        assert_eq!(schema.field(1).name(), RESERVED_COL_NAME_FILE);
        assert_eq!(schema.field(2).name(), "z");

        // Verify columns by name also works
        assert!(batches[0].column_by_name("x").is_some());
        assert!(batches[0].column_by_name(RESERVED_COL_NAME_FILE).is_some());
        assert!(batches[0].column_by_name("z").is_some());
    }

    #[test]
    fn test_select_file_column_only() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Select only the _file column
        let table_scan = fixture
            .table
            .scan()
            .select([RESERVED_COL_NAME_FILE])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        // Should have exactly 1 column
        assert_eq!(batches[0].num_columns(), 1);

        // Verify it's the _file column
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), RESERVED_COL_NAME_FILE);

        // Verify the batch has the correct number of rows
        // The scan reads files 1.parquet and 3.parquet (2.parquet is deleted)
        // Each file has 1024 rows, so total is 2048 rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2048);
    }

    #[test]
    fn test_file_column_with_multiple_files() {
        use std::collections::HashSet;

        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Select x and _file columns
        let table_scan = fixture
            .table
            .scan()
            .select(["x", RESERVED_COL_NAME_FILE])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        // Collect all unique file paths from the batches
        let mut file_paths = HashSet::new();
        for batch in &batches {
            let file_col = batch.column_by_name(RESERVED_COL_NAME_FILE).unwrap();
            let run_array = file_col
                .as_any()
                .downcast_ref::<arrow_array::RunArray<arrow_array::types::Int32Type>>(
                )
                .expect("_file column should be a RunArray");

            let values = run_array.values();
            let string_values = values.as_string::<i32>();
            for i in 0..string_values.len() {
                file_paths.insert(string_values.value(i).to_string());
            }
        }

        // We should have multiple files (the test creates 1.parquet and 3.parquet)
        assert!(!file_paths.is_empty(), "Should have at least one file path");

        // All paths should end with .parquet
        for path in &file_paths {
            assert!(
                path.ends_with(".parquet"),
                "All file paths should end with .parquet, got: {path}"
            );
        }
    }

    #[test]
    fn test_file_column_at_start() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Select _file at the start
        let table_scan = fixture
            .table
            .scan()
            .select([RESERVED_COL_NAME_FILE, "x", "y"])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_columns(), 3);

        // Verify _file is at position 0
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), RESERVED_COL_NAME_FILE);
        assert_eq!(schema.field(1).name(), "x");
        assert_eq!(schema.field(2).name(), "y");
    }

    #[test]
    fn test_file_column_at_end() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files();

        // Select _file at the end
        let table_scan = fixture
            .table
            .scan()
            .select(["x", "y", RESERVED_COL_NAME_FILE])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_columns(), 3);

        // Verify _file is at position 2 (the end)
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "x");
        assert_eq!(schema.field(1).name(), "y");
        assert_eq!(schema.field(2).name(), RESERVED_COL_NAME_FILE);
    }

    #[test]
    fn test_select_with_repeated_column_names() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        // Select with repeated column names - both regular columns and virtual columns
        // Repeated columns should appear multiple times in the result (duplicates are allowed)
        let table_scan = fixture
            .table
            .scan()
            .select([
                "x",
                RESERVED_COL_NAME_FILE,
                "x", // x repeated
                "y",
                RESERVED_COL_NAME_FILE, // _file repeated
                "y",                    // y repeated
            ])
            .with_row_selection_enabled(true)
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        // Verify we have exactly 6 columns (duplicates are allowed and preserved)
        assert_eq!(
            batches[0].num_columns(),
            6,
            "Should have exactly 6 columns with duplicates"
        );

        let schema = batches[0].schema();

        // Verify columns appear in the exact order requested: x, _file, x, y, _file, y
        assert_eq!(schema.field(0).name(), "x", "Column 0 should be x");
        assert_eq!(
            schema.field(1).name(),
            RESERVED_COL_NAME_FILE,
            "Column 1 should be _file"
        );
        assert_eq!(
            schema.field(2).name(),
            "x",
            "Column 2 should be x (duplicate)"
        );
        assert_eq!(schema.field(3).name(), "y", "Column 3 should be y");
        assert_eq!(
            schema.field(4).name(),
            RESERVED_COL_NAME_FILE,
            "Column 4 should be _file (duplicate)"
        );
        assert_eq!(
            schema.field(5).name(),
            "y",
            "Column 5 should be y (duplicate)"
        );

        // Verify all columns have correct data types
        assert!(
            matches!(schema.field(0).data_type(), arrow_schema::DataType::Int64),
            "Column x should be Int64"
        );
        assert!(
            matches!(schema.field(2).data_type(), arrow_schema::DataType::Int64),
            "Column x (duplicate) should be Int64"
        );
        assert!(
            matches!(schema.field(3).data_type(), arrow_schema::DataType::Int64),
            "Column y should be Int64"
        );
        assert!(
            matches!(schema.field(5).data_type(), arrow_schema::DataType::Int64),
            "Column y (duplicate) should be Int64"
        );
        assert!(
            matches!(
                schema.field(1).data_type(),
                arrow_schema::DataType::RunEndEncoded(_, _)
            ),
            "_file column should use RunEndEncoded type"
        );
        assert!(
            matches!(
                schema.field(4).data_type(),
                arrow_schema::DataType::RunEndEncoded(_, _)
            ),
            "_file column (duplicate) should use RunEndEncoded type"
        );
    }

    #[test]
    fn test_select_with_field_ids() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();

        let table_scan = fixture
            .table
            .scan()
            .select_field_ids([3, 1])
            .build()
            .unwrap();

        let batch_iterator = table_scan.to_arrow().unwrap();
        let batches: Vec<_> = batch_iterator.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(batches[0].num_columns(), 2);
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "z");
        assert_eq!(schema.field(1).name(), "x");
    }

    #[test]
    fn current_scan_accepts_fields_added_after_current_snapshot() {
        let mut fixture = TableTestFixture::new_unpartitioned();
        fixture.setup_unpartitioned_manifest_files();
        let mut metadata = fixture.table.metadata().clone();
        metadata.last_column_id = 100;
        fixture.table = fixture.table.with_metadata(Arc::new(metadata));

        let prepared = Transaction::new(&fixture.table)
            .update_schema()
            .add_column(AddColumn::optional(
                "schema_only_col",
                Type::Primitive(PrimitiveType::Int),
            ))
            .prepare(&fixture.table)
            .unwrap();
        let metadata = prepared
            .apply_to_metadata(fixture.table.metadata())
            .unwrap();
        let field_id = metadata
            .current_schema()
            .field_id_by_name("schema_only_col")
            .unwrap();
        fixture.table = fixture.table.with_metadata(Arc::new(metadata));

        fixture
            .table
            .scan()
            .select_field_ids([field_id])
            .build()
            .unwrap();
    }

    #[test]
    fn test_scan_deadlock() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_deadlock_manifests();

        // Create table scan with concurrency limit 1
        // This limit is now likely irrelevant for deadlock in synchronous code unless it limits recursion or something specific.
        let table_scan = fixture
            .table
            .scan()
            .with_concurrency_limit(1)
            .build()
            .unwrap();

        // This should not hang
        let result = table_scan.plan_files();

        // Assert it finished
        assert!(result.is_ok(), "Scan failed");
    }
}
