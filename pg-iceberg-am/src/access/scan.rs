//! Iceberg Table Scan Implementation.
//!
//! A scan's state is split in two:
//!
//! - [`ScanSpec`] is the immutable scan description (table, [`ScanColumns`],
//!   optional [`Predicate`]). Built once in
//!   [`AmScanSession::scan_begin`] and preserved across `scan_rescan`, so the
//!   visible snapshot is frozen for the scan's duration. This matches the Read
//!   Committed contract: every `scan_rescan` comes from the same statement that
//!   issued `scan_begin`, so holding the metadata is correct. Newly committed
//!   work becomes visible at the next statement's `scan_begin`.
//! - [`IcebergBatchCursor`] is the per-cursor mutable state; `scan_rescan`
//!   rebuilds only the cursor from the spec.
//!
//! `scan_rescan` re-translates the dispatcher-supplied keys (PostgreSQL's
//! "non-null replaces, null keeps" rule is applied by the dispatcher first).
//! The borrow is consumed within each callback, never retained.
//!
//! [`scan_keys_to_predicate`] is currently a stub returning `None`, so
//! predicates remain handled by the executor (`ExecQual`). Adding real pushdown
//! is a localized change there.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, RunArray, StringArray};
use iceberg_lite::expr::Predicate;
use iceberg_lite::metadata_columns::{
    RESERVED_COL_NAME_FILE, RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_FILE,
    RESERVED_FIELD_ID_POS,
};
use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::scan::{ArrowRecordBatchIterator, FileScanTask, TableScan};
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pg_arrow_conv::{ArrowBatchSource, ArrowColumnDecoder, BoundBatch};
use pg_lakebase_core::access::mutation::ModifyScanBinding;
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use crate::IcebergTableAm;
use crate::access::column_mapping::{RelationShape, ScanColumns};
use crate::access::isolation::PgTransactionIsolation;
use crate::access::mutation::{IcebergFileSource, IcebergModifyQueryState};
use crate::access::projection::Projection;
use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::catalog::row_mutations::IcebergFileId;
use crate::error::{IcebergError, IcebergResult};
use crate::storage::StorageContext;

/// Planned file tasks for one concrete scan predicate/projection.
#[derive(Debug)]
pub(crate) struct PlannedScanTasks {
    tasks: Arc<[FileScanTask]>,
    tasks_by_path: HashMap<Box<str>, Vec<usize>>,
}

impl PlannedScanTasks {
    fn query(tasks: Vec<FileScanTask>) -> Self {
        Self {
            tasks: Arc::from(tasks.into_boxed_slice()),
            tasks_by_path: HashMap::new(),
        }
    }

    pub(crate) fn mutation(tasks: Vec<FileScanTask>) -> Self {
        let mut tasks_by_path: HashMap<Box<str>, Vec<usize>> = HashMap::new();
        for (task_index, task) in tasks.iter().enumerate() {
            tasks_by_path
                .entry(Box::<str>::from(task.data_file_path.as_str()))
                .or_default()
                .push(task_index);
        }
        Self {
            tasks: Arc::from(tasks.into_boxed_slice()),
            tasks_by_path,
        }
    }

    fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    fn shared_tasks(&self) -> Arc<[FileScanTask]> {
        Arc::clone(&self.tasks)
    }

    pub(crate) fn mutation_tasks_for_path(
        &self,
        path: &str,
    ) -> IcebergResult<Vec<FileScanTask>> {
        let task_refs = self.tasks_by_path.get(path).ok_or_else(|| {
            IcebergError::MetadataTracker(format!(
                "cannot find Iceberg scan task metadata for deletion target {path}"
            ))
        })?;
        let mut tasks = Vec::with_capacity(task_refs.len());
        for task_index in task_refs {
            let task = self.tasks().get(*task_index).ok_or(
                IcebergError::InvariantViolated(
                    "scan task path index is inconsistent",
                ),
            )?;
            tasks.push(task.clone());
        }
        Ok(tasks)
    }
}

/// Immutable parameters for a scan: which table, snapshot schema, columns, and
/// predicate.
///
/// `ScanSpec::build*` is the only place metadata is read from storage during a
/// scan; `scan_rescan` reuses the spec and rebuilds only the cursor.
///
/// `pub(crate)` so the CustomScan provider in [`crate::customscan`] can build a
/// `ScanSpec` from a runtime-built [`Predicate`], reusing the same scan core in
/// both the TableAM seqscan and CustomScan paths.
pub(crate) struct ScanSpec {
    /// Ready-to-scan Iceberg table. Cheap to clone (`Arc`-backed).
    table: Arc<Table>,
    /// Schema-bound column plan for the captured snapshot. Drives the cursor
    /// decoder and exposes the `IcebergSchema` for predicate translation.
    plan: ScanColumns,
    /// Stable predicate used only for file planning. It must not contain
    /// `PARAM_EXEC`, because those values can change across rescans while the
    /// planned task cache remains fixed.
    planning_filter: Option<Predicate>,
    /// Current exact row predicate applied by the reader. It may contain
    /// runtime parameters and is replaced on rescan.
    row_filter: Option<Predicate>,
    /// Transaction-local Iceberg file delta captured for this statement.
    delta: Option<Arc<SnapshotDelta>>,
    /// Stable planned tasks for ordinary query projection.
    query_tasks: Option<Arc<PlannedScanTasks>>,
    /// Stable planned tasks for mutation projection, including row-location
    /// metadata and a path index used by v3 deletion-vector finalization.
    mutation_tasks: Option<Arc<PlannedScanTasks>>,
}

impl ScanSpec {
    /// Build a `ScanSpec` from an [`OwnedScanKeys`] (the TableAM seqscan path).
    ///
    /// A plain SeqScan never specifies needed columns, so the projection stays
    /// `None`; the full-schema [`ColumnMapping`](crate::access::column_mapping)
    /// built from `shape` still fixes dropped-column alignment.
    pub(crate) fn build(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        keys: &OwnedScanKeys,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let mut spec =
            Self::build_with_predicates(rel_oid, spc_oid, None, None, shape)?;

        // Translate keys after the schema is in hand (the translator needs
        // Iceberg type / field-id info from parsed metadata).
        let filter = scan_keys_to_predicate(keys, spec.plan.schema())?;
        spec.planning_filter.clone_from(&filter);
        spec.row_filter = filter;
        Ok(spec)
    }

    pub(crate) fn build_with_predicates(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let (table, schema, delta) = Self::load_table(rel_oid, spc_oid)?;
        let plan = ScanColumns::new(schema, shape)?;
        Ok(Self {
            table: Arc::new(table),
            plan,
            planning_filter,
            row_filter,
            delta,
            query_tasks: None,
            mutation_tasks: None,
        })
    }

    /// Build a `ScanSpec` for the CustomScan path with a column projection.
    ///
    /// `projection` drives both `select_field_ids` (read fewer columns) and a
    /// projected [`ColumnMapping`](crate::access::column_mapping). The decoder
    /// writes only the projected `dest` slots; projected-away positions are
    /// left untouched, which is safe because they are never read (see
    /// [`ColumnMapping`](crate::access::column_mapping)) — not because the
    /// cleared slot reads them back as NULL.
    pub(crate) fn build_with_projection(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        projection: Projection,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
        shape: &RelationShape,
        scan_attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let (table, schema, delta) = Self::load_table(rel_oid, spc_oid)?;
        let plan = ScanColumns::with_projection(
            schema,
            shape,
            &projection,
            scan_attr_types.len(),
            scan_attr_types,
        )?;
        Ok(Self {
            table: Arc::new(table),
            plan,
            planning_filter,
            row_filter,
            delta,
            query_tasks: None,
            mutation_tasks: None,
        })
    }

    /// Shared Iceberg scan core: resolve the relation's metadata through PG's
    /// transactional cache and build the `Arc<Table>` bound to the current
    /// snapshot's schema. Used by both PG-side entry points.
    fn load_table(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
    ) -> IcebergResult<(Table, Arc<IcebergSchema>, Option<Arc<SnapshotDelta>>)> {
        // Validate the transaction mode on every execution-side scan entry.
        // Serializable currently retains statement-level metadata visibility
        // and strengthens Iceberg row-level write validation; see
        // `access::isolation` for the intentionally incomplete PG SSI scope.
        PgTransactionIsolation::current()?;
        let ctx = StorageContext::for_tablespace(spc_oid)?;

        let loaded =
            TxMetadata::current().current_table_metadata(rel_oid, ctx.file_io())?;
        let schema = loaded.metadata.current_schema().clone();

        let table = Table::builder()
            .file_io(ctx.file_io().clone())
            .metadata_location(loaded.location)
            .metadata(loaded.metadata)
            .identifier(IcebergTableId::for_relation(rel_oid).into_table_ident())
            .build()?;

        Ok((table, schema, loaded.delta))
    }

    /// Replace both the planning and row predicates. Used by the TableAM
    /// scan-key path where a changed key must invalidate planned tasks.
    pub(crate) fn set_filter(
        &mut self,
        predicate: Option<Predicate>,
    ) -> IcebergResult<()> {
        if self.planning_filter != predicate {
            self.query_tasks = None;
            self.mutation_tasks = None;
        }
        self.planning_filter.clone_from(&predicate);
        self.row_filter = predicate;
        Ok(())
    }

    /// Replace the planning and row predicates independently. Used by
    /// CustomScan after it builds field-id predicate bindings from the same
    /// schema as the scan plan.
    pub(crate) fn set_predicates(
        &mut self,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
    ) {
        if self.planning_filter != planning_filter {
            self.query_tasks = None;
            self.mutation_tasks = None;
        }
        self.planning_filter = planning_filter;
        self.row_filter = row_filter;
    }

    /// Replace only the current row predicate. Used by CustomScan rescans when
    /// `PARAM_EXEC` values change; the stable planned file-task set remains a
    /// safe superset.
    pub(crate) fn set_row_filter(&mut self, predicate: Option<Predicate>) {
        self.row_filter = predicate;
    }

    pub(crate) fn prepared_mutation_tasks(&self) -> Option<Arc<PlannedScanTasks>> {
        self.mutation_tasks.clone()
    }

    pub(crate) fn prepare_mutation_tasks(&mut self) -> IcebergResult<()> {
        self.planned_mutation_tasks().map(|_| ())
    }

    fn planned_query_tasks(&mut self) -> IcebergResult<Arc<PlannedScanTasks>> {
        if let Some(tasks) = self.query_tasks.as_ref() {
            return Ok(Arc::clone(tasks));
        }
        let tasks = self
            .build_scan(false, self.planning_filter.as_ref())?
            .plan_files()?;
        let planned = Arc::new(PlannedScanTasks::query(tasks));
        self.query_tasks = Some(Arc::clone(&planned));
        Ok(planned)
    }

    fn planned_mutation_tasks(&mut self) -> IcebergResult<Arc<PlannedScanTasks>> {
        if let Some(tasks) = self.mutation_tasks.as_ref() {
            return Ok(Arc::clone(tasks));
        }
        let tasks = self
            .build_scan(true, self.planning_filter.as_ref())?
            .plan_files()?;
        let planned = Arc::new(PlannedScanTasks::mutation(tasks));
        self.mutation_tasks = Some(Arc::clone(&planned));
        Ok(planned)
    }

    fn read_planned_tasks(
        &self,
        include_row_locations: bool,
        tasks: Arc<PlannedScanTasks>,
    ) -> IcebergResult<ArrowRecordBatchIterator> {
        self.build_scan(include_row_locations, None)?
            .to_arrow_with_shared_tasks_and_filter(
                tasks.shared_tasks(),
                self.row_filter.clone(),
            )
            .map_err(IcebergError::from)
    }

    /// Construct a fresh slot-first [`IcebergBatchCursor`] for the TableAM scan.
    pub(crate) fn open_batch_cursor(&mut self) -> IcebergResult<IcebergBatchCursor> {
        let tasks = self.planned_query_tasks()?;
        let source = ArrowBatchSource::new(IcebergArrowBatches(
            self.read_planned_tasks(false, tasks)?,
        ));
        let decoder = ArrowColumnDecoder::new(self.plan.decoded_columns());
        Ok(IcebergBatchCursor::new(source, decoder, None))
    }

    /// Open the provider cursor for `ScanPurpose::Modify`. The cursor consumes
    /// Iceberg's metadata columns to produce the executor's synthetic `ctid`.
    pub(crate) fn open_mutation_batch_cursor(
        &mut self,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
        table_oid: pg_sys::Oid,
    ) -> IcebergResult<IcebergBatchCursor> {
        let tasks = self.planned_mutation_tasks()?;
        let source = ArrowBatchSource::new(IcebergArrowBatches(
            self.read_planned_tasks(true, tasks)?,
        ));
        let decoder = ArrowColumnDecoder::new(self.plan.decoded_columns());
        Ok(IcebergBatchCursor::new(
            source,
            decoder,
            Some(ModifyCursorContext {
                binding,
                table_oid,
                last_file: None,
            }),
        ))
    }

    /// Build the Iceberg [`TableScan`] for this spec's projection and optional
    /// planning filter.
    fn build_scan(
        &self,
        include_row_locations: bool,
        filter: Option<&Predicate>,
    ) -> IcebergResult<TableScan> {
        let mut builder = self.table.scan();
        builder =
            builder.select_field_ids(self.scan_field_ids(include_row_locations));
        if let Some(predicate) = filter {
            builder = builder.with_filter(predicate.clone());
        }
        if let Some(delta) = self.delta.as_ref() {
            builder = builder.with_delta(Arc::clone(delta));
        }
        Ok(builder.build()?)
    }

    fn scan_field_ids(&self, include_row_locations: bool) -> Vec<i32> {
        let mut field_ids = self.plan.project_field_ids().to_vec();
        if include_row_locations {
            field_ids.push(RESERVED_FIELD_ID_FILE);
            field_ids.push(RESERVED_FIELD_ID_POS);
        }
        field_ids
    }

    pub(crate) fn starting_snapshot_id(&self) -> Option<i64> {
        self.table.metadata().current_snapshot_id()
    }

    pub(crate) fn schema(&self) -> &IcebergSchema {
        self.plan.schema()
    }

    /// Re-translate the current effective [`OwnedScanKeys`] into a filter.
    fn refresh_filter(&mut self, keys: &OwnedScanKeys) -> IcebergResult<()> {
        self.set_filter(scan_keys_to_predicate(keys, self.plan.schema())?)
    }
}

/// Adapts the Iceberg Arrow batch iterator into the conversion crate's batch
/// source. The producer error (`iceberg_lite::Error`: IO, Parquet, metadata,
/// schema) is preserved as an [`IcebergError`] so it reaches the callback
/// boundary with its own SQLSTATE (IO/internal/feature) rather than being
/// reclassified as a `ConvError::DatumConversionError` (`DATA_EXCEPTION`).
/// `pg-arrow-conv` stays format-neutral: it only requires the error to map into
/// the boundary error, which `IcebergError` already does.
struct IcebergArrowBatches(ArrowRecordBatchIterator);

impl Iterator for IcebergArrowBatches {
    type Item = Result<RecordBatch, IcebergError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Cooperative cancellation at the batch boundary. PG's `ExecScanFetch`
        // already fires `CHECK_FOR_INTERRUPTS` once per *returned* tuple for both
        // the TableAM seqscan and the CustomScan, but a single `getnextslot` /
        // `next_slot` call can pull many batches here (skipping batches fully
        // eliminated by pushed filters, or reading the next Parquet row group),
        // so a query cancel issued mid-IO would otherwise wait until the next
        // tuple surfaces. Checking per batch — the unit of Iceberg scan IO —
        // closes that gap for both scan paths, which share this iterator.
        pgrx::pg_sys::check_for_interrupts!();
        self.0.next().map(|batch| batch.map_err(IcebergError::from))
    }
}

type IcebergArrowBatchSource = ArrowBatchSource<IcebergArrowBatches, IcebergError>;

struct IcebergBoundBatch {
    decoded: BoundBatch,
    pos_column: Option<Int64Array>,
    file_runs: Option<Box<[RegisteredFileRun]>>,
}

struct RegisteredFileRun {
    end_row: usize,
    file_id: IcebergFileId,
}

#[derive(Clone)]
struct ModifyCursorContext {
    binding: ModifyScanBinding<IcebergModifyQueryState>,
    table_oid: pg_sys::Oid,
    /// Fast path for adjacent runs/batches from the same planned file. This
    /// avoids re-hashing a long file path while retaining the transaction
    /// registry as the sole identity authority.
    last_file: Option<(Box<str>, IcebergFileId)>,
}

impl ModifyCursorContext {
    fn register_file(&mut self, path: &str) -> AmResult<IcebergFileId> {
        if let Some((cached_path, file_id)) = self.last_file.as_ref()
            && cached_path.as_ref() == path
        {
            return Ok(*file_id);
        }
        let source = IcebergFileSource::new(path);
        let file_id = self.binding.register_identity_source(&source)?;
        self.last_file = Some((path.into(), file_id));
        Ok(file_id)
    }
}

#[derive(Clone)]
enum MetadataStringColumn {
    Plain(StringArray),
    RunEndEncoded(RunArray<Int32Type>),
}

impl MetadataStringColumn {
    fn try_new(array: ArrayRef, name: &'static str) -> AmResult<Self> {
        if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(Self::Plain(strings.clone()));
        }

        if let Some(run_array) = array.as_any().downcast_ref::<RunArray<Int32Type>>()
            && run_array.values().as_any().is::<StringArray>()
        {
            return Ok(Self::RunEndEncoded(run_array.clone()));
        }

        Err(IcebergError::ArrowTypeMismatch(format!(
            "metadata column {name} has unexpected Arrow type {:?}",
            array.data_type()
        ))
        .into())
    }

    /// Visit contiguous logical runs without expanding run-end encoding.
    fn try_for_each_run<E>(
        &self,
        mut visit: impl FnMut(usize, &str) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<IcebergError>,
    {
        match self {
            Self::Plain(strings) => {
                let mut start = 0;
                while start < strings.len() {
                    if strings.is_null(start) {
                        return Err(IcebergError::InvariantViolated(
                            "Row identity file cannot be NULL",
                        )
                        .into());
                    }
                    let path = strings.value(start);
                    let mut end = start + 1;
                    while end < strings.len()
                        && !strings.is_null(end)
                        && strings.value(end) == path
                    {
                        end += 1;
                    }
                    visit(end, path)?;
                    start = end;
                }
            }
            Self::RunEndEncoded(runs) => {
                let values =
                    runs.values().as_any().downcast_ref::<StringArray>().expect(
                        "metadata string column values type checked at construction",
                    );
                let first_value = runs.get_start_physical_index();
                for (value_idx, end_row) in
                    (first_value..).zip(runs.run_ends().sliced_values())
                {
                    if values.is_null(value_idx) {
                        return Err(IcebergError::InvariantViolated(
                            "Row identity file cannot be NULL",
                        )
                        .into());
                    }
                    let end_row = usize::try_from(end_row).map_err(|_| {
                        IcebergError::InvariantViolated(
                            "Row identity run end cannot be negative",
                        )
                    })?;
                    visit(end_row, values.value(value_idx))?;
                }
            }
        }
        Ok(())
    }
}

/// Arrow batches decoded straight into the slot. Provider Modify mode consumes
/// `_file`/`_pos` internally to synthesize the PostgreSQL row-identity column.
pub struct IcebergBatchCursor {
    source: IcebergArrowBatchSource,
    decoder: ArrowColumnDecoder,
    current: Option<IcebergBoundBatch>,
    row_idx: usize,
    file_run_idx: usize,
    modify: Option<ModifyCursorContext>,
}

impl IcebergBatchCursor {
    fn new(
        source: IcebergArrowBatchSource,
        decoder: ArrowColumnDecoder,
        modify: Option<ModifyCursorContext>,
    ) -> Self {
        Self {
            source,
            decoder,
            current: None,
            row_idx: 0,
            file_run_idx: 0,
            modify,
        }
    }

    fn bind_batch(&mut self, batch: RecordBatch) -> AmResult<IcebergBoundBatch> {
        let (pos_column, file_column) = if self.modify.is_some() {
            let file_column = MetadataStringColumn::try_new(
                Self::metadata_column_ref(
                    &batch,
                    RESERVED_FIELD_ID_FILE,
                    RESERVED_COL_NAME_FILE,
                )?,
                RESERVED_COL_NAME_FILE,
            )?;
            let pos_column = Self::typed_metadata_column::<Int64Array>(
                &batch,
                RESERVED_FIELD_ID_POS,
                RESERVED_COL_NAME_POS,
            )?;

            (Some(pos_column), Some(file_column))
        } else {
            (None, None)
        };

        let file_runs = match (&file_column, self.modify.as_mut()) {
            (Some(files), Some(modify)) => {
                Some(Self::register_file_runs(files, modify)?)
            }
            (None, None) => None,
            _ => {
                return Err(IcebergError::InvariantViolated(
                    "row-location columns and Modify binding disagree",
                )
                .into());
            }
        };
        let decoded = self.decoder.bind(batch)?;
        Ok(IcebergBoundBatch {
            decoded,
            pos_column,
            file_runs,
        })
    }

    fn register_file_runs(
        files: &MetadataStringColumn,
        modify: &mut ModifyCursorContext,
    ) -> AmResult<Box<[RegisteredFileRun]>> {
        let mut runs = Vec::new();
        files.try_for_each_run(|end_row, path| -> AmResult<()> {
            let file_id = modify.register_file(path)?;
            runs.push(RegisteredFileRun { end_row, file_id });
            Ok(())
        })?;
        Ok(runs.into_boxed_slice())
    }

    fn typed_metadata_column<T: Array + Clone + 'static>(
        batch: &RecordBatch,
        field_id: i32,
        name: &'static str,
    ) -> AmResult<T> {
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| {
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|raw| raw.parse::<i32>().ok())
                    == Some(field_id)
            })
            .ok_or(IcebergError::InvariantViolated(
                "row-location metadata column is missing from mutation scan",
            ))?;
        let array = batch.column(index);
        array.as_any().downcast_ref::<T>().cloned().ok_or_else(|| {
            IcebergError::ArrowTypeMismatch(format!(
                "metadata column {name} has unexpected Arrow type {:?}",
                array.data_type()
            ))
            .into()
        })
    }

    fn metadata_column_ref(
        batch: &RecordBatch,
        field_id: i32,
        _name: &'static str,
    ) -> AmResult<ArrayRef> {
        let index = batch
            .schema()
            .fields()
            .iter()
            .position(|field| {
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|raw| raw.parse::<i32>().ok())
                    == Some(field_id)
            })
            .ok_or(IcebergError::InvariantViolated(
                "row-location metadata column is missing from mutation scan",
            ))?;
        Ok(Arc::clone(batch.column(index)))
    }

    /// Emit one modification row and encode its Iceberg row identity into the
    /// PostgreSQL `ctid` carried by the plan.
    pub(crate) fn next_mutation_into_slot(
        &mut self,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<bool> {
        let table_oid = self
            .modify
            .as_ref()
            .ok_or(IcebergError::InvariantViolated(
                "mutation cursor has no Modify binding",
            ))?
            .table_oid;
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_idx < self.decoder.num_rows(&bound.decoded)
            {
                let row_idx = self.row_idx;
                self.decoder.write_row(&bound.decoded, row_idx, out)?;
                let pos_column = bound.pos_column.as_ref().ok_or(
                    IcebergError::InvariantViolated(
                        "Modify scan is missing _pos metadata",
                    ),
                )?;
                if pos_column.is_null(row_idx) {
                    return Err(IcebergError::InvariantViolated(
                        "Row identity metadata cannot be NULL",
                    )
                    .into());
                }
                let position =
                    u64::try_from(pos_column.value(row_idx)).map_err(|_| {
                        IcebergError::InvariantViolated(
                            "Row position cannot be negative",
                        )
                    })?;

                let runs = bound.file_runs.as_ref().ok_or(
                    IcebergError::InvariantViolated(
                        "Modify scan has no registered file runs",
                    ),
                )?;
                while self.file_run_idx < runs.len()
                    && row_idx >= runs[self.file_run_idx].end_row
                {
                    self.file_run_idx += 1;
                }
                let run = runs.get(self.file_run_idx).ok_or(
                    IcebergError::InvariantViolated(
                        "Modify row has no registered file identity",
                    ),
                )?;
                let tid = IcebergModifyQueryState::encode_row_identity(
                    run.file_id,
                    &position,
                )?;
                out.set_tid(&tid);
                out.set_table_oid(table_oid);
                self.row_idx += 1;
                return Ok(true);
            }

            self.current = None;
            match self.source.next_batch()? {
                Some(batch) => {
                    self.current = Some(self.bind_batch(batch)?);
                    self.row_idx = 0;
                    self.file_run_idx = 0;
                }
                None => return Ok(false),
            }
        }
    }
}

impl ScanBatchDriver for IcebergBatchCursor {
    fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool> {
        if self.modify.is_some() {
            return self.next_mutation_into_slot(out);
        }
        loop {
            if let Some(bound) = self.current.as_ref()
                && self.row_idx < self.decoder.num_rows(&bound.decoded)
            {
                self.decoder.write_row(&bound.decoded, self.row_idx, out)?;
                self.row_idx += 1;
                return Ok(true);
            }

            self.current = None;
            match self.source.next_batch()? {
                Some(batch) => {
                    self.current = Some(self.bind_batch(batch)?);
                    self.row_idx = 0;
                    self.file_run_idx = 0;
                }
                None => return Ok(false),
            }
        }
    }
}

/// PostgreSQL-facing scan session for the Iceberg table AM. Thin bookkeeping
/// (`rel_oid` / `spc_oid` / `shape`) over the lazily-built [`ScanSpec`] and
/// current [`IcebergBatchCursor`].
pub struct IcebergScan {
    rel_oid: pg_sys::Oid,
    spc_oid: pg_sys::Oid,
    /// Relation shape captured in [`AmScanSession::new`] (the one place the
    /// `RelationHandle` is in scope), threaded into `ScanSpec::build`.
    shape: RelationShape,
    spec: Option<ScanSpec>,
    cursor: Option<IcebergBatchCursor>,
    analyze_block_started: bool,
}

impl AmScan for IcebergTableAm {}

impl AmScanSession for IcebergScan {
    type BatchDriver = IcebergBatchCursor;

    fn new(
        rel: &RelationHandle,
        _snapshot: Option<&SnapshotHandle>,
        _pscan: Option<&ParallelTableScanDescHandle>,
        _flags: u32,
    ) -> AmResult<Self> {
        // No metadata IO yet: defer schema-dependent work to `scan_begin`. The
        // relation shape is captured here, where the `RelationHandle` is in scope.
        Ok(IcebergScan {
            rel_oid: rel.oid(),
            spc_oid: rel.tablespace_oid(),
            shape: RelationShape::from_relation(rel),
            spec: None,
            cursor: None,
            analyze_block_started: false,
        })
    }

    fn scan_begin(&mut self, keys: &OwnedScanKeys) -> AmResult<()> {
        let mut spec =
            ScanSpec::build(self.rel_oid, self.spc_oid, keys, &self.shape)?;
        let cursor = spec.open_batch_cursor()?;
        self.spec = Some(spec);
        self.cursor = Some(cursor);
        Ok(())
    }

    /// Slot-first scan driver: the Arrow batch cursor that decodes the current
    /// batch straight into the slot. The framework drives every scan through
    /// this one path; there is no row variant for a columnar AM.
    fn scan_driver(&mut self) -> &mut Self::BatchDriver {
        // `scan_begin` builds the cursor before the executor fetches any row,
        // so it is always present by the time the framework calls this.
        self.cursor
            .as_mut()
            .expect("scan_driver called after scan_begin")
    }

    /// Restart the scan, re-translating the current effective scan keys.
    ///
    /// The dispatcher has already applied the "non-null replaces, null keeps"
    /// rule, so `keys` is the effective set. `set_params` and the `allow_*`
    /// flags only affect heap-AM strategy choices and are ignored. Metadata is
    /// not re-read: a single statement drives every `scan_rescan` and must see
    /// a consistent snapshot.
    fn scan_rescan(
        &mut self,
        keys: &OwnedScanKeys,
        _set_params: bool,
        _allow_strat: bool,
        _allow_sync: bool,
        _allow_pagemode: bool,
    ) -> AmResult<()> {
        let Some(spec) = self.spec.as_mut() else {
            // Defensive: rescan before the first scan_begin shouldn't happen.
            self.cursor = None;
            return Ok(());
        };

        spec.refresh_filter(keys)?;
        self.cursor = Some(spec.open_batch_cursor()?);
        Ok(())
    }

    fn scan_end(&mut self) -> AmResult<()> {
        self.cursor = None;
        self.spec = None;
        Ok(())
    }

    fn scan_analyze_next_block(
        &mut self,
        _stream: &ReadStreamHandle,
    ) -> AmResult<bool> {
        if self.analyze_block_started {
            Ok(false)
        } else {
            self.analyze_block_started = true;
            Ok(true)
        }
    }

    fn scan_analyze_next_tuple(
        &mut self,
        _oldest_xmin: pg_sys::TransactionId,
        out: &mut SlotColumns<'_>,
    ) -> AmResult<(bool, f64, f64)> {
        let found = self.scan_driver().next_into_slot(out)?;
        Ok((found, f64::from(found), 0.0))
    }
}

/// Translate PostgreSQL [`OwnedScanKeys`] into an Iceberg [`Predicate`].
///
/// The only place the AM converts PG filter representations into Iceberg
/// expressions, keeping predicate-pushdown work out of the scan lifecycle.
///
/// Current status: stub. `Ok(None)` is safe today because PostgreSQL only
/// supplies `ScanKey`s through paths the Iceberg AM does not advertise support
/// for; plain `SeqScan` lets the executor apply `WHERE` above the scan.
fn scan_keys_to_predicate(
    _keys: &OwnedScanKeys,
    _schema: &IcebergSchema,
) -> IcebergResult<Option<Predicate>> {
    // TODO(predicate-pushdown): map each ScanKeyEntry's attno -> Iceberg field,
    // strategy/subtype/argument -> Predicate, combine with `Predicate::and`,
    // and return `Ok(None)` for the whole set if any key cannot be translated.
    Ok(None)
}

#[cfg(test)]
mod metadata_column_tests {
    use super::*;
    use arrow_array::Int32Array;

    #[test]
    fn plain_strings_are_grouped_into_logical_runs() {
        let column = MetadataStringColumn::Plain(StringArray::from(vec![
            "a.parquet",
            "a.parquet",
            "b.parquet",
        ]));
        let mut actual = Vec::new();

        column
            .try_for_each_run(|end_row, path| -> IcebergResult<()> {
                actual.push((end_row, path.to_owned()));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            actual,
            vec![(2, "a.parquet".to_owned()), (3, "b.parquet".to_owned())]
        );
    }

    #[test]
    fn run_end_encoded_strings_visit_physical_runs_only() {
        let run_ends = Int32Array::from(vec![2, 5, 6]);
        let values = StringArray::from(vec!["a.parquet", "b.parquet", "c.parquet"]);
        let runs = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let column = MetadataStringColumn::RunEndEncoded(runs.slice(1, 4));
        let mut actual = Vec::new();

        column
            .try_for_each_run(|end_row, path| -> IcebergResult<()> {
                actual.push((end_row, path.to_owned()));
                Ok(())
            })
            .unwrap();

        assert_eq!(
            actual,
            vec![(1, "a.parquet".to_owned()), (4, "b.parquet".to_owned())]
        );
    }

    #[test]
    fn run_end_encoded_null_file_is_rejected_without_expansion() {
        let run_ends = Int32Array::from(vec![4]);
        let values = StringArray::from(vec![None::<&str>]);
        let runs = RunArray::<Int32Type>::try_new(&run_ends, &values).unwrap();
        let column = MetadataStringColumn::RunEndEncoded(runs);

        assert!(
            column
                .try_for_each_run(|_, _| -> IcebergResult<()> { Ok(()) })
                .is_err()
        );
    }
}
