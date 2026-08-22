//! Catalog-independent Iceberg scan description and file-task planning.

use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::expr::Predicate;
use iceberg_lite::metadata_columns::{RESERVED_FIELD_ID_FILE, RESERVED_FIELD_ID_POS};
use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::scan::{ArrowRecordBatchIterator, FileScanTask, TableScan};
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use pg_arrow_conv::{ArrowBatchSource, ArrowColumnDecoder};
use pgrx::pg_sys;

use crate::engine::schema::column_mapping::ScanColumns;
use crate::engine::schema::relation::RelationShape;
use crate::engine::write::PlannedMutationTasks;
use crate::error::{IcebergError, IcebergResult};

use super::IcebergQueryCursor;
use super::batch::{IcebergArrowBatchSource, IcebergArrowBatches};
use super::projection::Projection;

/// Statement-scoped scan state: snapshot, bound columns, predicates, and
/// planned-task caches.
///
/// Catalog adapters resolve a [`ScanSource`] once, then this value owns the
/// schema binding, predicates, and task caches reused across rescans.
pub(crate) struct ScanSpec {
    /// Ready-to-scan table bound to the captured metadata snapshot.
    table: Table,
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
    /// Storage bytes used by PostgreSQL to define this scan's virtual block
    /// population. Captured with the same metadata snapshot as the file tasks.
    storage_bytes: Option<u64>,
    /// Stable planned tasks for ordinary query projection.
    query_tasks: Option<Arc<[FileScanTask]>>,
    /// Stable planned tasks for mutation projection, including row-location
    /// metadata and a path index used by v3 deletion-vector finalization.
    mutation_tasks: Option<Rc<PlannedMutationTasks>>,
}

#[derive(Clone, Copy)]
enum RowLocationProjection {
    Exclude,
    Include,
}

/// Already-resolved table state supplied by an AM or FDW adapter.
pub(crate) struct ScanSource {
    table: Table,
    delta: Option<Arc<SnapshotDelta>>,
    storage_bytes: Option<u64>,
}

impl ScanSource {
    pub(crate) fn transaction_view(
        table: Table,
        delta: Option<Arc<SnapshotDelta>>,
        storage_bytes: Option<u64>,
    ) -> Self {
        Self {
            table,
            delta,
            storage_bytes,
        }
    }

    pub(crate) fn schema(&self) -> &Arc<IcebergSchema> {
        self.table.metadata().current_schema()
    }
}

/// Shared ANALYZE planning output. The AM adapter owns PostgreSQL sampling
/// policy; the scan engine owns Iceberg task planning and decoding.
pub(crate) struct AnalyzeScanInput {
    pub(crate) scan: TableScan,
    pub(crate) tasks: Vec<FileScanTask>,
    pub(crate) decoder: ArrowColumnDecoder,
    pub(crate) storage_bytes: u64,
}

/// Shared mutation-scan reader input. The adapter binds it to its executor
/// identity registry and synthetic-ctid callback surface.
pub(crate) struct MutationScanInput {
    pub(crate) source: IcebergArrowBatchSource,
    pub(crate) decoder: ArrowColumnDecoder,
}

impl ScanSpec {
    pub(crate) fn full(
        source: ScanSource,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let plan = ScanColumns::new(source.schema().clone(), shape)?;
        Ok(Self::from_parts(source, plan, planning_filter, row_filter))
    }

    /// Build a scan with a PostgreSQL output projection.
    ///
    /// `projection` drives both `select_field_ids` (read fewer columns) and a
    /// projected [`ColumnMapping`](crate::engine::schema::column_mapping). The decoder
    /// writes only the projected `dest` slots; projected-away positions are
    /// left untouched, which is safe because they are never read (see
    /// [`ColumnMapping`](crate::engine::schema::column_mapping)) — not because the
    /// cleared slot reads them back as NULL.
    pub(crate) fn projected(
        source: ScanSource,
        projection: Projection,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
        shape: &RelationShape,
        scan_attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let plan = ScanColumns::with_projection(
            source.schema().clone(),
            shape,
            &projection,
            scan_attr_types.len(),
            scan_attr_types,
        )?;
        Ok(Self::from_parts(source, plan, planning_filter, row_filter))
    }

    fn from_parts(
        source: ScanSource,
        plan: ScanColumns,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
    ) -> Self {
        let ScanSource {
            table,
            delta,
            storage_bytes,
        } = source;
        Self {
            table,
            plan,
            planning_filter,
            row_filter,
            delta,
            storage_bytes,
            query_tasks: None,
            mutation_tasks: None,
        }
    }

    /// Replace both the planning and row predicates. Used by the TableAM
    /// scan-key path where a changed key must invalidate planned tasks.
    pub(crate) fn set_filter(&mut self, predicate: Option<Predicate>) {
        if self.planning_filter != predicate {
            self.query_tasks = None;
            self.mutation_tasks = None;
        }
        self.planning_filter.clone_from(&predicate);
        self.row_filter = predicate;
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

    /// Replace only the current row predicate. Used by projected scan rescans
    /// when `PARAM_EXEC` values change; the stable planned file-task set remains
    /// a safe superset.
    pub(crate) fn set_row_filter(&mut self, predicate: Option<Predicate>) {
        self.row_filter = predicate;
    }

    #[inline]
    pub(crate) fn schema_id(&self) -> i32 {
        self.plan.schema().schema_id()
    }

    pub(crate) fn schema(&self) -> &IcebergSchema {
        self.plan.schema()
    }

    pub(crate) fn prepared_mutation_tasks(&self) -> Option<Rc<PlannedMutationTasks>> {
        self.mutation_tasks.as_ref().map(Rc::clone)
    }

    pub(crate) fn prepare_mutation_tasks(&mut self) -> IcebergResult<()> {
        self.planned_mutation_tasks().map(|_| ())
    }

    fn planned_query_tasks(&mut self) -> IcebergResult<Arc<[FileScanTask]>> {
        if let Some(tasks) = self.query_tasks.as_ref() {
            return Ok(Arc::clone(tasks));
        }
        let tasks = self
            .build_scan(
                RowLocationProjection::Exclude,
                self.planning_filter.as_ref(),
            )?
            .plan_files()?;
        let tasks = Arc::from(tasks.into_boxed_slice());
        self.query_tasks = Some(Arc::clone(&tasks));
        Ok(tasks)
    }

    fn planned_mutation_tasks(&mut self) -> IcebergResult<Rc<PlannedMutationTasks>> {
        if let Some(tasks) = self.mutation_tasks.as_ref() {
            return Ok(Rc::clone(tasks));
        }
        let tasks = self
            .build_scan(
                RowLocationProjection::Include,
                self.planning_filter.as_ref(),
            )?
            .plan_files()?;
        let planned = Rc::new(PlannedMutationTasks::new(tasks));
        self.mutation_tasks = Some(Rc::clone(&planned));
        Ok(planned)
    }

    fn read_planned_tasks(
        &self,
        row_locations: RowLocationProjection,
        tasks: Arc<[FileScanTask]>,
    ) -> IcebergResult<ArrowRecordBatchIterator> {
        self.build_scan(row_locations, None)?
            .to_arrow_with_shared_tasks_and_filter(tasks, self.row_filter.clone())
            .map_err(IcebergError::from)
    }

    /// Construct a query-only cursor without an AM/FDW discriminator in the
    /// per-row path.
    pub(crate) fn open_query_cursor(&mut self) -> IcebergResult<IcebergQueryCursor> {
        let tasks = self.planned_query_tasks()?;
        let source = ArrowBatchSource::new(IcebergArrowBatches(
            self.read_planned_tasks(RowLocationProjection::Exclude, tasks)?,
        ));
        Ok(IcebergQueryCursor::new(source, self.plan.decoder()))
    }

    /// Plan the whole logical snapshot for an adapter's ANALYZE implementation.
    pub(crate) fn analyze_input(&self) -> IcebergResult<AnalyzeScanInput> {
        let scan = self.build_scan(RowLocationProjection::Include, None)?;
        let tasks = scan.plan_files()?;
        Ok(AnalyzeScanInput {
            scan,
            tasks,
            decoder: self.plan.decoder(),
            storage_bytes: self.storage_bytes.ok_or(
                IcebergError::InvariantViolated(
                    "ANALYZE ScanSpec is missing storage-byte statistics",
                ),
            )?,
        })
    }

    /// Open a row-location-bearing reader for a writable adapter. Synthetic
    /// ctid registration and executor binding remain adapter responsibilities.
    pub(crate) fn mutation_input(&mut self) -> IcebergResult<MutationScanInput> {
        let tasks = self.planned_mutation_tasks()?;
        let source =
            ArrowBatchSource::new(IcebergArrowBatches(self.read_planned_tasks(
                RowLocationProjection::Include,
                tasks.shared_tasks(),
            )?));
        Ok(MutationScanInput {
            source,
            decoder: self.plan.decoder(),
        })
    }

    /// Build the Iceberg [`TableScan`] for this spec's projection and optional
    /// planning filter.
    fn build_scan(
        &self,
        row_locations: RowLocationProjection,
        filter: Option<&Predicate>,
    ) -> IcebergResult<TableScan> {
        let mut builder = self.table.scan();
        builder = match row_locations {
            RowLocationProjection::Exclude => builder
                .select_field_ids(self.plan.project_field_ids().iter().copied()),
            RowLocationProjection::Include => builder.select_field_ids(
                self.plan
                    .project_field_ids()
                    .iter()
                    .copied()
                    .chain([RESERVED_FIELD_ID_FILE, RESERVED_FIELD_ID_POS]),
            ),
        };
        if let Some(predicate) = filter {
            builder = builder.with_filter(predicate.clone());
        }
        if let Some(delta) = self.delta.as_ref() {
            builder = builder.with_delta(Arc::clone(delta));
        }
        Ok(builder.build()?)
    }

    pub(crate) fn starting_snapshot_id(&self) -> Option<i64> {
        self.table.metadata().current_snapshot_id()
    }
}
