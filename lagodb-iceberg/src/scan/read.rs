//! Statement-bound Iceberg reads and PostgreSQL row materialization.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_lite::expr::Predicate;
use iceberg_lite::metadata_columns::{RESERVED_FIELD_ID_FILE, RESERVED_FIELD_ID_POS};
use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::scan::{ArrowRecordBatchIterator, FileScanTask, TableScan};
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use lagodb_arrow::{ArrowBatchSource, ArrowColumnDecoder};
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};
use crate::schema::column_mapping::{PgRowProjection, ReadProjection};
use crate::schema::relation::RelationShape;

use super::PgRowCursor;
use super::batch::{
    AnalyzeBatchSource, ArrowBatches, InterruptibleArrowBatches, ScanBatchSource,
};
use super::projection::Projection;
use super::query::{QuerySourceBinding, QueryTaskPlanner};

/// Iceberg transaction view captured once for a statement.
pub(crate) struct IcebergReadSnapshot {
    table: Table,
    delta: Option<Arc<SnapshotDelta>>,
}

impl IcebergReadSnapshot {
    pub(crate) fn new(table: Table, delta: Option<Arc<SnapshotDelta>>) -> Self {
        Self { table, delta }
    }

    pub(crate) fn schema(&self) -> &Arc<IcebergSchema> {
        self.table.metadata().current_schema()
    }
}

/// Predicate proven statement-stable by its PostgreSQL planning adapter.
pub(crate) struct StablePruningPredicate(Option<Predicate>);

impl StablePruningPredicate {
    pub(crate) fn new(predicate: Option<Predicate>) -> Self {
        Self(predicate)
    }
}

/// Exact reader predicate for the currently bound executor values.
pub(crate) struct ReaderPredicate(Option<Predicate>);

impl ReaderPredicate {
    pub(crate) fn new(predicate: Option<Predicate>) -> Self {
        Self(predicate)
    }
}

/// Validated predicate roles for one statement-bound read.
pub(crate) struct ScanPredicates {
    stable_pruning: StablePruningPredicate,
    current_reader: ReaderPredicate,
}

impl ScanPredicates {
    pub(crate) fn unfiltered() -> Self {
        Self::same(None)
    }

    pub(crate) fn same(predicate: Option<Predicate>) -> Self {
        Self {
            stable_pruning: StablePruningPredicate::new(predicate.clone()),
            current_reader: ReaderPredicate::new(predicate),
        }
    }

    pub(crate) fn new(
        stable_pruning: StablePruningPredicate,
        current_reader: ReaderPredicate,
    ) -> Self {
        Self {
            stable_pruning,
            current_reader,
        }
    }
}

/// Storage-facing, statement-bound Iceberg query read.
///
/// This layer contains no PostgreSQL datum decoder. Query Offload can therefore
/// bind field ids and Arrow schema without constructing a decoder it discards.
pub(crate) struct PreparedIcebergRead {
    table: Table,
    projection: ReadProjection,
    predicates: ScanPredicates,
    delta: Option<Arc<SnapshotDelta>>,
    query_tasks: Option<Arc<[FileScanTask]>>,
}

/// Typed zero-column source used only by scalar `COUNT(*)` offload.
pub(crate) struct CountRowsRead(PreparedIcebergRead);

impl CountRowsRead {
    pub(crate) fn bind_query_source(self) -> IcebergResult<QuerySourceBinding> {
        self.0.bind_query_source()
    }
}

impl PreparedIcebergRead {
    /// Build the zero-column source needed by scalar `COUNT(*)` offload.
    ///
    /// The normal Iceberg reader still establishes row visibility through
    /// delete descriptors and the transaction overlay; only output columns and
    /// PostgreSQL datum materialization are omitted.
    pub(crate) fn count_rows(snapshot: IcebergReadSnapshot) -> CountRowsRead {
        let projection = ReadProjection::row_only(snapshot.schema().clone());
        CountRowsRead(Self::from_parts(
            snapshot,
            projection,
            ScanPredicates::unfiltered(),
        ))
    }

    pub(crate) fn projected_fields(
        snapshot: IcebergReadSnapshot,
        project_field_ids: Box<[i32]>,
        predicates: ScanPredicates,
    ) -> Self {
        let projection = ReadProjection::from_field_ids(
            snapshot.schema().clone(),
            project_field_ids,
        );
        Self::from_parts(snapshot, projection, predicates)
    }

    fn from_parts(
        snapshot: IcebergReadSnapshot,
        projection: ReadProjection,
        predicates: ScanPredicates,
    ) -> Self {
        Self {
            table: snapshot.table,
            projection,
            predicates,
            delta: snapshot.delta,
            query_tasks: None,
        }
    }

    fn replace_predicates(&mut self, predicates: ScanPredicates) {
        let stable_changed =
            self.predicates.stable_pruning.0 != predicates.stable_pruning.0;
        if stable_changed {
            self.query_tasks = None;
        }
        self.predicates = predicates;
    }

    pub(crate) fn rebind_reader_filter(&mut self, predicate: ReaderPredicate) {
        self.predicates.current_reader = predicate;
    }

    pub(crate) fn schema_id(&self) -> i32 {
        self.projection.schema().schema_id()
    }

    pub(crate) fn planned_query_tasks(
        &mut self,
    ) -> IcebergResult<Arc<[FileScanTask]>> {
        if let Some(tasks) = self.query_tasks.as_ref() {
            return Ok(Arc::clone(tasks));
        }
        let tasks = self
            .build_scan(
                RowLocationProjection::Exclude,
                self.predicates.stable_pruning.0.as_ref(),
            )?
            .plan_files()?;
        let tasks = Arc::from(tasks.into_boxed_slice());
        self.query_tasks = Some(Arc::clone(&tasks));
        Ok(tasks)
    }

    fn read_planned_tasks(
        &self,
        row_locations: RowLocationProjection,
        tasks: Arc<[FileScanTask]>,
    ) -> IcebergResult<ArrowRecordBatchIterator> {
        self.build_scan(row_locations, None)?
            .to_arrow_with_shared_tasks_and_filter(
                tasks,
                self.predicates.current_reader.0.clone(),
            )
            .map_err(IcebergError::from)
    }

    pub(crate) fn query_arrow_schema(
        &self,
    ) -> IcebergResult<arrow_schema::SchemaRef> {
        Ok(Arc::new(self.projection.query_arrow_schema()?))
    }

    pub(crate) fn table_properties(&self) -> &HashMap<String, String> {
        self.table.metadata().properties()
    }

    pub(crate) fn file_io(&self) -> iceberg_lite::io::FileIO {
        self.table.file_io().clone()
    }

    pub(crate) fn starting_snapshot_id(&self) -> Option<i64> {
        self.table.metadata().current_snapshot_id()
    }

    pub(crate) fn bind_query_source(self) -> IcebergResult<QuerySourceBinding> {
        let arrow_schema = Arc::new(self.projection.query_arrow_schema()?);
        let field_ids = self.projection.project_field_ids().into();
        let scan = self.build_scan(
            RowLocationProjection::Exclude,
            self.predicates.stable_pruning.0.as_ref(),
        )?;
        let task_planner = QueryTaskPlanner::new(
            self.table,
            field_ids,
            self.predicates.stable_pruning.0,
            self.delta,
        );
        Ok(QuerySourceBinding {
            scan,
            arrow_schema,
            row_filter: self.predicates.current_reader.0,
            task_planner,
        })
    }

    fn build_scan(
        &self,
        row_locations: RowLocationProjection,
        filter: Option<&Predicate>,
    ) -> IcebergResult<TableScan> {
        let mut builder = self.table.scan();
        builder = match row_locations {
            RowLocationProjection::Exclude => builder.select_field_ids(
                self.projection.project_field_ids().iter().copied(),
            ),
            RowLocationProjection::Include => builder.select_field_ids(
                self.projection
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
}

/// PostgreSQL row-producing read composed from a storage read and decoder.
///
/// It owns no mutation lifecycle or mutation task cache. Mutation adapters use
/// its row-location planning and reader primitives while retaining their own
/// task inventory.
pub(crate) struct PreparedRowScan {
    read: PreparedIcebergRead,
    decoder: ArrowColumnDecoder,
}

impl PreparedRowScan {
    pub(crate) fn full(
        snapshot: IcebergReadSnapshot,
        predicates: ScanPredicates,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let projection = PgRowProjection::full(snapshot.schema().clone(), shape)?;
        Ok(Self::from_projection(snapshot, projection, predicates))
    }

    pub(crate) fn projected(
        snapshot: IcebergReadSnapshot,
        projection: Projection,
        predicates: ScanPredicates,
        shape: &RelationShape,
        scan_attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let projection = PgRowProjection::projected(
            snapshot.schema().clone(),
            shape,
            &projection,
            scan_attr_types.len(),
            scan_attr_types,
        )?;
        Ok(Self::from_projection(snapshot, projection, predicates))
    }

    fn from_projection(
        snapshot: IcebergReadSnapshot,
        projection: PgRowProjection,
        predicates: ScanPredicates,
    ) -> Self {
        let (projection, decoder) = projection.into_parts();
        Self {
            read: PreparedIcebergRead::from_parts(snapshot, projection, predicates),
            decoder,
        }
    }

    pub(crate) fn replace_predicates(&mut self, predicates: ScanPredicates) {
        self.read.replace_predicates(predicates);
    }

    pub(crate) fn rebind_reader_filter(&mut self, predicate: ReaderPredicate) {
        self.read.rebind_reader_filter(predicate);
    }

    pub(crate) fn schema_id(&self) -> i32 {
        self.read.schema_id()
    }

    pub(crate) fn planned_query_tasks(
        &mut self,
    ) -> IcebergResult<Arc<[FileScanTask]>> {
        self.read.planned_query_tasks()
    }

    pub(crate) fn open_row_cursor(&mut self) -> IcebergResult<PgRowCursor> {
        let tasks = self.read.planned_query_tasks()?;
        self.open_row_cursor_with_tasks(tasks)
    }

    pub(crate) fn open_analyze_row_cursor(
        &mut self,
    ) -> IcebergResult<PgRowCursor<AnalyzeBatchSource>> {
        let tasks = self.read.planned_query_tasks()?;
        let source = ArrowBatchSource::new(InterruptibleArrowBatches(
            self.read
                .read_planned_tasks(RowLocationProjection::Exclude, tasks)?,
        ));
        Ok(PgRowCursor::new(source, self.decoder.clone()))
    }

    pub(crate) fn open_row_cursor_with_tasks(
        &self,
        tasks: Arc<[FileScanTask]>,
    ) -> IcebergResult<PgRowCursor> {
        let source = ArrowBatchSource::new(ArrowBatches(
            self.read
                .read_planned_tasks(RowLocationProjection::Exclude, tasks)?,
        ));
        Ok(PgRowCursor::new(source, self.decoder.clone()))
    }

    pub(crate) fn query_arrow_schema(
        &self,
    ) -> IcebergResult<arrow_schema::SchemaRef> {
        self.read.query_arrow_schema()
    }

    pub(crate) fn table_properties(&self) -> &HashMap<String, String> {
        self.read.table_properties()
    }

    pub(crate) fn file_io(&self) -> iceberg_lite::io::FileIO {
        self.read.file_io()
    }

    pub(crate) fn starting_snapshot_id(&self) -> Option<i64> {
        self.read.starting_snapshot_id()
    }

    pub(crate) fn analyze_input(
        &self,
        storage_bytes: u64,
    ) -> IcebergResult<AnalyzeScanInput> {
        let scan = self.read.build_scan(RowLocationProjection::Include, None)?;
        let tasks = scan.plan_files()?;
        Ok(AnalyzeScanInput {
            scan,
            tasks,
            decoder: self.decoder.clone(),
            storage_bytes,
        })
    }

    /// Plan the row-location-bearing task inventory needed by a concrete
    /// mutation adapter. Ownership and caching of that inventory belong to the
    /// adapter's mutation lifecycle.
    pub(crate) fn plan_row_location_tasks(&self) -> IcebergResult<Vec<FileScanTask>> {
        self.read
            .build_scan(
                RowLocationProjection::Include,
                self.read.predicates.stable_pruning.0.as_ref(),
            )?
            .plan_files()
            .map_err(IcebergError::from)
    }

    /// Open a row-location-bearing reader over an adapter-owned task inventory.
    pub(crate) fn open_row_location_scan(
        &self,
        tasks: Arc<[FileScanTask]>,
    ) -> IcebergResult<RowLocationScanInput> {
        let source = ArrowBatchSource::new(ArrowBatches(
            self.read
                .read_planned_tasks(RowLocationProjection::Include, tasks)?,
        ));
        Ok(RowLocationScanInput {
            source,
            decoder: self.decoder.clone(),
        })
    }
}

#[derive(Clone, Copy)]
enum RowLocationProjection {
    Exclude,
    Include,
}

/// Shared ANALYZE planning output.
pub(crate) struct AnalyzeScanInput {
    pub(crate) scan: TableScan,
    pub(crate) tasks: Vec<FileScanTask>,
    pub(crate) decoder: ArrowColumnDecoder,
    pub(crate) storage_bytes: u64,
}

/// Low-level row-location reader input. Managed and foreign mutation adapters
/// bind it to their own distinct identity and lifecycle objects.
pub(crate) struct RowLocationScanInput {
    pub(crate) source: ScanBatchSource,
    pub(crate) decoder: ArrowColumnDecoder,
}
