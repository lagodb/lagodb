//! Statement-scoped Iceberg scan description and file-task planning.

use std::rc::Rc;
use std::sync::Arc;

use iceberg_lite::expr::Predicate;
use iceberg_lite::metadata_columns::{RESERVED_FIELD_ID_FILE, RESERVED_FIELD_ID_POS};
use iceberg_lite::overlay::SnapshotDelta;
use iceberg_lite::scan::{ArrowRecordBatchIterator, FileScanTask, TableScan};
use iceberg_lite::spec::Schema as IcebergSchema;
use iceberg_lite::table::Table;
use pg_arrow_conv::ArrowBatchSource;
use pg_lakebase_core::access::mutation::ModifyScanBinding;
use pg_lakebase_core::prelude::*;
use pgrx::pg_sys;

use super::batch::IcebergArrowBatches;
use super::cursor::IcebergBatchCursor;
use super::tasks::PlannedMutationTasks;
use crate::access::analyze::AnalyzePreparation;
use crate::access::column_mapping::ScanColumns;
use crate::access::isolation::PgTransactionIsolation;
use crate::access::mutation::IcebergModifyQueryState;
use crate::access::projection::Projection;
use crate::catalog::bridge::IcebergTableId;
use crate::catalog::metadata_tracker::TxMetadata;
use crate::error::{IcebergError, IcebergResult};
use crate::relation_binding::RelationShape;
use crate::storage::StorageContext;

/// Statement-scoped scan state: snapshot, bound columns, predicates, and
/// planned-task caches.
///
/// `ScanSpec::build*` is the only place metadata is read from storage during a
/// scan; `scan_rescan` reuses the spec and rebuilds only the cursor.
///
/// `pub(crate)` so the CustomScan provider in [`crate::customscan`] can build a
/// `ScanSpec` from a runtime-built [`Predicate`], reusing the same scan core in
/// both the TableAM seqscan and CustomScan paths.
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
enum ScanMetadataPurpose {
    Query,
    Analyze,
}

#[derive(Clone, Copy)]
enum RowLocationProjection {
    Exclude,
    Include,
}

pub(crate) struct LoadedScanMetadata {
    table: Table,
    schema: Arc<IcebergSchema>,
    delta: Option<Arc<SnapshotDelta>>,
    storage_bytes: Option<u64>,
}

impl LoadedScanMetadata {
    pub(crate) fn load_query(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
    ) -> IcebergResult<Self> {
        Self::load(rel_oid, spc_oid, ScanMetadataPurpose::Query)
    }

    pub(crate) fn schema(&self) -> &Arc<IcebergSchema> {
        &self.schema
    }

    fn load(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        purpose: ScanMetadataPurpose,
    ) -> IcebergResult<Self> {
        PgTransactionIsolation::current()?;
        let ctx = StorageContext::for_tablespace(spc_oid)?;
        let loaded =
            TxMetadata::current().current_table_metadata(rel_oid, ctx.file_io())?;
        let schema = loaded.metadata.current_schema().clone();
        let storage_bytes = match purpose {
            ScanMetadataPurpose::Query => None,
            ScanMetadataPurpose::Analyze => {
                Some(loaded.relation_stats(ctx.file_io())?.1)
            }
        };

        let table = Table::builder()
            .file_io(ctx.file_io().clone())
            .metadata_location(loaded.location)
            .metadata(loaded.metadata)
            .identifier(IcebergTableId::for_relation(rel_oid).into_table_ident())
            .build()?;

        Ok(Self {
            table,
            schema,
            delta: loaded.delta,
            storage_bytes,
        })
    }
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
            Self::build_full(rel_oid, spc_oid, shape, ScanMetadataPurpose::Query)?;

        // Translate keys after the schema is in hand (the translator needs
        // Iceberg type / field-id info from parsed metadata).
        let filter = scan_keys_to_predicate(keys, spec.plan.schema())?;
        spec.planning_filter.clone_from(&filter);
        spec.row_filter = filter;
        Ok(spec)
    }

    pub(super) fn build_for_analyze(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        Self::build_full(rel_oid, spc_oid, shape, ScanMetadataPurpose::Analyze)
    }

    /// Build the full-column CustomScan plan.
    pub(crate) fn build_for_custom_scan(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        planning_filter: Option<Predicate>,
        row_filter: Option<Predicate>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let LoadedScanMetadata {
            table,
            schema,
            delta,
            storage_bytes,
        } = LoadedScanMetadata::load_query(rel_oid, spc_oid)?;
        let plan = ScanColumns::new(schema, shape)?;
        Ok(Self {
            table,
            plan,
            planning_filter,
            row_filter,
            delta,
            storage_bytes,
            query_tasks: None,
            mutation_tasks: None,
        })
    }

    fn build_full(
        rel_oid: pg_sys::Oid,
        spc_oid: pg_sys::Oid,
        shape: &RelationShape,
        purpose: ScanMetadataPurpose,
    ) -> IcebergResult<Self> {
        let LoadedScanMetadata {
            table,
            schema,
            delta,
            storage_bytes,
        } = LoadedScanMetadata::load(rel_oid, spc_oid, purpose)?;
        let plan = ScanColumns::new(schema, shape)?;
        Ok(Self {
            table,
            plan,
            planning_filter: None,
            row_filter: None,
            delta,
            storage_bytes,
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
        let LoadedScanMetadata {
            table,
            schema,
            delta,
            storage_bytes,
        } = LoadedScanMetadata::load_query(rel_oid, spc_oid)?;
        let plan = ScanColumns::with_projection(
            schema,
            shape,
            &projection,
            scan_attr_types.len(),
            scan_attr_types,
        )?;
        Ok(Self {
            table,
            plan,
            planning_filter,
            row_filter,
            delta,
            storage_bytes,
            query_tasks: None,
            mutation_tasks: None,
        })
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

    /// Replace only the current row predicate. Used by CustomScan rescans when
    /// `PARAM_EXEC` values change; the stable planned file-task set remains a
    /// safe superset.
    pub(crate) fn set_row_filter(&mut self, predicate: Option<Predicate>) {
        self.row_filter = predicate;
    }

    #[inline]
    pub(crate) fn schema_id(&self) -> i32 {
        self.plan.schema().schema_id()
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

    /// Construct a fresh slot-first [`IcebergBatchCursor`] for the TableAM scan.
    pub(crate) fn open_batch_cursor(&mut self) -> IcebergResult<IcebergBatchCursor> {
        let tasks = self.planned_query_tasks()?;
        let source = ArrowBatchSource::new(IcebergArrowBatches(
            self.read_planned_tasks(RowLocationProjection::Exclude, tasks)?,
        ));
        Ok(IcebergBatchCursor::query(source, self.plan.decoder()))
    }

    /// Plan the whole logical snapshot for PostgreSQL ANALYZE. Sampling is
    /// intentionally deferred until PostgreSQL supplies its ReadStream tickets.
    pub(crate) fn prepare_analyze(
        &self,
        #[cfg(not(feature = "pg17"))] statistics_target: i32,
    ) -> IcebergResult<AnalyzePreparation> {
        let scan = self.build_scan(RowLocationProjection::Include, None)?;
        let tasks = scan.plan_files()?;
        AnalyzePreparation::try_new(
            scan,
            tasks,
            self.plan.decoder(),
            self.storage_bytes.ok_or(IcebergError::InvariantViolated(
                "ANALYZE ScanSpec is missing storage-byte statistics",
            ))?,
            #[cfg(not(feature = "pg17"))]
            statistics_target,
        )
    }

    /// Open the provider cursor for `ScanPurpose::Modify`. The cursor consumes
    /// Iceberg's metadata columns to produce the executor's synthetic `ctid`.
    pub(crate) fn open_mutation_batch_cursor(
        &mut self,
        binding: ModifyScanBinding<IcebergModifyQueryState>,
        table_oid: pg_sys::Oid,
    ) -> IcebergResult<IcebergBatchCursor> {
        let tasks = self.planned_mutation_tasks()?;
        let source =
            ArrowBatchSource::new(IcebergArrowBatches(self.read_planned_tasks(
                RowLocationProjection::Include,
                tasks.shared_tasks(),
            )?));
        Ok(IcebergBatchCursor::mutation(
            source,
            self.plan.decoder(),
            binding,
            table_oid,
        ))
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

    /// Re-translate the current effective [`OwnedScanKeys`] into a filter.
    pub(super) fn refresh_filter(
        &mut self,
        keys: &OwnedScanKeys,
    ) -> IcebergResult<()> {
        let filter = scan_keys_to_predicate(keys, self.plan.schema())?;
        self.set_filter(filter);
        Ok(())
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
