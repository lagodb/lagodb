//! Statement binding and run-local task plans for managed-Iceberg scans.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use iceberg_lite::Result as IcebergLiteResult;
use iceberg_lite::expr::Predicate;
use iceberg_lite::scan::{ArrowRecordBatchIterator, FileScanTask, TableScan};
use lagodb_arrow::query_source::ScanStreamOptions;
use lagodb_core::expr::pushdown::PredicatePlan;
use lagodb_core::runtime_api::{RuntimePruningPredicate, TableScanTaskMetrics};

use super::{
    IcebergArrowStream, IcebergTableScanError,
    runtime_predicate::IcebergPredicatePlanner,
};
use crate::engine::scan::{BoundQueryScanInput, QueryTaskPlanner};
use crate::error::{IcebergError, IcebergResult};

/// Immutable statement snapshot and schema binding. Physical tasks are
/// deliberately absent and are planned only after DataFusion optimization.
#[derive(Debug)]
pub(super) struct BoundIcebergScan {
    pub(super) scan: TableScan,
    pub(super) arrow_schema: SchemaRef,
    pub(super) row_filter: Option<Predicate>,
    task_planner: QueryTaskPlanner,
    planned_tasks: Mutex<Option<CachedIcebergTaskSet>>,
}

impl BoundIcebergScan {
    pub(super) fn open_batches(
        &self,
        tasks: Arc<[FileScanTask]>,
        row_filter: Option<Predicate>,
        batch_size: usize,
    ) -> IcebergLiteResult<ArrowRecordBatchIterator> {
        self.scan
            .to_arrow_with_shared_tasks_and_filter_and_batch_size(
                tasks, row_filter, batch_size,
            )
    }

    pub(super) fn plan_predicate(
        &self,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        IcebergPredicatePlanner::new(
            self.arrow_schema.as_ref(),
            self.task_planner.field_ids(),
        )
        .plan(predicate)
    }
}

/// Statement-owned Iceberg scan binding.
///
/// The retained values contain no direct PostgreSQL plan/executor node,
/// Relation, MemoryContext, Datum, or borrowed backend pointer. `TableScan`
/// owns the exact snapshot/schema/overlay reader context used later to produce
/// tasks; its `FileIO` may encapsulate a backend-thread storage service behind
/// the private Iceberg trait adapter. Consequently this value is not an
/// independently thread-safe capability even though the upstream trait bounds
/// require it to be `Send + Sync`.
#[derive(Debug)]
pub(crate) struct BoundIcebergTableScan {
    scan: Arc<BoundIcebergScan>,
}

impl BoundIcebergTableScan {
    pub(super) fn new(input: BoundQueryScanInput) -> Self {
        let scan = BoundIcebergScan {
            scan: input.scan,
            arrow_schema: input.arrow_schema,
            row_filter: input.row_filter,
            task_planner: input.task_planner,
            planned_tasks: Mutex::new(None),
        };
        Self {
            scan: Arc::new(scan),
        }
    }

    pub(super) fn plan_tasks(
        &self,
        projection: &[usize],
        static_filter: Option<Predicate>,
        runtime_filter: Option<Predicate>,
    ) -> IcebergResult<PlannedIcebergTableScan> {
        let projected_field_ids = projection
            .iter()
            .map(|position| {
                self.scan.task_planner.field_ids().get(*position).copied().ok_or(
                    IcebergError::InvariantViolated(
                        "Iceberg execution projection exceeds the bound source schema",
                    ),
                )
            })
            .collect::<IcebergResult<Vec<_>>>()?;
        let projected_schema = Arc::new(
            self.scan
                .arrow_schema
                .project(projection)
                .map_err(IcebergError::from)?,
        );
        if let Some(runtime_filter) = runtime_filter {
            // A complete HashJoin predicate belongs to this QueryRun. Its task
            // subset must never enter the statement cache because a ReScan can
            // rebuild the join with different keys.
            let additional_filter = Some(match static_filter {
                Some(static_filter) => Predicate::and(static_filter, runtime_filter),
                None => runtime_filter,
            });
            let tasks = Arc::from(
                self.scan
                    .task_planner
                    .plan_files(&projected_field_ids, additional_filter.as_ref())?
                    .into_boxed_slice(),
            );
            return Ok(PlannedIcebergTableScan {
                task_set: Arc::new(IcebergTaskSet::new(tasks)),
                row_filter: self.row_filter(additional_filter),
                schema: projected_schema,
            });
        }
        // Projection and static DataFusion predicates are statement-stable, so
        // the most recently requested inventory can be reused across rescans.
        let mut cached = self.scan.planned_tasks.lock().map_err(|_| {
            IcebergError::InvariantViolated(
                "Iceberg query task-plan cache was poisoned",
            )
        })?;
        let task_set = match cached.as_ref() {
            Some(cached)
                if cached.projection.as_ref() == projection
                    && cached.static_filter.as_ref() == static_filter.as_ref() =>
            {
                Arc::clone(&cached.task_set)
            }
            _ => {
                let tasks = Arc::from(
                    self.scan
                        .task_planner
                        .plan_files(&projected_field_ids, static_filter.as_ref())?
                        .into_boxed_slice(),
                );
                let task_set = Arc::new(IcebergTaskSet::new(tasks));
                *cached = Some(CachedIcebergTaskSet {
                    projection: projection.into(),
                    static_filter: static_filter.clone(),
                    task_set: Arc::clone(&task_set),
                });
                task_set
            }
        };
        Ok(PlannedIcebergTableScan {
            task_set,
            row_filter: self.row_filter(static_filter),
            schema: projected_schema,
        })
    }

    pub(super) fn plan_predicate(
        &self,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> IcebergResult<PredicatePlan<Predicate>> {
        self.scan.plan_predicate(predicate)
    }

    fn row_filter(&self, additional: Option<Predicate>) -> Option<Predicate> {
        match (self.scan.row_filter.as_ref(), additional) {
            (Some(base), Some(additional)) => {
                Some(Predicate::and(base.clone(), additional))
            }
            (Some(base), None) => Some(base.clone()),
            (None, additional) => additional,
        }
    }

    pub(super) fn open_stream(
        &self,
        planned: &PlannedIcebergTableScan,
        batch_size: usize,
        options: ScanStreamOptions,
    ) -> Result<IcebergArrowStream, IcebergTableScanError> {
        IcebergArrowStream::new(
            Arc::clone(&self.scan),
            Arc::clone(&planned.task_set.tasks),
            planned.row_filter.clone(),
            Arc::clone(&planned.schema),
            batch_size,
            options,
        )
    }

    pub(super) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.scan.arrow_schema)
    }
}

/// Run-local handle retaining either the shared statement-stable inventory or
/// a QueryRun-owned inventory narrowed by a complete runtime predicate.
pub(crate) struct PlannedIcebergTableScan {
    task_set: Arc<IcebergTaskSet>,
    row_filter: Option<Predicate>,
    schema: SchemaRef,
}

#[derive(Debug)]
struct CachedIcebergTaskSet {
    projection: Box<[usize]>,
    static_filter: Option<Predicate>,
    task_set: Arc<IcebergTaskSet>,
}

#[derive(Debug)]
struct IcebergTaskSet {
    tasks: Arc<[FileScanTask]>,
    metrics: TableScanTaskMetrics,
}

impl IcebergTaskSet {
    fn new(tasks: Arc<[FileScanTask]>) -> Self {
        let planned_files = tasks
            .iter()
            .map(FileScanTask::data_file_path)
            .collect::<HashSet<_>>()
            .len() as u64;
        let planned_bytes = tasks
            .iter()
            .fold(0_u64, |bytes, task| bytes.saturating_add(task.length));
        let metrics = TableScanTaskMetrics {
            planned_tasks: tasks.len() as u64,
            planned_files,
            planned_bytes,
        };
        Self { tasks, metrics }
    }
}

impl PlannedIcebergTableScan {
    pub(super) fn metrics(&self) -> TableScanTaskMetrics {
        self.task_set.metrics
    }
}
