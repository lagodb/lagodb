//! Statement binding and run-local task plans for Iceberg scans.

use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use iceberg_lite::Result as IcebergLiteResult;
use iceberg_lite::expr::Predicate;
use iceberg_lite::scan::{ArrowRecordBatchIterator, FileScanTask, TableScan};
use lagodb_arrow::scan::ScanStreamOptions;
use lagodb_core::expr::pushdown::PredicatePlan;
use lagodb_core::runtime_api::{RuntimePruningPredicate, TableScanTaskMetrics};

use super::{ArrowStream, runtime_predicate::IcebergPredicatePlanner};
use crate::error::{IcebergError, IcebergResult};
use crate::scan::parallel::{TaskGrouping, TaskGroupingConfig};
use crate::scan::{
    IcebergTaskMetrics, ScanError, ScanSourceBinding, ScanTaskPlanner,
};

/// Immutable statement snapshot and schema binding. Physical tasks are
/// deliberately absent and are planned only after DataFusion optimization.
#[derive(Debug)]
pub(super) struct StatementScan {
    pub(super) scan: TableScan,
    pub(super) arrow_schema: SchemaRef,
    pub(super) row_filter: Option<Predicate>,
    task_planner: ScanTaskPlanner,
    planned_tasks: Mutex<Option<CachedTaskSet>>,
}

impl StatementScan {
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
pub(crate) struct BoundScan {
    scan: Arc<StatementScan>,
    task_grouping: TaskGroupingConfig,
}

impl BoundScan {
    pub(crate) fn new(
        input: ScanSourceBinding,
        task_grouping: TaskGroupingConfig,
    ) -> Self {
        let scan = StatementScan {
            scan: input.scan,
            arrow_schema: input.arrow_schema,
            row_filter: input.row_filter,
            task_planner: input.task_planner,
            planned_tasks: Mutex::new(None),
        };
        Self {
            scan: Arc::new(scan),
            task_grouping,
        }
    }

    pub(crate) fn plan_tasks(
        &self,
        projection: &[usize],
        static_filter: Option<Predicate>,
        runtime_filter: Option<Predicate>,
    ) -> IcebergResult<PlannedScan> {
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
            return Ok(PlannedScan {
                task_set: Arc::new(TaskSet::new(tasks)),
                row_filter: self.row_filter(additional_filter),
                schema: projected_schema,
            });
        }
        // Projection and static DataFusion predicates are statement-stable, so
        // the most recently requested inventory can be reused across rescans.
        let mut cached = self.scan.planned_tasks.lock().map_err(|_| {
            IcebergError::InvariantViolated(
                "Iceberg scan task-plan cache was poisoned",
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
                let task_set = Arc::new(TaskSet::new(tasks));
                *cached = Some(CachedTaskSet {
                    projection: projection.into(),
                    static_filter: static_filter.clone(),
                    task_set: Arc::clone(&task_set),
                });
                task_set
            }
        };
        Ok(PlannedScan {
            task_set,
            row_filter: self.row_filter(static_filter),
            schema: projected_schema,
        })
    }

    pub(crate) fn plan_predicate(
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

    pub(crate) fn open_stream(
        &self,
        planned: &PlannedScan,
        batch_size: usize,
        options: ScanStreamOptions,
    ) -> Result<ArrowStream, ScanError> {
        ArrowStream::new(
            Arc::clone(&self.scan),
            Arc::clone(&planned.task_set.tasks),
            planned.row_filter.clone(),
            Arc::clone(&planned.schema),
            batch_size,
            options,
        )
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.scan.arrow_schema)
    }

    pub(crate) fn task_grouping(&self) -> Result<TaskGrouping, ScanError> {
        self.task_grouping.resolve()
    }
}

/// Run-local handle retaining either the shared statement-stable inventory or
/// a QueryRun-owned inventory narrowed by a complete runtime predicate.
pub(crate) struct PlannedScan {
    task_set: Arc<TaskSet>,
    row_filter: Option<Predicate>,
    schema: SchemaRef,
}

#[derive(Debug)]
struct CachedTaskSet {
    projection: Box<[usize]>,
    static_filter: Option<Predicate>,
    task_set: Arc<TaskSet>,
}

#[derive(Debug)]
struct TaskSet {
    tasks: Arc<[FileScanTask]>,
    metrics: TableScanTaskMetrics,
}

impl TaskSet {
    fn new(tasks: Arc<[FileScanTask]>) -> Self {
        let metrics = tasks.as_ref().task_metrics();
        Self { tasks, metrics }
    }
}

impl PlannedScan {
    pub(crate) fn metrics(&self) -> TableScanTaskMetrics {
        self.task_set.metrics
    }

    pub(crate) fn tasks(&self) -> &Arc<[FileScanTask]> {
        &self.task_set.tasks
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}
