//! Iceberg implementation of the provider-neutral table-scan SPI.

use arrow_schema::SchemaRef;
use iceberg_lite::expr::Predicate;
use lagodb_arrow::scan::{
    PlannedScan, PlannedScanTasks, ScanPlanningContext, ScanProjection,
    ScanStreamOptions, ScanSupport, ScanTaskPlanningOptions, TableScanAdapter,
    TableScanProvider, TableScanWorkerAdapter, TableScanWorkerProvider,
    WorkerSourcePayload, WorkerStreamOptions,
};
use lagodb_core::expr::pushdown::{FilterPlanningContext, PredicatePlan};
use lagodb_core::fdw::{FdwScan, ForeignDataWrapper};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::ScanCost;
use lagodb_core::runtime_api::{
    RuntimePruningPredicate, SourceWorkId, TableScanRoutes,
};
use pgrx::pg_sys;

use crate::config::scan_fraction;
use crate::error::IcebergError;
use crate::foreign_table::{ForeignPlanningSource, LagodbIceberg};
use crate::managed_table::ICEBERG_AM_NAME;
use crate::scan::ScanError;
use crate::scan::columnar::PlannedScan as PlannedQueryScan;
use crate::scan::parallel::WorkerSource;

use super::error::Error;
use super::filter::{Filter, FilterPlanner};
use super::plan::{BoundScan, Plan, PlanProjection};
use super::stream::{Stream as QueryStream, WorkerStream};
use super::worker::ReopenPlan;

pub(super) struct Provider;

static PROVIDER: Provider = Provider;

// Foreign bind must resolve the REST table before it can produce a row, and
// parallel workers may resolve it again while rebuilding FileIO. Match the
// native FDW's `REST_SCAN_STARTUP_COST` heuristic until both planners obtain
// startup cost from a shared, source-owned cost policy.
const FOREIGN_REST_SCAN_STARTUP_COST: f64 = 100.0;

impl Provider {
    fn foreign_planning_source<'a>(
        &self,
        context: &'a ScanPlanningContext<'_>,
    ) -> &'a ForeignPlanningSource {
        // SAFETY: PostgreSQL initializes every foreign base relation through
        // the selected FDW's GetForeignRelSize before join/upper path planning.
        // Provider routing already matched this relation's Iceberg handler,
        // and this synchronous callback does not overlap a mutable FDW callback.
        let state = unsafe { LagodbIceberg::planning_state(context.relation()) };
        state.planning_source()
    }

    fn scan_cost(
        &self,
        context: &ScanPlanningContext<'_>,
    ) -> Result<ScanCost, Error> {
        let fraction = scan_fraction(context.pruning_selectivity());
        let startup_cost = if context.relation_kind() == pg_sys::RELKIND_FOREIGN_TABLE
        {
            FOREIGN_REST_SCAN_STARTUP_COST
        } else {
            0.0
        };
        ScanCost::try_new(
            context.relation_rows() * fraction,
            context.relation_physical_bytes() * fraction,
            startup_cost,
        )
        .map_err(Into::into)
    }
}

impl TableScanProvider for Provider {
    const ROUTES: TableScanRoutes =
        TableScanRoutes::access_method_and_foreign_data_wrapper(
            ICEBERG_AM_NAME,
            LagodbIceberg::NAME,
        );
    type Filter = Filter;
    type Predicate = Predicate;
    type ScanPlan = Plan;
    type BoundScan = BoundScan;
    type PlannedTasks = PlannedQueryScan;
    type Stream = QueryStream;
    type Error = Error;

    fn owns_foreign_server(&self, server_oid: pg_sys::Oid) -> bool {
        LagodbIceberg::handles_server(server_oid)
    }

    fn begin_filter_planning(
        &self,
        scan: &ScanPlanningContext<'_>,
        context: &FilterPlanningContext,
    ) -> Result<FilterPlanner, Self::Error> {
        match scan.relation_kind() {
            pg_sys::RELKIND_RELATION => FilterPlanner::managed(context),
            pg_sys::RELKIND_FOREIGN_TABLE => {
                FilterPlanner::foreign(context, self.foreign_planning_source(scan))
            }
            _ => Err(IcebergError::InvariantViolated(
                "query filter planning received a non-table relation",
            )
            .into()),
        }
    }

    fn plan_scan(
        &self,
        context: &ScanPlanningContext<'_>,
    ) -> Result<ScanSupport<PlannedScan<Self::ScanPlan>>, Self::Error> {
        let projection = match context.projection() {
            ScanProjection::RowCount => PlanProjection::CountRows,
            ScanProjection::Columns(attnos) => PlanProjection::Columns(attnos.into()),
        };
        let plan = match context.relation_kind() {
            pg_sys::RELKIND_RELATION => {
                Plan::managed(context.relation_oid(), projection)
            }
            pg_sys::RELKIND_FOREIGN_TABLE => Plan::foreign(
                context.relation_oid(),
                context.check_as_user_id(),
                self.foreign_planning_source(context).identity().clone(),
                projection,
            ),
            _ => return Ok(ScanSupport::Unsupported),
        };
        Ok(ScanSupport::Planned(PlannedScan::new(
            plan,
            self.scan_cost(context)?,
        )))
    }

    fn encode_scan_plan(
        &self,
        plan: &Self::ScanPlan,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error> {
        plan.encode(writer);
        Ok(())
    }

    fn decode_scan_plan(
        &self,
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self::ScanPlan, Self::Error> {
        Plan::decode(reader)
    }

    fn bind_scan(
        &self,
        plan: &Self::ScanPlan,
    ) -> Result<Self::BoundScan, Self::Error> {
        plan.bind()
    }

    fn bound_schema(&self, bound: &Self::BoundScan) -> SchemaRef {
        bound.scan.schema()
    }

    fn plan_predicate(
        &self,
        bound: &Self::BoundScan,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error> {
        Ok(bound.scan.plan_predicate(predicate)?)
    }

    fn plan_scan_tasks(
        &self,
        bound: &Self::BoundScan,
        options: &ScanTaskPlanningOptions,
        static_predicates: &[&Self::Predicate],
        runtime_predicate: Option<&Self::Predicate>,
    ) -> Result<PlannedScanTasks<Self::PlannedTasks>, Self::Error> {
        let static_filter = static_predicates
            .iter()
            .map(|predicate| (*predicate).clone())
            .reduce(Predicate::and);
        let planned = bound.scan.plan_tasks(
            options.projection(),
            static_filter,
            runtime_predicate.cloned(),
        )?;
        let metrics = planned.metrics();
        Ok(PlannedScanTasks::new(planned, metrics))
    }

    fn open_stream(
        &self,
        bound: &Self::BoundScan,
        planned: &Self::PlannedTasks,
        options: ScanStreamOptions,
    ) -> Result<Self::Stream, Self::Error> {
        let batch_size =
            usize::try_from(options.maximum_batch_rows()).map_err(|_| {
                Error::BatchRowLimit {
                    value: options.maximum_batch_rows(),
                }
            })?;
        Ok(QueryStream::new(
            bound.scan.open_stream(planned, batch_size, options)?,
        ))
    }
}

impl TableScanWorkerProvider for Provider {
    type WorkerSource = WorkerSource;
    type WorkerStream = WorkerStream;

    fn prepare_worker_source(
        &self,
        bound: &Self::BoundScan,
        planned: &Self::PlannedTasks,
    ) -> Result<ScanSupport<WorkerSourcePayload>, Self::Error> {
        let grouped = bound.scan.task_grouping()?.group(planned.tasks())?;
        let work_count = u32::try_from(grouped.group_count()).map_err(|_| {
            ScanError::WorkerPayload("worker group count exceeds u32".to_owned())
        })?;
        if work_count == 0 {
            return Ok(ScanSupport::Unsupported);
        }
        let mut task_metrics = planned.metrics();
        task_metrics.planned_tasks = grouped.range_count() as u64;
        let prefix = bound.worker.encode_prefix()?;
        let bytes = WorkerSource::encode_prefixed(
            &prefix,
            planned.schema(),
            planned.tasks(),
            grouped,
        )?;
        let payload = WorkerSourcePayload::new(bytes, work_count, task_metrics)
            .map_err(|message| ScanError::WorkerPayload(message.to_owned()))?;
        Ok(ScanSupport::Planned(payload))
    }

    unsafe fn decode_worker_source(
        &self,
        payload: &[u8],
    ) -> Result<Self::WorkerSource, Self::Error> {
        let (plan, inventory) = ReopenPlan::decode(payload)?;
        let file_io = plan.file_io()?;
        Ok(unsafe { WorkerSource::decode_shared(inventory, file_io) }?)
    }

    fn open_worker_stream(
        &self,
        source: &Self::WorkerSource,
        work_ids: &[SourceWorkId],
        options: WorkerStreamOptions,
    ) -> Result<Self::WorkerStream, Self::Error> {
        let batch_size =
            usize::try_from(options.maximum_batch_rows()).map_err(|_| {
                Error::BatchRowLimit {
                    value: options.maximum_batch_rows(),
                }
            })?;
        Ok(WorkerStream::new(source.open(work_ids, batch_size)?))
    }
}

pub(crate) fn register() {
    TableScanAdapter::register(&PROVIDER);
    TableScanWorkerAdapter::register(&PROVIDER);
}
