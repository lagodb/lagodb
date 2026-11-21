//! Parquet foreign tables through the provider-neutral table-scan SPI.

use std::rc::Rc;

use arrow_schema::SchemaRef;
use lagodb_arrow::scan::{
    PlannedScan, PlannedScanTasks, ScanPlanningContext, ScanProjection,
    ScanStreamOptions, ScanSupport, ScanTaskPlanningOptions, TableScanAdapter,
    TableScanProvider, TableScanWorkerAdapter, TableScanWorkerProvider,
    WorkerSourcePayload, WorkerStreamOptions,
};
use lagodb_core::expr::pushdown::PredicatePlan;
use lagodb_core::handles::{ExecutorHandle, RelationGuard};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::ScanCost;
use lagodb_core::runtime_api::{
    RuntimePruningPredicate, SourceWorkId, TableScanRoutes,
};
use pgrx::pg_sys;

use crate::CONNECTOR_FDW_NAME;
use crate::fdw::{LagodbConnectors, ResolvedForeignRelation};
use crate::format::parquet::{ParquetBoundPredicate, ParquetRuntimePlanner};
use crate::format::{FormatKind, ParquetFormat};

use super::columns::ScanColumns;
use super::error::Error;
use super::plan::{Plan, PlanProjection};
use super::scan::{BoundScan, PlannedTasks};
use super::stream::{EvolvingFilters, Stream, TaskSplits};
use super::worker::{WorkerPayload, WorkerSource};

pub(super) struct Provider;

static PROVIDER: Provider = Provider;

impl TableScanProvider for Provider {
    const ROUTES: TableScanRoutes =
        TableScanRoutes::foreign_data_wrapper(CONNECTOR_FDW_NAME);
    type Filter = LagodbConnectors;
    type Predicate = ParquetBoundPredicate;
    type ScanPlan = Plan;
    type BoundScan = BoundScan;
    type PlannedTasks = PlannedTasks;
    type Stream = Stream;
    type Error = Error;

    fn owns_foreign_server(&self, server_oid: pg_sys::Oid) -> bool {
        LagodbConnectors::owns_server(server_oid)
    }

    fn plan_scan(
        &self,
        context: &ScanPlanningContext<'_>,
    ) -> Result<ScanSupport<PlannedScan<Self::ScanPlan>>, Self::Error> {
        let relation_oid = context.relation_oid();
        if ResolvedForeignRelation::resolve(relation_oid)?.kind()
            != FormatKind::Parquet
        {
            return Ok(ScanSupport::Unsupported);
        }
        let projection = match context.projection() {
            ScanProjection::RowCount => PlanProjection::CountRows,
            ScanProjection::Columns(attnos) => PlanProjection::Columns(attnos.into()),
        };
        // The planner already holds the relation's lock.
        let relation = RelationGuard::open_table(
            relation_oid,
            pg_sys::NoLock as pg_sys::LOCKMODE,
        )?;
        let Some(columns) =
            ScanColumns::load(&relation.as_handle(), projection.attnos())?
        else {
            return Ok(ScanSupport::Unsupported);
        };
        // The foreign-scan path's estimate, without pruning credit: row-group
        // statistics are unknown until the files are read.
        let cost = ScanCost::try_new(
            context.relation_rows(),
            context.relation_physical_bytes(),
            ParquetFormat::SCAN_STARTUP_COST,
        )?;
        Ok(ScanSupport::Planned(PlannedScan::new(
            Plan::new(relation_oid, context.check_as_user_id(), projection),
            cost,
            SchemaRef::clone(columns.schema()),
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
        _executor: &ExecutorHandle<'_>,
    ) -> Result<Self::BoundScan, Self::Error> {
        plan.bind()
    }

    fn bound_schema(&self, bound: &Self::BoundScan) -> SchemaRef {
        SchemaRef::clone(bound.schema())
    }

    fn plan_predicate(
        &self,
        bound: &Self::BoundScan,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error> {
        Ok(ParquetRuntimePlanner::new(bound.schema()).plan(predicate)?)
    }

    fn plan_scan_tasks(
        &self,
        bound: &Self::BoundScan,
        options: &ScanTaskPlanningOptions,
        static_predicates: &[&Self::Predicate],
        runtime_predicate: Option<&Self::Predicate>,
    ) -> Result<PlannedScanTasks<Self::PlannedTasks>, Self::Error> {
        let filters = static_predicates
            .iter()
            .copied()
            .chain(runtime_predicate)
            .cloned()
            .collect();
        let planned = bound.plan_tasks(options.projection(), filters)?;
        let exact_rows = if options.requests_exact_rows() {
            bound.exact_rows(&planned)?
        } else {
            None
        };
        let metrics = planned.metrics();
        Ok(PlannedScanTasks::new(planned, metrics).with_exact_rows(exact_rows))
    }

    fn open_stream(
        &self,
        bound: &Self::BoundScan,
        planned: &Self::PlannedTasks,
        options: ScanStreamOptions,
    ) -> Result<Self::Stream, Self::Error> {
        let evolving = options.has_evolving_predicate().then(|| {
            EvolvingFilters::new(
                options,
                SchemaRef::clone(bound.schema()),
                planned.filters().into(),
            )
        });
        Stream::open(
            planned.columns(),
            TaskSplits::new(
                Rc::clone(planned.input()),
                planned.whole_files(),
                evolving,
            ),
            planned.filters().into(),
            options.maximum_batch_rows(),
        )
    }
}

impl TableScanWorkerProvider for Provider {
    type WorkerSource = WorkerSource;
    type WorkerStream = Stream;

    fn prepare_worker_source(
        &self,
        bound: &Self::BoundScan,
        planned: &Self::PlannedTasks,
    ) -> Result<ScanSupport<WorkerSourcePayload>, Self::Error> {
        let (groups, split_count) = planned.worker_groups()?;
        let work_count = u32::try_from(groups.len())
            .map_err(|_| Error::worker_payload("worker group count exceeds u32"))?;
        let mut task_metrics = planned.metrics();
        task_metrics.planned_tasks = split_count as u64;
        let bytes = WorkerPayload::encode(bound, planned, groups)?;
        let payload = WorkerSourcePayload::new(bytes, work_count, task_metrics)
            .map_err(Error::worker_payload)?;
        Ok(ScanSupport::Planned(payload))
    }

    unsafe fn decode_worker_source(
        &self,
        payload: &[u8],
    ) -> Result<Self::WorkerSource, Self::Error> {
        // The decoded source copies everything it keeps from `payload`.
        WorkerSource::decode(payload)
    }

    fn open_worker_stream(
        &self,
        source: &Self::WorkerSource,
        work_ids: &[SourceWorkId],
        options: WorkerStreamOptions,
    ) -> Result<Self::WorkerStream, Self::Error> {
        source.open(work_ids, options.maximum_batch_rows())
    }
}

pub(crate) fn register() {
    TableScanAdapter::register(&PROVIDER);
    TableScanWorkerAdapter::register(&PROVIDER);
}
