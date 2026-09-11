//! Managed-Iceberg implementation of the provider-neutral table-scan SPI.

use arrow_schema::SchemaRef;
use iceberg_lite::expr::Predicate;
use lagodb_arrow::query_source::{
    PlannedScan, PlannedScanTasks, ScanPlanningContext, ScanProjection,
    ScanStreamOptions, ScanSupport, ScanTaskPlanningOptions, TableScanAdapter,
    TableScanProvider,
};
use lagodb_core::expr::pushdown::PredicatePlan;
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::ScanCost;
use lagodb_core::runtime_api::{RuntimePruningPredicate, TableScanRoutes};

use crate::managed_table::constants::ICEBERG_AM_NAME;
use crate::managed_table::customscan::IcebergCustomScanProvider;
use crate::managed_table::gucs::scan_fraction;

use super::{
    BoundIcebergTableScan, IcebergArrowStream, IcebergScanPlan,
    IcebergTableScanError, PlannedIcebergTableScan,
};

pub(super) struct IcebergTableScanProvider;

static ICEBERG_TABLE_SCAN: IcebergTableScanProvider = IcebergTableScanProvider;

impl IcebergTableScanProvider {
    fn scan_cost(
        &self,
        context: &ScanPlanningContext<'_>,
    ) -> Result<ScanCost, IcebergTableScanError> {
        let fraction = scan_fraction(context.pruning_selectivity());
        ScanCost::try_new(
            context.relation_rows() * fraction,
            context.relation_physical_bytes() * fraction,
            0.0,
        )
        .map_err(Into::into)
    }
}

impl TableScanProvider for IcebergTableScanProvider {
    const ROUTES: TableScanRoutes = TableScanRoutes::access_method(ICEBERG_AM_NAME);
    type Filter = IcebergCustomScanProvider;
    type Predicate = Predicate;
    type ScanPlan = IcebergScanPlan;
    type BoundScan = BoundIcebergTableScan;
    type PlannedTasks = PlannedIcebergTableScan;
    type SerialStream = IcebergArrowStream;
    type Error = IcebergTableScanError;

    fn plan_scan(
        &self,
        context: &ScanPlanningContext<'_>,
    ) -> Result<ScanSupport<PlannedScan<Self::ScanPlan>>, Self::Error> {
        let planned = match context.projection() {
            ScanProjection::RowCount => {
                let plan = IcebergScanPlan::scalar_count(
                    context.relation_oid(),
                    context.tablespace_oid(),
                );
                PlannedScan::new(plan, self.scan_cost(context)?)
            }
            ScanProjection::Columns(attnos) => {
                let plan = IcebergScanPlan::columns(
                    context.relation_oid(),
                    context.tablespace_oid(),
                    attnos,
                );
                PlannedScan::new(plan, self.scan_cost(context)?)
            }
        };
        Ok(ScanSupport::Planned(planned))
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
        Ok(IcebergScanPlan::decode(reader)?)
    }

    fn bind_scan(
        &self,
        plan: &Self::ScanPlan,
    ) -> Result<Self::BoundScan, Self::Error> {
        plan.bind()
    }

    fn bound_schema(&self, bound: &Self::BoundScan) -> SchemaRef {
        bound.schema()
    }

    fn plan_predicate(
        &self,
        bound: &Self::BoundScan,
        predicate: &RuntimePruningPredicate<'_>,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error> {
        Ok(bound.plan_predicate(predicate)?)
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
        let runtime_filter = runtime_predicate.cloned();
        let planned =
            bound.plan_tasks(options.projection(), static_filter, runtime_filter)?;
        let metrics = planned.metrics();
        Ok(PlannedScanTasks::new(planned, metrics))
    }

    fn open_serial_stream(
        &self,
        bound: &Self::BoundScan,
        planned: &Self::PlannedTasks,
        options: ScanStreamOptions,
    ) -> Result<Self::SerialStream, Self::Error> {
        let batch_size =
            usize::try_from(options.maximum_batch_rows()).map_err(|_| {
                IcebergTableScanError::BatchRowLimit {
                    value: options.maximum_batch_rows(),
                }
            })?;
        bound.open_stream(planned, batch_size, options)
    }
}

pub(crate) fn register() {
    TableScanAdapter::register(&ICEBERG_TABLE_SCAN);
}
