//! Managed-Iceberg implementation of the provider-neutral table-scan SPI.

use arrow_schema::SchemaRef;
use lagodb_arrow::query_source::{
    PlannedScan, ScanPlanningContext, ScanProjection, ScanStreamOptions, ScanSupport,
    TableScanAdapter, TableScanProvider,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{ScanEstimate, ScanId};

use crate::engine::predicate::BoundIcebergPredicate;
use crate::managed_table::catalog::IcebergAccessMethod;
use crate::managed_table::customscan::IcebergCustomScanProvider;

use super::{
    IcebergArrowStream, IcebergScanPlan, IcebergTableScanError,
    PreparedIcebergTableScan,
};

pub(super) struct IcebergTableScanProvider;

static ICEBERG_TABLE_SCAN: IcebergTableScanProvider = IcebergTableScanProvider;

impl IcebergTableScanProvider {
    fn estimate_count_rows(
        &self,
        context: &ScanPlanningContext<'_>,
    ) -> Result<ScanEstimate, IcebergTableScanError> {
        ScanEstimate::try_new(
            context.relation_rows(),
            context.relation_physical_bytes(),
        )
        .map_err(Into::into)
    }
}

impl TableScanProvider for IcebergTableScanProvider {
    type Filter = IcebergCustomScanProvider;
    type ScanPlan = IcebergScanPlan;
    type PreparedScan = PreparedIcebergTableScan;
    type SerialStream = IcebergArrowStream;
    type Error = IcebergTableScanError;

    fn plan_scan(
        &self,
        context: &ScanPlanningContext<'_>,
    ) -> Result<ScanSupport<PlannedScan<Self::ScanPlan>>, Self::Error> {
        if !IcebergAccessMethod::matches_oid(context.access_method_oid()) {
            return Ok(ScanSupport::NotOwned);
        }
        let planned = match context.projection() {
            ScanProjection::RowCount => {
                let plan = IcebergScanPlan::scalar_count(
                    context.scan(),
                    context.relation_oid(),
                    context.tablespace_oid(),
                );
                PlannedScan::new(plan, self.estimate_count_rows(context)?)
            }
            ScanProjection::Columns(attnos) => {
                let plan = IcebergScanPlan::columns(
                    context.scan(),
                    context.relation_oid(),
                    context.tablespace_oid(),
                    attnos,
                );
                PlannedScan::new(plan, self.estimate_count_rows(context)?)
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
        scan: ScanId,
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self::ScanPlan, Self::Error> {
        Ok(IcebergScanPlan::decode(reader, scan)?)
    }

    fn prepare_scan(
        &self,
        plan: &Self::ScanPlan,
        predicate: Option<&BoundIcebergPredicate>,
    ) -> Result<Self::PreparedScan, Self::Error> {
        plan.prepare(predicate)
    }

    fn prepared_schema(&self, prepared: &Self::PreparedScan) -> SchemaRef {
        prepared.schema()
    }

    fn open_serial_stream(
        &self,
        prepared: &Self::PreparedScan,
        options: ScanStreamOptions,
    ) -> Result<Self::SerialStream, Self::Error> {
        let batch_size =
            usize::try_from(options.maximum_batch_rows()).map_err(|_| {
                IcebergTableScanError::BatchRowLimit {
                    value: options.maximum_batch_rows(),
                }
            })?;
        Ok(prepared.open_stream(batch_size))
    }
}

pub(crate) fn register() {
    TableScanAdapter::register(&ICEBERG_TABLE_SCAN);
}
