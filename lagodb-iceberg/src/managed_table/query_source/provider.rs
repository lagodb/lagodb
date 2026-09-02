//! Managed-Iceberg implementation of the provider-neutral source SPI.

use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::query_contract::{SourceEstimate, SourceId};
use pg_arrow_conv::{
    PlannedSource, QuerySourceAdapter, QuerySourceProvider, SourcePlanningContext,
    SourceProjection, SourceStreamOptions, SourceSupport,
};

use crate::managed_table::catalog::IcebergAccessMethod;

use super::{
    IcebergArrowStream, IcebergQuerySourceError, IcebergSourcePlan,
    PreparedIcebergSource,
};

pub(super) struct IcebergQuerySourceProvider;

static ICEBERG_QUERY_SOURCE: IcebergQuerySourceProvider = IcebergQuerySourceProvider;

impl IcebergQuerySourceProvider {
    fn estimate_count_rows(
        &self,
        context: &SourcePlanningContext<'_>,
    ) -> Result<SourceEstimate, IcebergQuerySourceError> {
        SourceEstimate::try_new(
            context.relation_rows(),
            context.relation_physical_bytes(),
        )
        .map_err(Into::into)
    }
}

impl QuerySourceProvider for IcebergQuerySourceProvider {
    type SourcePlan = IcebergSourcePlan;
    type PreparedSource = PreparedIcebergSource;
    type SerialStream = IcebergArrowStream;
    type Error = IcebergQuerySourceError;

    fn plan_source(
        &self,
        context: &SourcePlanningContext<'_>,
    ) -> Result<SourceSupport<PlannedSource<Self::SourcePlan>>, Self::Error> {
        if !IcebergAccessMethod::matches_oid(context.access_method_oid()) {
            return Ok(SourceSupport::NotOwned);
        }
        let planned = match context.projection() {
            SourceProjection::CountRows => {
                let plan = IcebergSourcePlan::scalar_count(
                    context.source(),
                    context.relation_oid(),
                    context.tablespace_oid(),
                )?;
                PlannedSource::new(plan, self.estimate_count_rows(context)?)
            }
        };
        Ok(SourceSupport::Planned(planned))
    }

    fn encode_source_plan(
        &self,
        plan: &Self::SourcePlan,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error> {
        plan.encode(writer);
        Ok(())
    }

    fn decode_source_plan(
        &self,
        source: SourceId,
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self::SourcePlan, Self::Error> {
        Ok(IcebergSourcePlan::decode(reader, source)?)
    }

    fn prepare_source(
        &self,
        plan: &Self::SourcePlan,
    ) -> Result<Self::PreparedSource, Self::Error> {
        Ok(plan.prepare()?)
    }

    fn open_serial_stream(
        &self,
        prepared: &Self::PreparedSource,
        options: SourceStreamOptions,
    ) -> Result<Self::SerialStream, Self::Error> {
        let batch_size =
            usize::try_from(options.maximum_batch_rows()).map_err(|_| {
                IcebergQuerySourceError::BatchRowLimit {
                    value: options.maximum_batch_rows(),
                }
            })?;
        Ok(prepared.open_stream(batch_size))
    }
}

pub(crate) fn register() {
    QuerySourceAdapter::register(&ICEBERG_QUERY_SOURCE);
}
