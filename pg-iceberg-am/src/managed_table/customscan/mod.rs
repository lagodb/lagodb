//! Iceberg CustomScan provider and predicate pushdown implementation.

mod projection;
mod scan_state;

#[cfg(feature = "pg_test")]
mod pg_test;

use core::ffi::CStr;

use pg_lakebase_core::customscan::modify::{
    LakebaseCustomModifyProvider, ModifyBindContext, ModifyCapabilities,
    register_provider as register_modify_provider,
};
use pg_lakebase_core::customscan::provider::{
    BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
    CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
    NoPrivateData, PathContext, PathVariant, ReScanContext, RelationContext,
    register_provider as register_scan_provider,
};
use pg_lakebase_core::expr::pushdown::{
    FilterBindResult, FilterPlanningContext, FilterPushdown, FilterValueBindings,
};
use pg_lakebase_core::plan_data::{PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::engine::predicate::{
    BoundIcebergPredicate, IcebergFilterError, IcebergFilterPlanner,
    PlannedIcebergPredicate,
};
use crate::error::IcebergError;
use crate::managed_table::IcebergTableAm;
use crate::managed_table::access::mutation::IcebergModifyScanContext;
use crate::managed_table::access::scan::LoadedScanMetadata;
use crate::managed_table::catalog::IcebergAccessMethod;
use crate::managed_table::gucs::scan_fraction;

use scan_state::IcebergScanState;

/// Zero-sized marker for the Iceberg [`LakebaseCustomScanProvider`].
struct IcebergCustomScanProvider;

impl FilterPushdown for IcebergCustomScanProvider {
    type Planner = IcebergFilterPlanner;
    type PlannedPredicate = PlannedIcebergPredicate;
    type BoundPredicate = BoundIcebergPredicate;
    type Error = IcebergFilterError;

    fn begin_filter_planning(
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        let metadata = LoadedScanMetadata::load_query(
            context.relation_oid(),
            context.tablespace_oid(),
        )?;
        IcebergFilterPlanner::from_schema(context, metadata.schema())
    }

    fn encode_planned(
        predicate: &Self::PlannedPredicate,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error> {
        predicate.encode(writer);
        Ok(())
    }

    fn decode_planned(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self::PlannedPredicate, Self::Error> {
        PlannedIcebergPredicate::decode(reader, binding_count)
    }

    fn bind_filter(
        predicate: &Self::PlannedPredicate,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
        predicate.bind(values)
    }
}

impl From<IcebergError> for CustomScanError {
    fn from(err: IcebergError) -> Self {
        CustomScanError::provider(err)
    }
}

impl LakebaseCustomScanProvider for IcebergCustomScanProvider {
    const NAME: &'static CStr = c"pg-iceberg-am";

    type PrivateData = NoPrivateData;
    type State = IcebergScanState;

    /// True when the relation uses the Iceberg access method.
    fn supports_relation(ctx: &RelationContext<'_>) -> bool {
        IcebergAccessMethod::matches_oid(ctx.access_method_oid())
    }

    /// Query paths require provider-planned filters. Modify paths remain eligible
    /// when the planned set is empty so projection pruning can compete with
    /// the standard TableAM path through normal costing.
    fn create_path(
        ctx: &PathContext<'_>,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>> {
        if !variant.purpose.is_modify() && !variant.pushdown.has_planned_filters() {
            return None;
        }

        let fraction = scan_fraction(variant.pushdown.pruning_selectivity);

        let builder = builder
            .scanned_pages(ctx.baserel_pages() * fraction)
            .scanned_tuples(ctx.baserel_tuples() * fraction);
        Some(builder.build(NoPrivateData))
    }

    fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
        IcebergScanState::default()
    }

    fn begin(ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
        IcebergScanState::begin(ctx)
    }

    fn next_slot(ctx: NextSlotContext<'_, Self>) -> Result<bool, CustomScanError> {
        IcebergScanState::next_slot(ctx)
    }

    fn rescan(ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
        IcebergScanState::rescan(ctx)
    }

    fn end(ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
        IcebergScanState::end(ctx)
    }
}

impl LakebaseCustomModifyProvider for IcebergCustomScanProvider {
    type AccessMethod = IcebergTableAm;

    const MODIFY_NAME: &'static CStr = c"LakebaseModifyTable";

    const MODIFY_CAPABILITIES: ModifyCapabilities = ModifyCapabilities::NONE;

    fn bind_modify(ctx: ModifyBindContext<'_, Self>) -> Result<(), CustomScanError> {
        IcebergScanState::bind_modify(ctx)
    }

    fn supports_modify_target(ctx: &RelationContext<'_>) -> bool {
        matches!(
            ctx.relkind(),
            pg_sys::RELKIND_RELATION | pg_sys::RELKIND_PARTITIONED_TABLE
        ) && IcebergAccessMethod::matches_oid(ctx.access_method_oid())
    }

    fn modify_scan_context(state: &Self::State) -> Option<IcebergModifyScanContext> {
        state.modify_scan_context()
    }
}

/// Register the Iceberg provider once from `_PG_init`.
pub(super) fn register() {
    register_scan_provider::<IcebergCustomScanProvider>();
    register_modify_provider::<IcebergCustomScanProvider>();
}

#[cfg(test)]
mod sqlstate_tests {
    use pg_lakebase_core::customscan::provider::CustomScanError;
    use pg_lakebase_core::diag::SqlStateError;
    use pgrx::prelude::PgSqlErrorCode;

    use crate::error::IcebergError;

    #[test]
    fn iceberg_error_sqlstate_preserved_through_custom_scan_error() {
        let err: CustomScanError =
            IcebergError::ColumnNotFound("missing_col".into()).into();
        assert_eq!(
            err.sql_error_code(),
            PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN
        );

        let err: CustomScanError =
            IcebergError::NotImplemented("scan feature").into();
        assert_eq!(
            err.sql_error_code(),
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED
        );
    }
}
