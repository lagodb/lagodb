//! Iceberg CustomScan provider and predicate pushdown implementation.

mod execution;
mod projection;
mod scan_state;

#[cfg(feature = "pg_test")]
mod pg_test;

use core::ffi::CStr;

use lagodb_core::customscan::modify::{
    LagodbCustomModifyProvider, ModifyBindContext, ModifyCapabilities,
    register_provider as register_modify_provider,
};
use lagodb_core::customscan::provider::{
    BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
    CustomScanError, EndContext, LagodbCustomScanProvider, NextSlotContext,
    NextSlotResult, NoPrivateData, PathContext, PathVariant, ReScanContext,
    RelationContext, register_provider as register_scan_provider,
};
use lagodb_core::expr::RuntimeValueBindings;
use lagodb_core::expr::pushdown::{
    FilterBindResult, FilterPlanningContext, FilterPushdown,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;

use crate::config::scan_fraction;
use crate::error::IcebergError;
use crate::managed_table::IcebergTableAm;
use crate::managed_table::ManagedTableSnapshot;
use crate::managed_table::access::mutation::IcebergModifyScanContext;
use crate::managed_table::catalog::IcebergAccessMethod;
use crate::managed_table::catalog::metadata_tracker::TxMetadata;
use crate::predicate::{
    BoundIcebergPredicate, IcebergFilterError, IcebergFilterPlanner,
    PlannedIcebergPredicate,
};

use scan_state::IcebergScanState;

/// Zero-sized marker for the Iceberg [`LagodbCustomScanProvider`].
pub(crate) struct IcebergCustomScanProvider;

impl FilterPushdown for IcebergCustomScanProvider {
    type Planner = IcebergFilterPlanner;
    type PlannedPredicate = PlannedIcebergPredicate;
    type BoundPredicate = BoundIcebergPredicate;
    type Error = IcebergFilterError;

    fn begin_filter_planning(
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        let metadata = ManagedTableSnapshot::load_query(
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
        values: RuntimeValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
        predicate.bind(values)
    }
}

impl From<IcebergError> for CustomScanError {
    fn from(err: IcebergError) -> Self {
        CustomScanError::provider(err)
    }
}

impl From<crate::scan::ScanError> for CustomScanError {
    fn from(err: crate::scan::ScanError) -> Self {
        CustomScanError::provider(err)
    }
}

impl LagodbCustomScanProvider for IcebergCustomScanProvider {
    const NAME: &'static CStr = c"lagodb-iceberg";
    const NATIVE_PARALLEL: bool = true;
    const SUPPRESS_TABLE_AM_PARALLEL_SCAN: bool = true;

    type PrivateData = NoPrivateData;
    type State = IcebergScanState;

    /// True when the relation uses the Iceberg access method.
    fn supports_relation(ctx: &RelationContext<'_>) -> bool {
        IcebergAccessMethod::matches_oid(ctx.access_method_oid())
    }

    /// Query paths remain eligible without a pushed filter so PostgreSQL can
    /// build native-parallel partial scans for ordinary relation reads. Any
    /// unsupported predicate remains a PostgreSQL residual on the CustomScan.
    fn create_path(
        ctx: &PathContext<'_>,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>> {
        let fraction = scan_fraction(variant.pushdown.pruning_selectivity);

        let worker_reconstructible = !TxMetadata::has_local_actions(ctx.rel_oid());
        let builder = builder
            .scanned_pages(ctx.baserel_pages() * fraction)
            .scanned_tuples(ctx.baserel_tuples() * fraction)
            // A complete path has no DSM callback. It is safe in a PG worker
            // only when that worker can reconstruct the committed view without
            // losing this backend's transaction-local Iceberg actions.
            .parallel_safe_complete(worker_reconstructible)
            .native_parallel_partial(worker_reconstructible);
        Some(builder.build(NoPrivateData))
    }

    fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
        IcebergScanState::default()
    }

    fn begin(ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
        IcebergScanState::begin(ctx)
    }

    fn next_slot<'a>(
        ctx: NextSlotContext<'a, Self>,
    ) -> Result<NextSlotResult<'a>, CustomScanError> {
        IcebergScanState::next_slot(ctx)
    }

    fn rescan(ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
        IcebergScanState::rescan(ctx)
    }

    fn end(ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
        IcebergScanState::end(ctx)
    }

    fn estimate_dsm(
        state: &mut Self::State,
    ) -> Result<pg_sys::Size, CustomScanError> {
        state.estimate_dsm()
    }

    unsafe fn initialize_dsm(
        state: &mut Self::State,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { state.initialize_dsm(coordinate) }
    }

    unsafe fn reinitialize_dsm(
        state: &mut Self::State,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { state.reinitialize_dsm(coordinate) }
    }

    unsafe fn initialize_worker(
        state: &mut Self::State,
        _toc: *mut pg_sys::shm_toc,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), CustomScanError> {
        unsafe { state.initialize_worker(coordinate) }
    }

    fn shutdown_parallel(state: &mut Self::State) -> Result<(), CustomScanError> {
        state.shutdown_parallel();
        Ok(())
    }
}

impl LagodbCustomModifyProvider for IcebergCustomScanProvider {
    type AccessMethod = IcebergTableAm;

    const MODIFY_NAME: &'static CStr = c"LagoDBModifyTable";

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
    use lagodb_core::customscan::provider::CustomScanError;
    use lagodb_core::diag::SqlStateError;
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
