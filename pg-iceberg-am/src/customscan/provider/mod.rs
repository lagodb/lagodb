//! Iceberg provider SPI implementation and runtime collaborators.

mod projection;
mod scan_state;

use core::ffi::CStr;

use pg_lakebase_core::customscan::provider::{
    BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
    CustomScanError, EndContext, LakebaseCustomModifyProvider,
    LakebaseCustomScanProvider, ModifyBindContext, ModifyCapabilities,
    NextSlotContext, NoPrivateData, PathVariant, PlanTranslateContext, ReScanContext,
    RelPathContext,
};
use pg_lakebase_core::expr::predicate::PlanPredicate;
use pg_lakebase_core::expr::split::QualPushdownDecision;
use pgrx::pg_sys;

use crate::IcebergTableAm;
use crate::catalog::IcebergAccessMethod;
use crate::error::IcebergError;
use crate::predicate::IcebergPredicateClassifier;

pub use scan_state::IcebergScanState;

/// Zero-sized marker for the Iceberg [`LakebaseCustomScanProvider`].
pub struct IcebergCustomScanProvider;

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
    fn supports_relation(ctx: &RelPathContext) -> bool {
        // Defense-in-depth: refuse non-relation RTEs even if the router is bypassed.
        if ctx.rtekind() != pg_sys::RTEKind::RTE_RELATION {
            return false;
        }

        // Defense-in-depth: only concrete heap-shaped storage relkinds.
        if !matches!(
            ctx.relkind(),
            pg_sys::RELKIND_RELATION
                | pg_sys::RELKIND_MATVIEW
                | pg_sys::RELKIND_TOASTVALUE
        ) {
            return false;
        }

        let Some(iceberg_am_oid) = IcebergAccessMethod::oid() else {
            return false;
        };

        ctx.access_method_oid() == iceberg_am_oid
    }

    fn classify_predicate(
        ctx: &PlanTranslateContext,
        predicate: &PlanPredicate,
    ) -> QualPushdownDecision {
        IcebergPredicateClassifier.classify_predicate(ctx, predicate)
    }

    /// Query paths require pushed predicates. Modify paths remain eligible even
    /// when the pushdown set is empty so projection pruning can compete with
    /// the standard TableAM path through normal costing.
    fn create_path(
        ctx: &RelPathContext,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>> {
        if !variant.purpose.is_modify() && !variant.pushdown.has_pushed_predicates() {
            return None;
        }

        let fraction =
            crate::gucs::scan_fraction(variant.pushdown.pruning_selectivity);

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

    fn supports_modify_target(ctx: &RelPathContext) -> bool {
        ctx.rtekind() == pg_sys::RTEKind::RTE_RELATION
            && matches!(
                ctx.relkind(),
                pg_sys::RELKIND_RELATION | pg_sys::RELKIND_PARTITIONED_TABLE
            )
            && IcebergAccessMethod::oid()
                .is_some_and(|oid| ctx.access_method_oid() == oid)
    }

    fn modify_scan_context(
        state: &Self::State,
    ) -> Option<crate::access::mutation::IcebergModifyScanContext> {
        state.modify_scan_context()
    }
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
