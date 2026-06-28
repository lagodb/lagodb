//! Iceberg provider SPI implementation and runtime collaborators.

mod projection;
mod scan_state;

use core::ffi::CStr;

use pg_lakebase_core::customscan::provider::{
    BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
    CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
    NoPrivateData, PathVariant, PlanTranslateContext, ReScanContext, RelPathContext,
};
use pg_lakebase_core::expr::predicate::PlanPredicate;
use pg_lakebase_core::expr::split::QualPushdownDecision;
use pgrx::pg_sys;

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
        IcebergPredicateClassifier::default().classify_predicate(ctx, predicate)
    }

    /// Emit a CustomPath when core reports pushed predicates; scale baseline cost
    /// by [`PathVariant::pushdown`] selectivity so the planner sees pruning benefit.
    fn create_path(
        ctx: &RelPathContext,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>> {
        if !variant.pushdown.has_pushed_predicates() {
            return None;
        }

        let fraction =
            crate::gucs::scan_fraction(variant.pushdown.pruning_selectivity);

        Some(
            builder
                .scanned_pages(ctx.baserel_pages() * fraction)
                .scanned_tuples(ctx.baserel_tuples() * fraction)
                .build(NoPrivateData),
        )
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
