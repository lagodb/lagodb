//! Backend coverage for the executor state wrapper's raw-pointer contract.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::lakebase_core::customscan::support::TestScanState;
    use pg_lakebase_core::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
        PathContext, PathVariant, PlanTranslateContext, ReScanContext,
        RelationContext,
    };
    use pg_lakebase_core::customscan::provider::{CustomScanPrivate, NoPrivateData};
    use pg_lakebase_core::expr::QualPushdownDecision;
    use pgrx::pg_sys;
    use pgrx::pg_test;

    struct StateProvider;

    impl LakebaseCustomScanProvider for StateProvider {
        const NAME: &'static core::ffi::CStr = c"state-wrapper-test-provider";
        type PrivateData = NoPrivateData;
        type State = ();

        fn supports_relation(_ctx: &RelationContext<'_>) -> bool {
            false
        }

        fn classify_predicate(
            _ctx: &PlanTranslateContext,
            _predicate: &pg_lakebase_core::expr::predicate::PlanPredicate,
        ) -> QualPushdownDecision {
            QualPushdownDecision::Unsupported
        }

        fn create_path(
            _ctx: &PathContext<'_>,
            _variant: &PathVariant<'_>,
            _builder: CustomPathBuilder<Self>,
        ) -> Option<CustomPathPlan<Self>> {
            None
        }

        fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {}

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn next_slot(
            _ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            Ok(false)
        }

        fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            Ok(())
        }
    }

    // Keep this bound in the test module so a future provider fixture cannot
    // accidentally stop satisfying the private-data contract.
    fn assert_private_data_contract<T: CustomScanPrivate>() {}

    #[pg_test]
    fn custom_scan_state_base_pointer_round_trips() {
        assert_private_data_contract::<NoPrivateData>();

        let mut state = unsafe { TestScanState::<StateProvider>::new() };
        let node = state.node_ptr();
        let base = unsafe { state.base_mut() as *mut pg_sys::CustomScanState };

        assert_eq!(
            base, node,
            "CustomScanStateWrapper::base must remain the first field"
        );
    }
}
