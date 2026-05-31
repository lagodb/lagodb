//! Backend tests for CustomScan method tables, hook signatures, and FFI glue.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::CStr;

    use pg_lakebase_core::customscan::builder::path_methods_for;
    use pg_lakebase_core::customscan::codec::{PrivateDataReader, PrivateDataWriter};
    use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;
    use pg_lakebase_core::customscan::hook::pg_test_assert_set_rel_pathlist_callback_signature;
    use pg_lakebase_core::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
        PathVariant, PlanTranslateContext, ReScanContext, RelPathContext,
    };
    use pg_lakebase_core::customscan::state::{exec_methods_for, scan_methods_for};
    use pg_lakebase_core::expr::runtime_params::ffi_accessors::{
        ExecSetParamPlanFn, ExecSetParamPlanMultiFn, exec_set_param_plan,
        exec_set_param_plan_multi,
    };
    use pg_lakebase_core::expr::split::QualPushdownDecision;
    use pgrx::pg_sys;
    use pgrx::pg_test;

    struct GluePrivate;

    impl CustomScanPrivate for GluePrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(GluePrivate)
        }
    }

    struct GlueStateA;
    struct GlueStateB;

    macro_rules! impl_glue_provider {
        ($ty:ty, $name:expr, $state:ty) => {
            impl LakebaseCustomScanProvider for $ty {
                const NAME: &'static CStr = $name;
                type PrivateData = GluePrivate;
                type State = $state;

                fn supports_relation(_ctx: &RelPathContext) -> bool {
                    false
                }

                fn classify_predicate(
                    _ctx: &PlanTranslateContext,
                    _predicate: &pg_lakebase_core::expr::predicate::PlanPredicate<'_>,
                ) -> QualPushdownDecision {
                    QualPushdownDecision::Unsupported
                }

                fn create_path(
                    _ctx: &RelPathContext,
                    _variant: &PathVariant<'_>,
                    _builder: CustomPathBuilder<Self>,
                ) -> Option<CustomPathPlan<Self>> {
                    None
                }

                fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
                    unreachable!("glue method-table tests do not call create_state");
                }

                fn begin(
                    _ctx: BeginContext<'_, Self>,
                ) -> Result<(), CustomScanError> {
                    unreachable!("glue method-table tests do not call begin");
                }

                fn next_slot(
                    _ctx: NextSlotContext<'_, Self>,
                ) -> Result<bool, CustomScanError> {
                    unreachable!("glue method-table tests do not call next_slot");
                }

                fn rescan(
                    _ctx: ReScanContext<'_, Self>,
                ) -> Result<(), CustomScanError> {
                    unreachable!("glue method-table tests do not call rescan");
                }

                fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
                    unreachable!("glue method-table tests do not call end");
                }
            }
        };
    }

    struct GlueProviderA;
    struct GlueProviderB;

    impl_glue_provider!(GlueProviderA, c"glue-method-tables-provider-a", GlueStateA);
    impl_glue_provider!(GlueProviderB, c"glue-method-tables-provider-b", GlueStateB);

    #[pg_test]
    fn runtime_params_ffi_signatures_typecheck() {
        let set_one = exec_set_param_plan();
        let _: ExecSetParamPlanFn = set_one;
        let set_multi = exec_set_param_plan_multi();
        let _: Option<ExecSetParamPlanMultiFn> = set_multi;
        assert!(
            set_multi.is_some(),
            "PG17 must expose ExecSetParamPlanMulti"
        );
    }

    #[pg_test]
    fn exec_methods_for_is_stable_per_provider() {
        let a1 =
            exec_methods_for::<GlueProviderA>() as *const pg_sys::CustomExecMethods;
        let a2 =
            exec_methods_for::<GlueProviderA>() as *const pg_sys::CustomExecMethods;
        let b1 =
            exec_methods_for::<GlueProviderB>() as *const pg_sys::CustomExecMethods;
        assert_eq!(a1, a2);
        assert_ne!(a1, b1);
    }

    #[pg_test]
    fn exec_methods_for_carries_provider_name() {
        let table = exec_methods_for::<GlueProviderA>();
        let name = unsafe { CStr::from_ptr(table.CustomName) };
        assert_eq!(name, GlueProviderA::NAME);
    }

    #[pg_test]
    fn exec_methods_for_trampoline_wiring() {
        let table = exec_methods_for::<GlueProviderA>();
        assert!(table.BeginCustomScan.is_some());
        assert!(table.ReScanCustomScan.is_some());
        assert!(table.ExecCustomScan.is_some());
        assert!(table.EndCustomScan.is_some());
        assert!(table.ExplainCustomScan.is_some());
    }

    #[pg_test]
    fn scan_methods_for_is_stable_per_provider_and_distinct_across() {
        let a1 =
            scan_methods_for::<GlueProviderA>() as *const pg_sys::CustomScanMethods;
        let a2 =
            scan_methods_for::<GlueProviderA>() as *const pg_sys::CustomScanMethods;
        let b1 =
            scan_methods_for::<GlueProviderB>() as *const pg_sys::CustomScanMethods;
        assert_eq!(a1, a2);
        assert_ne!(a1, b1);
    }

    #[pg_test]
    fn scan_methods_for_carries_name_and_create_state_callback() {
        let table = scan_methods_for::<GlueProviderA>();
        let name = unsafe { CStr::from_ptr(table.CustomName) };
        assert_eq!(name, GlueProviderA::NAME);
        assert!(table.CreateCustomScanState.is_some());
    }

    #[pg_test]
    fn path_methods_for_is_stable_per_provider_and_distinct_across() {
        let a1 =
            path_methods_for::<GlueProviderA>() as *const pg_sys::CustomPathMethods;
        let a2 =
            path_methods_for::<GlueProviderA>() as *const pg_sys::CustomPathMethods;
        let b1 =
            path_methods_for::<GlueProviderB>() as *const pg_sys::CustomPathMethods;
        assert_eq!(a1, a2);
        assert_ne!(a1, b1);
    }

    #[pg_test]
    fn path_methods_for_carries_name_and_plan_callback() {
        let table = path_methods_for::<GlueProviderA>();
        let name = unsafe { CStr::from_ptr(table.CustomName) };
        assert_eq!(name, GlueProviderA::NAME);
        assert!(table.PlanCustomPath.is_some());
        assert!(table.ReparameterizeCustomPathByChild.is_some());
    }

    #[pg_test]
    fn hook_callback_matches_set_rel_pathlist_hook_type() {
        pg_test_assert_set_rel_pathlist_callback_signature();
    }

    /// `emit_custom_path` emptiness uses `bms_membership == BMS_EMPTY_SET`
    /// (NULL and member-less sets both classify as empty).
    #[pg_test]
    fn bms_empty_classification_matches_null_and_member_less() {
        // SAFETY: `bms_membership` accepts NULL (empty set) and valid `Bitmapset*`.
        let is_empty = |relids: *mut pg_sys::Bitmapset| -> bool {
            unsafe {
                pg_sys::bms_membership(relids)
                    == pg_sys::BMS_Membership::BMS_EMPTY_SET
            }
        };

        let null_relids: *mut pg_sys::Bitmapset = core::ptr::null_mut();
        assert!(
            is_empty(null_relids),
            "a NULL Relids must classify as empty",
        );

        // SAFETY: `bms_make_singleton`/`bms_del_member` in current memory context.
        let emptied: *mut pg_sys::Bitmapset = unsafe {
            let singleton = pg_sys::bms_make_singleton(1);
            pg_sys::bms_del_member(singleton, 1)
        };
        assert!(
            is_empty(emptied),
            "a member-less (emptied) Bitmapset must classify as \
             empty, identically to a NULL Relids",
        );

        // SAFETY: `bms_make_singleton` in current memory context.
        let singleton: *mut pg_sys::Bitmapset =
            unsafe { pg_sys::bms_make_singleton(1) };
        assert!(
            !is_empty(singleton),
            "a non-empty Relids must NOT classify as empty",
        );
    }
}
