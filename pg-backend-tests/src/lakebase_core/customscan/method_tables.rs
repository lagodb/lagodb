//! Backend tests for CustomScan method tables, hook signatures, and FFI glue.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::CStr;

    use crate::lakebase_core::customscan::support::impl_reject_all_filters;
    use pg_lakebase_core::customscan::hook::assert_set_rel_pathlist_callback_signature;
    use pg_lakebase_core::customscan::provider::methods::method_tables_for;
    use pg_lakebase_core::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
        PathContext, PathVariant, ReScanContext, RelationContext,
    };
    use pg_lakebase_core::customscan::provider::{
        CustomScanPrivate, PrivateDataReader, PrivateDataWriter,
    };
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
            impl_reject_all_filters!($ty);

            impl LakebaseCustomScanProvider for $ty {
                const NAME: &'static CStr = $name;
                type PrivateData = GluePrivate;
                type State = $state;

                fn supports_relation(_ctx: &RelationContext<'_>) -> bool {
                    false
                }

                fn create_path(
                    _ctx: &PathContext<'_>,
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
    fn provider_exec_methods_are_stable_per_provider() {
        let a1 = method_tables_for::<GlueProviderA>().exec()
            as *const pg_sys::CustomExecMethods;
        let a2 = method_tables_for::<GlueProviderA>().exec()
            as *const pg_sys::CustomExecMethods;
        let b1 = method_tables_for::<GlueProviderB>().exec()
            as *const pg_sys::CustomExecMethods;
        assert_eq!(a1, a2);
        assert_ne!(a1, b1);
    }

    #[pg_test]
    fn provider_exec_methods_carry_provider_name() {
        let table = method_tables_for::<GlueProviderA>().exec();
        let name = unsafe { CStr::from_ptr(table.CustomName) };
        assert_eq!(name, GlueProviderA::NAME);
    }

    #[pg_test]
    fn provider_exec_methods_wire_trampolines() {
        let table = method_tables_for::<GlueProviderA>().exec();
        assert!(table.BeginCustomScan.is_some());
        assert!(table.ReScanCustomScan.is_some());
        assert!(table.ExecCustomScan.is_some());
        assert!(table.EndCustomScan.is_some());
        assert!(table.ExplainCustomScan.is_some());
    }

    #[pg_test]
    fn provider_scan_methods_are_stable_and_type_specific() {
        let a1 = method_tables_for::<GlueProviderA>().scan()
            as *const pg_sys::CustomScanMethods;
        let a2 = method_tables_for::<GlueProviderA>().scan()
            as *const pg_sys::CustomScanMethods;
        let b1 = method_tables_for::<GlueProviderB>().scan()
            as *const pg_sys::CustomScanMethods;
        assert_eq!(a1, a2);
        assert_ne!(a1, b1);
    }

    #[pg_test]
    fn provider_scan_methods_carry_name_and_create_state() {
        let table = method_tables_for::<GlueProviderA>().scan();
        let name = unsafe { CStr::from_ptr(table.CustomName) };
        assert_eq!(name, GlueProviderA::NAME);
        assert!(table.CreateCustomScanState.is_some());
    }

    #[pg_test]
    fn provider_path_methods_are_stable_and_type_specific() {
        let a1 = method_tables_for::<GlueProviderA>().path()
            as *const pg_sys::CustomPathMethods;
        let a2 = method_tables_for::<GlueProviderA>().path()
            as *const pg_sys::CustomPathMethods;
        let b1 = method_tables_for::<GlueProviderB>().path()
            as *const pg_sys::CustomPathMethods;
        assert_eq!(a1, a2);
        assert_ne!(a1, b1);
    }

    #[pg_test]
    fn provider_path_methods_carry_name_and_plan_callbacks() {
        let table = method_tables_for::<GlueProviderA>().path();
        let name = unsafe { CStr::from_ptr(table.CustomName) };
        assert_eq!(name, GlueProviderA::NAME);
        assert!(table.PlanCustomPath.is_some());
        assert!(table.ReparameterizeCustomPathByChild.is_some());
    }

    #[pg_test]
    fn hook_callback_matches_set_rel_pathlist_hook_type() {
        assert_set_rel_pathlist_callback_signature();
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
