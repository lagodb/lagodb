//! Stable CustomScan executor facade.
//!
//! Lifecycle callbacks live in [`lifecycle`]; row production and EPQ recheck
//! live in [`scan`]. Re-exports keep the method-table and test entry points
//! independent of that internal split.

pub(crate) use super::lifecycle::end_custom_scan_trampoline;
pub(crate) use super::lifecycle::provider_scan_purpose;
pub use super::lifecycle::{
    begin_custom_scan_trampoline, rescan_custom_scan_trampoline,
};
pub(crate) use super::scan::exec_custom_scan_trampoline;

pub use super::lifecycle::check_scan_relation_oid;
pub use super::scan::next_slot_wrapper;

#[cfg(test)]
mod tests {
    use super::super::lifecycle::check_scan_relation_oid;
    use super::super::scan;
    use core::ffi::CStr;
    use core::ffi::c_int;
    use core::marker::PhantomData;
    use std::collections::HashSet;

    use pgrx::pg_sys;
    use pgrx::prelude::PgSqlErrorCode;
    use proptest::prelude::*;

    use crate::customscan::plan_data::custom_exprs::validate_custom_expr_section_counts;
    use crate::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, CustomScanPrivate, EndContext, LakebaseCustomScanProvider,
        NextSlotContext, PathContext, PathVariant, PrivateDataReader,
        PrivateDataWriter, ReScanContext, RelationContext,
    };
    use crate::diag::SqlStateError;
    use crate::expr::pushdown::{
        FilterBindResult, FilterFragment, FilterPlan, FilterPlanningContext,
        FilterPushdown, FilterPushdownPlanner, FilterValueBindings,
    };
    use crate::plan_data::{PlanDataReader, PlanDataWriter};

    struct NoopPrivate;

    impl CustomScanPrivate for NoopPrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(Self)
        }
    }

    trait NoopProviderSpec: 'static {
        const NAME: &'static CStr;
        type State: 'static;

        fn state() -> Self::State;
    }

    struct NoopProvider<S>(PhantomData<S>);

    struct RejectAllPlanner;

    impl FilterPushdownPlanner for RejectAllPlanner {
        type PlannedPredicate = ();
        type Error = CustomScanError;

        fn try_plan_filter(
            &mut self,
            _fragment: &FilterFragment,
        ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
            Ok(FilterPlan::Unsupported)
        }
    }

    impl<S: NoopProviderSpec> FilterPushdown for NoopProvider<S> {
        type Planner = RejectAllPlanner;
        type PlannedPredicate = ();
        type BoundPredicate = ();
        type Error = CustomScanError;

        fn begin_filter_planning(
            _context: &FilterPlanningContext,
        ) -> Result<Self::Planner, Self::Error> {
            Ok(RejectAllPlanner)
        }

        fn encode_planned(
            _predicate: &Self::PlannedPredicate,
            _writer: &mut PlanDataWriter,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn decode_planned(
            _reader: &mut PlanDataReader<'_>,
            _binding_count: usize,
        ) -> Result<Self::PlannedPredicate, Self::Error> {
            Ok(())
        }

        fn bind_filter(
            _predicate: &Self::PlannedPredicate,
            _values: FilterValueBindings<'_>,
        ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
            Ok(FilterBindResult::ValueNotRepresentable)
        }
    }

    impl<S: NoopProviderSpec> LakebaseCustomScanProvider for NoopProvider<S> {
        const NAME: &'static CStr = S::NAME;
        type PrivateData = NoopPrivate;
        type State = S::State;

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
            S::state()
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("NoopProvider::begin is not exercised by host-only tests")
        }

        fn next_slot(
            _ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            unreachable!(
                "NoopProvider::next_slot is not exercised by host-only tests"
            )
        }

        fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("NoopProvider::rescan is not exercised by host-only tests")
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("NoopProvider::end is not exercised by host-only tests")
        }
    }

    struct DummyState;

    struct ExecProviderSpec;

    impl NoopProviderSpec for ExecProviderSpec {
        const NAME: &'static core::ffi::CStr = c"exec-test-dummy";
        type State = DummyState;

        fn state() -> Self::State {
            DummyState
        }
    }

    type DummyProvider = NoopProvider<ExecProviderSpec>;

    #[test]
    fn create_state_context_new_compiles() {
        let _ctx: CreateStateContext<DummyProvider> =
            CreateStateContext::<DummyProvider>::new();
    }

    fn exec_ids_for_bitmap(refs: &[(pg_sys::ParamKind::Type, c_int)]) -> Vec<c_int> {
        refs.iter()
            .filter(|(kind, _)| *kind == pg_sys::ParamKind::PARAM_EXEC)
            .map(|(_, id)| *id)
            .collect()
    }

    fn exec_id_set(refs: &[(pg_sys::ParamKind::Type, c_int)]) -> HashSet<c_int> {
        exec_ids_for_bitmap(refs).into_iter().collect()
    }

    fn params_changed(chgparam: &HashSet<c_int>, exec_ids: &HashSet<c_int>) -> bool {
        !chgparam.is_disjoint(exec_ids)
    }

    #[test]
    fn exec_ids_for_bitmap_excludes_extern() {
        let refs = vec![
            (pg_sys::ParamKind::PARAM_EXTERN, 1),
            (pg_sys::ParamKind::PARAM_EXEC, 1),
            (pg_sys::ParamKind::PARAM_EXTERN, 2),
            (pg_sys::ParamKind::PARAM_EXEC, 5),
        ];
        assert_eq!(exec_ids_for_bitmap(&refs), vec![1, 5]);
        assert_eq!(exec_id_set(&refs), HashSet::from([1, 5]));
    }

    #[test]
    fn params_changed_is_intersection_nonempty() {
        assert!(params_changed(
            &HashSet::from([1, 3]),
            &HashSet::from([3, 7]),
        ));
        assert!(!params_changed(
            &HashSet::from([2, 4]),
            &HashSet::from([1, 3]),
        ));
        assert!(!params_changed(&HashSet::from([1, 2, 3]), &HashSet::new()));
    }

    #[test]
    fn extern_id_alone_never_flips_verdict() {
        let refs = vec![
            (pg_sys::ParamKind::PARAM_EXTERN, 10),
            (pg_sys::ParamKind::PARAM_EXEC, 4),
        ];
        let exec_ids = exec_id_set(&refs);
        assert!(!params_changed(&HashSet::from([10]), &exec_ids));
        assert!(params_changed(&HashSet::from([4]), &exec_ids));
    }

    fn kind_strategy() -> impl Strategy<Value = pg_sys::ParamKind::Type> {
        prop_oneof![
            Just(pg_sys::ParamKind::PARAM_EXTERN),
            Just(pg_sys::ParamKind::PARAM_EXEC),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn chgparam_gating_soundness(
            refs in prop::collection::vec((kind_strategy(), 0i32..8), 0..12),
            chgparam in prop::collection::hash_set(0i32..8, 0..8),
        ) {
            let expected_exec: HashSet<c_int> = refs
                .iter()
                .filter(|(kind, _)| *kind == pg_sys::ParamKind::PARAM_EXEC)
                .map(|(_, id)| *id)
                .collect();
            let exec_ids = exec_id_set(&refs);
            prop_assert_eq!(&exec_ids, &expected_exec);
            prop_assert_eq!(
                params_changed(&chgparam, &exec_ids),
                !chgparam.is_disjoint(&exec_ids)
            );
        }
    }

    #[test]
    fn decide_covers_all_four_combinations() {
        assert_eq!(scan::decide(true, false), scan::SlotOutcome::Return);
        assert_eq!(
            scan::decide(true, true),
            scan::SlotOutcome::RaiseEmptyProduced
        );
        assert_eq!(
            scan::decide(false, false),
            scan::SlotOutcome::RaiseFilledEof
        );
        assert_eq!(scan::decide(false, true), scan::SlotOutcome::Eof);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn decide_never_truncates(
            produced in any::<bool>(),
            slot_empty in any::<bool>(),
        ) {
            let outcome = scan::decide(produced, slot_empty);
            match (produced, slot_empty) {
                (true, false) => prop_assert_eq!(outcome, scan::SlotOutcome::Return),
                (true, true) => prop_assert_eq!(
                    outcome,
                    scan::SlotOutcome::RaiseEmptyProduced
                ),
                (false, false) => prop_assert_eq!(
                    outcome,
                    scan::SlotOutcome::RaiseFilledEof
                ),
                (false, true) => prop_assert_eq!(outcome, scan::SlotOutcome::Eof),
            }
            if produced {
                prop_assert_ne!(outcome, scan::SlotOutcome::Eof);
            }
        }
    }

    #[test]
    fn check_scan_relation_oid_ok_on_equal() {
        let oid = pg_sys::Oid::from(50500u32);
        assert!(check_scan_relation_oid(oid, oid).is_ok());
    }

    #[test]
    fn check_scan_relation_oid_err_and_display_on_mismatch() {
        let expected = pg_sys::Oid::from(50500u32);
        let opened = pg_sys::Oid::from(50501u32);
        let err = check_scan_relation_oid(expected, opened).unwrap_err();
        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
        assert!(
            err.to_string().contains("relation_oid=50500")
                && err.to_string().contains("rd_id=50501")
        );
    }

    #[test]
    fn custom_expr_section_counts_null_branch_returns_err() {
        let err = validate_custom_expr_section_counts(None, 1, 0).unwrap_err();
        assert!(
            err.to_string().contains("binding_count=1")
                && err.to_string().contains("pushed_count=0")
        );
    }

    #[test]
    fn custom_expr_section_counts_zero_counts_returns_zero() {
        assert_eq!(validate_custom_expr_section_counts(None, 0, 0).unwrap(), 0);
    }
}
