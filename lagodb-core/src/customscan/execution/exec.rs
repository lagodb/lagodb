//! Stable CustomScan executor facade.
//!
//! Lifecycle callbacks live in [`lifecycle`] and [`super::start`]; row production
//! and EPQ recheck live in [`scan`]. Re-exports keep method-table and test entry points
//! independent of that internal split.

pub(crate) use super::lifecycle::end_custom_scan_trampoline;
pub(crate) use super::lifecycle::provider_scan_purpose;
pub use super::lifecycle::{
    begin_custom_scan_trampoline, rescan_custom_scan_trampoline,
};
pub(crate) use super::scan::exec_custom_scan_trampoline;
pub(crate) use super::start::first_exec_custom_scan_trampoline;

pub use super::lifecycle::check_scan_relation_oid;
pub use super::scan::next_slot_wrapper;

#[cfg(test)]
mod tests {
    use super::super::lifecycle::check_scan_relation_oid;
    use core::ffi::c_int;
    use std::collections::HashSet;

    use pgrx::pg_sys;
    use pgrx::prelude::PgSqlErrorCode;
    use proptest::prelude::*;

    use crate::customscan::plan_data::custom_exprs::PgExpressionSections;
    use crate::diag::SqlStateError;

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
        let err = PgExpressionSections::validate_counts(None, 1, 0).unwrap_err();
        assert!(
            err.to_string().contains("binding_count=1")
                && err.to_string().contains("pushed_count=0")
        );
    }

    #[test]
    fn custom_expr_section_counts_zero_counts_returns_zero() {
        assert_eq!(
            PgExpressionSections::validate_counts(None, 0, 0).unwrap(),
            0
        );
    }
}
