//! Backend tests for `slice_pushed_recheck` and `check_scan_relation_oid`.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ptr;

    use crate::lakebase_core::customscan::exec::support::ExecExprFixture;
    use pg_lakebase_core::customscan::CustomScanError;
    use pg_lakebase_core::customscan::custom_exprs::CustomExprSections;
    use pg_lakebase_core::customscan::exec::check_scan_relation_oid;
    use pg_lakebase_core::diag::ReportableError;
    use pgrx::pg_sys;
    use pgrx::pg_test;

    /// Split `custom_exprs` in the backend test crate so the production core
    /// only exposes the validated `CustomExprSections` abstraction.
    unsafe fn slice_pushed_recheck(
        list: *mut pg_sys::List,
        pushed_count: usize,
        recheck_count: usize,
    ) -> Result<(Vec<*mut pg_sys::Expr>, Vec<*mut pg_sys::Expr>), CustomScanError>
    {
        let sections = unsafe {
            CustomExprSections::from_custom_exprs(list, pushed_count, recheck_count)
        }?;
        Ok((sections.pushed().to_vec(), sections.recheck().to_vec()))
    }

    /// Split `custom_exprs` into pushed and recheck windows without copying cells.
    #[pg_test]
    fn slice_pushed_recheck_partitions_custom_exprs() {
        unsafe {
            let p0 = ExecExprFixture::int4_const(10);
            let p1 = ExecExprFixture::int4_const(20);
            let p2 = ExecExprFixture::int4_const(30);
            let r0 = ExecExprFixture::int4_const(40);
            let r1 = ExecExprFixture::int4_const(50);
            let list = ExecExprFixture::expr_list(&[p0, p1, p2, r0, r1]);

            let (pushed, recheck) = slice_pushed_recheck(list, 3, 2).report_unwrap();

            assert_eq!(
                pushed.len(),
                3,
                "pushed window must have pushed_count cells"
            );
            assert_eq!(
                recheck.len(),
                2,
                "recheck window must have recheck_count cells"
            );
            assert_eq!(pushed[0], p0, "pushed[0] must alias the first list cell");
            assert_eq!(pushed[1], p1);
            assert_eq!(pushed[2], p2);
            assert_eq!(
                recheck[0], r0,
                "recheck[0] must alias the first cell after pushed"
            );
            assert_eq!(recheck[1], r1);
        }
    }

    /// NULL list and zero counts yield two empty vectors.
    #[pg_test]
    fn slice_pushed_recheck_handles_empty_list() {
        unsafe {
            let (pushed, recheck) =
                slice_pushed_recheck(ptr::null_mut(), 0, 0).report_unwrap();
            assert!(pushed.is_empty());
            assert!(recheck.is_empty());
        }
    }

    /// Pushed-only split leaves an empty recheck window.
    #[pg_test]
    fn slice_pushed_recheck_pushed_only() {
        unsafe {
            let p0 = ExecExprFixture::int4_const(1);
            let p1 = ExecExprFixture::int4_const(2);
            let list = ExecExprFixture::expr_list(&[p0, p1]);
            let (pushed, recheck) = slice_pushed_recheck(list, 2, 0).report_unwrap();
            assert_eq!(pushed.len(), 2);
            assert_eq!(recheck.len(), 0);
            assert_eq!(pushed[0], p0);
            assert_eq!(pushed[1], p1);
        }
    }

    /// Length mismatch raises ERROR (harness checks message).
    #[pg_test(
        error = "customscan BeginCustomScan: custom_exprs length mismatch (got 1, expected pushed_count + recheck_count = 3)"
    )]
    fn slice_pushed_recheck_rejects_length_mismatch() {
        unsafe {
            let p0 = ExecExprFixture::int4_const(1);
            let list = ExecExprFixture::expr_list(&[p0]);
            let _ = slice_pushed_recheck(list, 2, 1).report_unwrap();
            panic!(
                "slice_pushed_recheck returned instead of raising \
                 ereport(ERROR) for a length mismatch"
            );
        }
    }

    /// Non-zero counts with a NULL list raise ERROR.
    #[pg_test(
        error = "customscan BeginCustomScan: custom_exprs is NULL but pushed_count=1 recheck_count=0"
    )]
    fn slice_pushed_recheck_rejects_null_list_with_nonzero_count() {
        unsafe {
            let _ = slice_pushed_recheck(ptr::null_mut(), 1, 0).report_unwrap();
            panic!(
                "slice_pushed_recheck returned instead of raising \
                 ereport(ERROR) for a NULL list with non-zero counts"
            );
        }
    }

    /// OID mismatch raises at the framework boundary (harness checks message).
    #[pg_test(
        error = "customscan BeginCustomScan: scan relation OID mismatch (custom_private.relation_oid=50500, ss_currentRelation->rd_id=50501)"
    )]
    fn check_scan_relation_oid_boundary_raises_on_mismatch() {
        let expected = pg_sys::Oid::from(50_500u32);
        let opened = pg_sys::Oid::from(50_501u32);
        check_scan_relation_oid(expected, opened).report_unwrap();
        panic!(
            "check_scan_relation_oid returned instead of raising ereport(ERROR) \
             for a scan-relation OID mismatch"
        );
    }
}
