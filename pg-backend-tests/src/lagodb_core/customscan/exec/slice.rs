//! Backend tests for binding/pushed sections and `check_scan_relation_oid`.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ptr;

    use crate::lagodb_core::customscan::exec::support::ExecExprFixture;
    use lagodb_core::customscan::CustomScanError;
    use lagodb_core::customscan::custom_exprs::CustomExprSections;
    use lagodb_core::customscan::exec::check_scan_relation_oid;
    use lagodb_core::diag::ReportableError;
    use pgrx::pg_sys;
    use pgrx::pg_test;

    /// Split `custom_exprs` in the backend test crate so the production core
    /// only exposes the validated `CustomExprSections` abstraction.
    unsafe fn slice_bindings_pushed(
        list: *mut pg_sys::List,
        binding_count: usize,
        pushed_count: usize,
    ) -> Result<(Vec<*mut pg_sys::Expr>, Vec<*mut pg_sys::Expr>), CustomScanError>
    {
        let sections = unsafe {
            CustomExprSections::from_custom_exprs(list, binding_count, pushed_count)
        }?;
        Ok((sections.bindings().to_vec(), sections.pushed().to_vec()))
    }

    /// Split `custom_exprs` into binding and pushed windows without copying cells.
    #[pg_test]
    fn slice_bindings_pushed_partitions_custom_exprs() {
        unsafe {
            let p0 = ExecExprFixture::int4_const(10);
            let p1 = ExecExprFixture::int4_const(20);
            let p2 = ExecExprFixture::int4_const(30);
            let f0 = ExecExprFixture::int4_const(40);
            let f1 = ExecExprFixture::int4_const(50);
            let list = ExecExprFixture::expr_list(&[p0, p1, p2, f0, f1]);

            let (bindings, pushed) =
                slice_bindings_pushed(list, 3, 2).report_unwrap();

            assert_eq!(
                bindings.len(),
                3,
                "binding window must have binding_count cells"
            );
            assert_eq!(
                pushed.len(),
                2,
                "pushed window must have pushed_count cells"
            );
            assert_eq!(bindings[0], p0, "bindings[0] must alias the first cell");
            assert_eq!(bindings[1], p1);
            assert_eq!(bindings[2], p2);
            assert_eq!(
                pushed[0], f0,
                "pushed[0] must alias the first cell after bindings"
            );
            assert_eq!(pushed[1], f1);
        }
    }

    /// NULL list and zero counts yield two empty vectors.
    #[pg_test]
    fn slice_bindings_pushed_handles_empty_list() {
        unsafe {
            let (bindings, pushed) =
                slice_bindings_pushed(ptr::null_mut(), 0, 0).report_unwrap();
            assert!(bindings.is_empty());
            assert!(pushed.is_empty());
        }
    }

    /// Binding-only split leaves an empty pushed window.
    #[pg_test]
    fn slice_bindings_pushed_bindings_only() {
        unsafe {
            let p0 = ExecExprFixture::int4_const(1);
            let p1 = ExecExprFixture::int4_const(2);
            let list = ExecExprFixture::expr_list(&[p0, p1]);
            let (bindings, pushed) =
                slice_bindings_pushed(list, 2, 0).report_unwrap();
            assert_eq!(bindings.len(), 2);
            assert_eq!(pushed.len(), 0);
            assert_eq!(bindings[0], p0);
            assert_eq!(bindings[1], p1);
        }
    }

    /// Length mismatch raises ERROR (harness checks message).
    #[pg_test(
        error = "customscan BeginCustomScan: custom_exprs length mismatch (got 1, expected binding_count + pushed_count = 3)"
    )]
    fn slice_bindings_pushed_rejects_length_mismatch() {
        unsafe {
            let p0 = ExecExprFixture::int4_const(1);
            let list = ExecExprFixture::expr_list(&[p0]);
            let _ = slice_bindings_pushed(list, 2, 1).report_unwrap();
            panic!(
                "slice_bindings_pushed returned instead of raising \
                 ereport(ERROR) for a length mismatch"
            );
        }
    }

    /// Non-zero counts with a NULL list raise ERROR.
    #[pg_test(
        error = "customscan BeginCustomScan: custom_exprs is NULL but binding_count=1 pushed_count=0"
    )]
    fn slice_bindings_pushed_rejects_null_list_with_nonzero_count() {
        unsafe {
            let _ = slice_bindings_pushed(ptr::null_mut(), 1, 0).report_unwrap();
            panic!(
                "slice_bindings_pushed returned instead of raising \
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
