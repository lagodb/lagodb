//! Backend tests for `referenced_attnos` / needed-column analysis on plan nodes.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ptr;

    use crate::support::pg::{INT4_EQ_OPNO, PgNodeBuilder};
    use pg_lakebase_core::customscan::provider::{
        NeededColumns, pg_test_referenced_attnos,
    };
    use pgrx::pg_sys;
    use pgrx::pg_test;

    const SCAN_RELID: core::ffi::c_int = 1;
    const OTHER_RELID: core::ffi::c_int = 2;

    struct ReferencedAttnosFixture;

    impl ReferencedAttnosFixture {
        fn nodes() -> PgNodeBuilder {
            PgNodeBuilder::new(SCAN_RELID)
        }

        unsafe fn var(
            varno: core::ffi::c_int,
            varattno: pg_sys::AttrNumber,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_var_at(varno, varattno) }
        }

        unsafe fn func_expr(arg: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_func_expr(arg) }
        }

        /// `Var(varno, attno) = const` — exercises `OpExpr.args` recursion.
        unsafe fn eq_op(
            varno: core::ffi::c_int,
            attno: pg_sys::AttrNumber,
        ) -> *mut pg_sys::Expr {
            let left = unsafe { Self::var(varno, attno) };
            let right = unsafe { Self::nodes().int4_const(0) };
            unsafe { Self::nodes().int4_op_clause(INT4_EQ_OPNO, left, right) }
        }
    }

    /// Manual `TargetEntry` — `makeTargetEntry` is not exposed through `pg_sys`.
    unsafe fn push_target_entry(
        list: *mut pg_sys::List,
        expr: *mut pg_sys::Expr,
        resno: pg_sys::AttrNumber,
    ) -> *mut pg_sys::List {
        unsafe {
            let te = pg_sys::palloc0(core::mem::size_of::<pg_sys::TargetEntry>())
                as *mut pg_sys::TargetEntry;
            (*te).xpr.type_ = pg_sys::NodeTag::T_TargetEntry;
            (*te).expr = expr;
            (*te).resno = resno;
            (*te).resjunk = false;
            pg_sys::lappend(list, te.cast())
        }
    }

    unsafe fn push_expr(
        list: *mut pg_sys::List,
        expr: *mut pg_sys::Expr,
    ) -> *mut pg_sys::List {
        unsafe { pg_sys::lappend(list, expr.cast()) }
    }

    unsafe fn make_cscan(
        targetlist: *mut pg_sys::List,
        qual: *mut pg_sys::List,
    ) -> *mut pg_sys::CustomScan {
        unsafe {
            let cscan = pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomScan>())
                as *mut pg_sys::CustomScan;
            let scan = &mut (*cscan).scan;
            scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
            scan.plan.targetlist = targetlist;
            scan.plan.qual = qual;
            scan.scanrelid = SCAN_RELID as pg_sys::Index;
            cscan
        }
    }

    fn assert_subset(result: NeededColumns, expected: &[pg_sys::AttrNumber]) {
        match result {
            NeededColumns::Subset(got) => assert_eq!(
                got, expected,
                "expected Subset({expected:?}), got Subset({got:?})"
            ),
            NeededColumns::All => {
                panic!("expected Subset({expected:?}), got All")
            }
        }
    }

    #[pg_test]
    fn targetlist_only_collects_user_attnos() {
        unsafe {
            let mut tlist: *mut pg_sys::List = ptr::null_mut();
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 3),
                1,
            );
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 1),
                2,
            );
            let cscan = make_cscan(tlist, ptr::null_mut());

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &[]);
            assert_subset(result, &[1, 3]);
        }
    }

    #[pg_test]
    fn targetlist_collects_var_nested_under_func_expr() {
        unsafe {
            let mut tlist: *mut pg_sys::List = ptr::null_mut();
            let nested = ReferencedAttnosFixture::func_expr(
                ReferencedAttnosFixture::var(SCAN_RELID, 6),
            );
            tlist = push_target_entry(tlist, nested, 1);
            let cscan = make_cscan(tlist, ptr::null_mut());

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &[]);
            assert_subset(result, &[6]);
        }
    }

    #[pg_test]
    fn qual_only_collects_user_attnos() {
        unsafe {
            let mut qual: *mut pg_sys::List = ptr::null_mut();
            qual = push_expr(qual, ReferencedAttnosFixture::eq_op(SCAN_RELID, 2));
            let cscan = make_cscan(ptr::null_mut(), qual);

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &[]);
            assert_subset(result, &[2]);
        }
    }

    #[pg_test]
    fn recheck_only_collects_user_attnos() {
        unsafe {
            let cscan = make_cscan(ptr::null_mut(), ptr::null_mut());
            let recheck = [ReferencedAttnosFixture::eq_op(SCAN_RELID, 4)];

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &recheck);
            assert_subset(result, &[4]);
        }
    }

    #[pg_test]
    fn union_dedup_and_sort_across_sources() {
        unsafe {
            let mut tlist: *mut pg_sys::List = ptr::null_mut();
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 5),
                1,
            );
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 2),
                2,
            );

            let mut qual: *mut pg_sys::List = ptr::null_mut();
            qual = push_expr(qual, ReferencedAttnosFixture::eq_op(SCAN_RELID, 2));
            qual = push_expr(qual, ReferencedAttnosFixture::eq_op(SCAN_RELID, 1));

            let cscan = make_cscan(tlist, qual);
            let recheck = [ReferencedAttnosFixture::eq_op(SCAN_RELID, 5)];

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &recheck);
            assert_subset(result, &[1, 2, 5]);
        }
    }

    #[pg_test]
    fn whole_row_var_forces_all() {
        unsafe {
            let mut tlist: *mut pg_sys::List = ptr::null_mut();
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 1),
                1,
            );
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 0),
                2,
            );
            let cscan = make_cscan(tlist, ptr::null_mut());

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &[]);
            assert_eq!(result, NeededColumns::All);
        }
    }

    #[pg_test]
    fn system_column_forces_all() {
        unsafe {
            let mut qual: *mut pg_sys::List = ptr::null_mut();
            qual = push_expr(qual, ReferencedAttnosFixture::eq_op(SCAN_RELID, -1));
            let cscan = make_cscan(ptr::null_mut(), qual);

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &[]);
            assert_eq!(result, NeededColumns::All);
        }
    }

    #[pg_test]
    fn mixed_user_and_system_forces_all() {
        unsafe {
            let mut tlist: *mut pg_sys::List = ptr::null_mut();
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 2),
                1,
            );
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 7),
                2,
            );

            let mut qual: *mut pg_sys::List = ptr::null_mut();
            qual = push_expr(qual, ReferencedAttnosFixture::eq_op(SCAN_RELID, -3));

            let cscan = make_cscan(tlist, qual);

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &[]);
            assert_eq!(result, NeededColumns::All);
        }
    }

    #[pg_test]
    fn outer_relation_vars_excluded() {
        unsafe {
            let mut tlist: *mut pg_sys::List = ptr::null_mut();
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(SCAN_RELID, 1),
                1,
            );
            tlist = push_target_entry(
                tlist,
                ReferencedAttnosFixture::var(OTHER_RELID, 0),
                2,
            );

            let mut qual: *mut pg_sys::List = ptr::null_mut();
            qual = push_expr(qual, ReferencedAttnosFixture::eq_op(OTHER_RELID, 9));

            let cscan = make_cscan(tlist, qual);

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &[]);
            assert_subset(result, &[1]);
        }
    }

    #[pg_test]
    fn count_star_shape_returns_empty_subset() {
        unsafe {
            let cscan = make_cscan(ptr::null_mut(), ptr::null_mut());

            let result = pg_test_referenced_attnos(cscan, SCAN_RELID, &[]);
            assert_eq!(result, NeededColumns::Subset(Vec::new()));
        }
    }
}
