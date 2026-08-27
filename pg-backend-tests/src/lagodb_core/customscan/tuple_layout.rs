//! Backend tests for base CustomScan tuple-shape and storage-column planning.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::{c_int, c_void};
    use std::ptr;

    use crate::lagodb_core::support::pg::{INT4_EQ_OPNO, PgNodeBuilder};
    use lagodb_core::customscan::provider::NeededColumns;
    use lagodb_core::customscan::tuple_planner::{
        ScanTuplePlanProbe, ScanTupleShape,
    };
    use pgrx::{Spi, pg_sys, pg_test};

    const SCAN_RELID: pg_sys::Index = 1;
    const OTHER_RELID: c_int = 2;

    struct TupleLayoutFixture;

    impl TupleLayoutFixture {
        fn nodes() -> PgNodeBuilder {
            PgNodeBuilder::new(SCAN_RELID as c_int)
        }

        unsafe fn targetlist(exprs: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
            let mut list: *mut pg_sys::List = ptr::null_mut();
            for (index, &expr) in exprs.iter().enumerate() {
                let resno = pg_sys::AttrNumber::try_from(index + 1)
                    .expect("test targetlist width fits AttrNumber");
                let tle = unsafe {
                    pg_sys::makeTargetEntry(expr, resno, ptr::null_mut(), false)
                };
                list = unsafe { pg_sys::lappend(list, tle.cast::<c_void>()) };
            }
            list
        }

        unsafe fn expr_list(exprs: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
            unsafe { Self::nodes().expr_list(exprs) }
        }

        unsafe fn eq(attno: pg_sys::AttrNumber) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_var_op_const(INT4_EQ_OPNO, attno, 0) }
        }

        unsafe fn tableoid_var() -> *mut pg_sys::Expr {
            unsafe {
                pg_sys::makeVar(
                    SCAN_RELID as c_int,
                    pg_sys::TableOidAttributeNumber as pg_sys::AttrNumber,
                    pg_sys::OIDOID,
                    -1,
                    pg_sys::Oid::INVALID,
                    0,
                )
                .cast()
            }
        }

        unsafe fn ctid_var() -> *mut pg_sys::Expr {
            unsafe {
                pg_sys::makeVar(
                    SCAN_RELID as c_int,
                    pg_sys::SelfItemPointerAttributeNumber as pg_sys::AttrNumber,
                    pg_sys::TIDOID,
                    -1,
                    pg_sys::Oid::INVALID,
                    0,
                )
                .cast()
            }
        }

        unsafe fn subplan(
            testexpr: *mut pg_sys::Expr,
            args: &[*mut pg_sys::Expr],
        ) -> *mut pg_sys::Expr {
            let subplan = unsafe {
                pg_sys::palloc0(core::mem::size_of::<pg_sys::SubPlan>())
                    as *mut pg_sys::SubPlan
            };
            unsafe {
                (*subplan).xpr.type_ = pg_sys::NodeTag::T_SubPlan;
                (*subplan).testexpr = testexpr.cast();
                (*subplan).args = Self::expr_list(args);
            }
            subplan.cast()
        }

        unsafe fn plan(
            targetlist: *mut pg_sys::List,
            path_target_exprs: *mut pg_sys::List,
            qual: *mut pg_sys::List,
            custom_exprs: *mut pg_sys::List,
        ) -> ScanTuplePlanProbe {
            unsafe {
                ScanTuplePlanProbe::plan_base_scan(
                    SCAN_RELID,
                    pg_sys::Oid::INVALID,
                    targetlist,
                    path_target_exprs,
                    qual,
                    custom_exprs,
                )
            }
        }

        unsafe fn count_only_plan(relation_oid: pg_sys::Oid) -> ScanTuplePlanProbe {
            unsafe {
                ScanTuplePlanProbe::plan_base_scan(
                    SCAN_RELID,
                    relation_oid,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            }
        }

        unsafe fn relation_plan(
            targetlist: *mut pg_sys::List,
            qual: *mut pg_sys::List,
        ) -> ScanTuplePlanProbe {
            unsafe {
                ScanTuplePlanProbe::plan_relation_scan(
                    SCAN_RELID,
                    pg_sys::Oid::INVALID,
                    targetlist,
                    ptr::null_mut(),
                    qual,
                    ptr::null_mut(),
                )
            }
        }
    }

    fn assert_subset(columns: NeededColumns<'_>, expected: &[pg_sys::AttrNumber]) {
        match columns {
            NeededColumns::Subset(actual) => assert_eq!(actual, expected),
            NeededColumns::All => panic!("expected Subset({expected:?}), got All"),
        }
    }

    unsafe fn assert_projected(
        plan: &ScanTuplePlanProbe,
        expected: &[pg_sys::AttrNumber],
    ) {
        match plan.shape() {
            ScanTupleShape::ProjectedBase(actual) => assert_eq!(actual, expected),
            ScanTupleShape::Relation => {
                panic!("expected ProjectedBase({expected:?})")
            }
        }
        assert_subset(plan.required_columns(), expected);

        let tlist = plan.custom_scan_tlist();
        assert!(!tlist.is_null(), "projected layout must have a scan tlist");
        assert_eq!(
            unsafe { pg_sys::list_length(tlist) } as usize,
            expected.len()
        );
        for (index, &attno) in expected.iter().enumerate() {
            let tle = unsafe { pg_sys::list_nth(tlist, index as c_int) }
                as *mut pg_sys::TargetEntry;
            assert!(!tle.is_null());
            assert_eq!(unsafe { (*tle).resno } as usize, index + 1);
            let var = unsafe { (*tle).expr }.cast::<pg_sys::Var>();
            assert_eq!(unsafe { (*var).varno }, SCAN_RELID as i32);
            assert_eq!(unsafe { (*var).varattno }, attno);
        }
    }

    #[pg_test]
    fn projected_layout_unions_all_executor_visible_sources() {
        unsafe {
            let nodes = TupleLayoutFixture::nodes();
            let targetlist = TupleLayoutFixture::targetlist(&[nodes.int4_var(3)]);
            let path_target = TupleLayoutFixture::expr_list(&[nodes.int4_var(1)]);
            let qual = TupleLayoutFixture::expr_list(&[TupleLayoutFixture::eq(2)]);
            let custom_exprs =
                TupleLayoutFixture::expr_list(&[TupleLayoutFixture::eq(5)]);

            let plan =
                TupleLayoutFixture::plan(targetlist, path_target, qual, custom_exprs);
            assert_projected(&plan, &[3, 1, 2, 5]);
        }
    }

    #[pg_test]
    fn subplan_local_vars_remain_projectable() {
        unsafe {
            let nodes = TupleLayoutFixture::nodes();
            let targetlist = TupleLayoutFixture::targetlist(&[nodes.int4_var(1)]);
            let subplan = TupleLayoutFixture::subplan(
                nodes.int4_var(4),
                &[nodes.int4_var(6), nodes.int4_var_at(OTHER_RELID, 9)],
            );
            let qual = TupleLayoutFixture::expr_list(&[subplan]);

            let plan = TupleLayoutFixture::plan(
                targetlist,
                ptr::null_mut(),
                qual,
                ptr::null_mut(),
            );
            assert_projected(&plan, &[1, 4, 6]);
        }
    }

    #[pg_test]
    fn tableoid_keeps_relation_shape_with_pruned_storage() {
        unsafe {
            let nodes = TupleLayoutFixture::nodes();
            let targetlist = TupleLayoutFixture::targetlist(&[
                TupleLayoutFixture::tableoid_var(),
                nodes.int4_var(2),
            ]);
            let qual = TupleLayoutFixture::expr_list(&[TupleLayoutFixture::eq(4)]);

            let plan = TupleLayoutFixture::plan(
                targetlist,
                ptr::null_mut(),
                qual,
                ptr::null_mut(),
            );
            assert_eq!(plan.shape(), ScanTupleShape::Relation);
            assert!(plan.custom_scan_tlist().is_null());
            assert_subset(plan.required_columns(), &[2, 4]);
        }
    }

    #[pg_test]
    fn whole_row_requires_relation_shape_and_all_storage_columns() {
        unsafe {
            let nodes = TupleLayoutFixture::nodes();
            let targetlist = TupleLayoutFixture::targetlist(&[
                nodes.int4_var(0),
                nodes.int4_var(2),
            ]);

            let plan = TupleLayoutFixture::plan(
                targetlist,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            );
            assert_eq!(plan.shape(), ScanTupleShape::Relation);
            assert_eq!(plan.required_columns(), NeededColumns::All);
        }
    }

    #[pg_test]
    fn mismatched_varnullingrels_falls_back_without_losing_storage_pruning() {
        unsafe {
            let nodes = TupleLayoutFixture::nodes();
            let first = nodes.int4_var(2);
            let second = nodes.int4_var(2);
            (*second.cast::<pg_sys::Var>()).varnullingrels =
                pg_sys::bms_make_singleton(7);

            let targetlist = TupleLayoutFixture::targetlist(&[first]);
            let qual = TupleLayoutFixture::expr_list(&[second]);
            let plan = TupleLayoutFixture::plan(
                targetlist,
                ptr::null_mut(),
                qual,
                ptr::null_mut(),
            );

            assert_eq!(plan.shape(), ScanTupleShape::Relation);
            assert_subset(plan.required_columns(), &[2]);
        }
    }

    #[pg_test]
    fn count_only_scan_adds_one_live_dummy_dependency() {
        Spi::run("CREATE TEMP TABLE tuple_layout_count_only(a int4, b text)")
            .expect("CREATE TEMP TABLE failed");
        let relation_oid = Spi::get_one::<i64>(
            "SELECT 'pg_temp.tuple_layout_count_only'::regclass::oid::int8",
        )
        .expect("regclass lookup failed")
        .map(|oid| pg_sys::Oid::from(oid as u32))
        .expect("regclass lookup returned NULL");

        unsafe {
            let plan = TupleLayoutFixture::count_only_plan(relation_oid);
            assert_projected(&plan, &[1]);
        }
    }

    #[pg_test]
    fn modify_relation_layout_prunes_to_predicate_columns() {
        unsafe {
            let targetlist =
                TupleLayoutFixture::targetlist(&[TupleLayoutFixture::ctid_var()]);
            let qual = TupleLayoutFixture::expr_list(&[TupleLayoutFixture::eq(3)]);
            let plan = TupleLayoutFixture::relation_plan(targetlist, qual);

            assert_eq!(plan.shape(), ScanTupleShape::Relation);
            assert!(plan.custom_scan_tlist().is_null());
            assert_subset(plan.required_columns(), &[3]);
        }
    }

    #[pg_test]
    fn identity_only_modify_reads_no_business_columns() {
        unsafe {
            let targetlist =
                TupleLayoutFixture::targetlist(&[TupleLayoutFixture::ctid_var()]);
            let plan = TupleLayoutFixture::relation_plan(targetlist, ptr::null_mut());

            assert_eq!(plan.shape(), ScanTupleShape::Relation);
            assert_subset(plan.required_columns(), &[]);
        }
    }
}
