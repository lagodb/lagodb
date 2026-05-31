//! Backend tests for customscan exec helpers and rescan trampolines.
//! Full `ExecInitCustomScan` coverage lives in pg-iceberg-am regressions.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_int;
    use std::ptr;

    use crate::support::pg::{INT4_EQ_OPNO, OpExprSpec, PgNodeBuilder};
    use pg_lakebase_core::customscan::codec::{PrivateDataReader, PrivateDataWriter};
    use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;
    use pg_lakebase_core::customscan::exec::{
        check_scan_relation_oid, collect_param_refs, slice_pushed_recheck,
    };
    use pg_lakebase_core::diag::ReportableError;
    use pg_lakebase_core::expr::runtime_params::{
        ExecParamRef, ExternParamRef, RuntimeParamResolver,
    };
    use pgrx::pg_sys;
    use pgrx::pg_test;

    struct ExecExprFixture;

    impl ExecExprFixture {
        fn nodes() -> PgNodeBuilder {
            PgNodeBuilder::new(1)
        }

        unsafe fn param(
            kind: pg_sys::ParamKind::Type,
            param_id: c_int,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_param(kind, param_id) }
        }

        unsafe fn var(attno: pg_sys::AttrNumber) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_var(attno) }
        }

        unsafe fn int4_const(value: i32) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_const(value) }
        }

        unsafe fn op_expr(
            left: *mut pg_sys::Expr,
            right: *mut pg_sys::Expr,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_op_clause(INT4_EQ_OPNO, left, right) }
        }

        unsafe fn bool_expr(
            boolop: pg_sys::BoolExprType::Type,
            children: &[*mut pg_sys::Expr],
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().bool_expr(boolop, children) }
        }

        unsafe fn null_test(
            arg: *mut pg_sys::Expr,
            nulltesttype: pg_sys::NullTestType::Type,
        ) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().null_test(arg, nulltesttype) }
        }

        unsafe fn relabel(arg: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().relabel_int4(arg) }
        }

        unsafe fn func_expr(arg: *mut pg_sys::Expr) -> *mut pg_sys::Expr {
            unsafe { Self::nodes().int4_func_expr(arg) }
        }

        unsafe fn expr_list(cells: &[*mut pg_sys::Expr]) -> *mut pg_sys::List {
            unsafe { Self::nodes().expr_list(cells) }
        }
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

    /// EXTERN params go to `extern_params` but not the chgParam bitmap.
    #[pg_test]
    fn collect_param_refs_extern_singleton() {
        unsafe {
            let p = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 7);
            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(p, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 1);
            assert_eq!(exec_params.len(), 0);
            assert_eq!(extern_params[0].param_id, 7);
            assert!(
                !pg_sys::bms_is_member(7, bm),
                "a PARAM_EXTERN id must not enter the chgParam bitmap",
            );
            assert!(
                bm.is_null(),
                "an EXTERN-only walk leaves the EXEC-only bitmap empty (NULL)",
            );
        }
    }

    /// EXEC params go to `exec_params` and enter the chgParam bitmap.
    #[pg_test]
    fn collect_param_refs_exec_singleton() {
        unsafe {
            let p = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXEC, 3);
            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(p, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 0);
            assert_eq!(exec_params.len(), 1);
            assert_eq!(exec_params[0].param_id, 3);
            assert!(pg_sys::bms_is_member(3, bm));
            assert_eq!(pg_sys::bms_num_members(bm), 1);
        }
    }

    /// Walker recurses OpExpr/BoolExpr; EXTERN params skip the chgParam bitmap.
    #[pg_test]
    fn collect_param_refs_recurses_into_opexpr_and_boolexpr() {
        unsafe {
            // (var(1) = $1) AND (var(2) = $2)
            let var1 = ExecExprFixture::var(1);
            let p1 = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 1);
            let op1 = ExecExprFixture::op_expr(var1, p1);

            let var2 = ExecExprFixture::var(2);
            let p2 = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 2);
            let op2 = ExecExprFixture::op_expr(var2, p2);

            let and = ExecExprFixture::bool_expr(
                pg_sys::BoolExprType::AND_EXPR,
                &[op1, op2],
            );

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(and, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 2);
            assert_eq!(extern_params[0].param_id, 1);
            assert_eq!(extern_params[1].param_id, 2);
            assert!(
                !pg_sys::bms_is_member(1, bm),
                "PARAM_EXTERN id 1 must not enter the chgParam bitmap",
            );
            assert!(
                !pg_sys::bms_is_member(2, bm),
                "PARAM_EXTERN id 2 must not enter the chgParam bitmap",
            );
            assert!(
                bm.is_null(),
                "an EXTERN-only walk leaves the EXEC-only bitmap empty (NULL)",
            );
        }
    }

    /// Walker recurses through `NullTest` and `RelabelType` wrappers.
    #[pg_test]
    fn collect_param_refs_recurses_into_nulltest_and_relabel() {
        unsafe {
            // RelabelType( $1::int4 ) IS NULL
            let p1 = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXEC, 5);
            let labelled = ExecExprFixture::relabel(p1);
            let nt =
                ExecExprFixture::null_test(labelled, pg_sys::NullTestType::IS_NULL);

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(nt, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 0);
            assert_eq!(exec_params.len(), 1);
            assert_eq!(exec_params[0].param_id, 5);
            assert!(pg_sys::bms_is_member(5, bm));
            assert_eq!(pg_sys::bms_num_members(bm), 1);
        }
    }

    /// Walker recurses through expression nodes covered by PG core walker.
    #[pg_test]
    fn collect_param_refs_recurses_into_func_expr() {
        unsafe {
            let p1 = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXEC, 8);
            let func = ExecExprFixture::func_expr(p1);

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(func, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 0);
            assert_eq!(exec_params.len(), 1);
            assert_eq!(exec_params[0].param_id, 8);
            assert!(pg_sys::bms_is_member(8, bm));
            assert_eq!(pg_sys::bms_num_members(bm), 1);
        }
    }

    /// Duplicate EXTERN ids stay in `extern_params` but not the chgParam bitmap.
    #[pg_test]
    fn collect_param_refs_duplicate_param_ids_dedupe_only_in_bitmap() {
        unsafe {
            // ($1 = $1)
            let p_left = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 4);
            let p_right = ExecExprFixture::param(pg_sys::ParamKind::PARAM_EXTERN, 4);
            let op = ExecExprFixture::op_expr(p_left, p_right);

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(op, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(
                extern_params.len(),
                2,
                "duplicate param ids surface verbatim in extern_params (Vec carries duplicates)"
            );
            assert_eq!(extern_params[0].param_id, 4);
            assert_eq!(extern_params[1].param_id, 4);
            assert!(
                !pg_sys::bms_is_member(4, bm),
                "a PARAM_EXTERN id must not enter the chgParam bitmap, \
                 even when referenced more than once",
            );
            assert!(
                bm.is_null(),
                "an EXTERN-only walk leaves the EXEC-only bitmap empty (NULL)",
            );
        }
    }

    /// `Var` and `Const` are leaves; empty bitmap stays NULL (PG convention).
    #[pg_test]
    fn collect_param_refs_var_and_const_are_leaves() {
        unsafe {
            let var = ExecExprFixture::var(1);
            let lit = ExecExprFixture::int4_const(42);
            let op = ExecExprFixture::op_expr(var, lit);

            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();

            collect_param_refs(op, &mut extern_params, &mut exec_params, &mut bm);

            assert_eq!(extern_params.len(), 0);
            assert_eq!(exec_params.len(), 0);
            assert!(
                bm.is_null(),
                "an empty bitmap must remain NULL (PG's empty-set convention)"
            );
        }
    }

    /// NULL expression is a no-op (defense in depth for corrupted plan trees).
    #[pg_test]
    fn collect_param_refs_null_is_noop() {
        unsafe {
            let mut extern_params: Vec<ExternParamRef> = Vec::new();
            let mut exec_params: Vec<ExecParamRef> = Vec::new();
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();
            collect_param_refs(
                ptr::null_mut(),
                &mut extern_params,
                &mut exec_params,
                &mut bm,
            );
            assert!(extern_params.is_empty());
            assert!(exec_params.is_empty());
            assert!(bm.is_null());
        }
    }

    /// Build a `Bitmapset`; NULL is PG's empty-set representation.
    unsafe fn make_bms(members: &[c_int]) -> *mut pg_sys::Bitmapset {
        unsafe {
            let mut bm: *mut pg_sys::Bitmapset = ptr::null_mut();
            for &m in members {
                bm = pg_sys::bms_add_member(bm, m);
            }
            bm
        }
    }

    /// NULL `chgParam` never overlaps a non-empty cached bitmap.
    #[pg_test]
    fn rescan_bms_overlap_null_chgparam_is_disjoint() {
        unsafe {
            let cached = make_bms(&[1, 2, 3]);

            assert!(
                !pg_sys::bms_overlap(ptr::null(), cached),
                "bms_overlap(NULL, non-empty) must be false ",
            );
            assert!(
                !pg_sys::bms_overlap(ptr::null(), ptr::null()),
                "bms_overlap(NULL, NULL) must be false",
            );
        }
    }

    /// NULL cached ids stay disjoint even when chgParam is non-empty.
    #[pg_test]
    fn rescan_bms_overlap_null_cached_ids_is_disjoint() {
        unsafe {
            let chg = make_bms(&[7]);
            assert!(
                !pg_sys::bms_overlap(chg, ptr::null()),
                "bms_overlap(non-empty, NULL) must be false ",
            );
        }
    }

    /// Disjoint non-empty bitmaps do not overlap.
    #[pg_test]
    fn rescan_bms_overlap_disjoint_nonempty_is_false() {
        unsafe {
            let chg = make_bms(&[1, 2]);
            let cached = make_bms(&[3, 4]);
            assert!(
                !pg_sys::bms_overlap(chg, cached),
                "disjoint non-empty bitmaps must not overlap \
                 ",
            );
        }
    }

    /// Intersecting bitmaps overlap.
    #[pg_test]
    fn rescan_bms_overlap_intersecting_is_true() {
        unsafe {
            let chg = make_bms(&[1, 2, 3]);
            let cached = make_bms(&[3, 4, 5]);
            assert!(
                pg_sys::bms_overlap(chg, cached),
                "bitmaps sharing member 3 must overlap \
                 ",
            );

            let single_chg = make_bms(&[42]);
            let single_cached = make_bms(&[42]);
            assert!(
                pg_sys::bms_overlap(single_chg, single_cached),
                "identical singletons must overlap",
            );
        }
    }

    use pg_lakebase_core::customscan::custom_private::encode_split;
    use pg_lakebase_core::customscan::exec::rescan_custom_scan_trampoline;
    use pg_lakebase_core::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
        PathVariant, PlanTranslateContext, ReScanContext, RelPathContext,
    };
    use pg_lakebase_core::customscan::state::CachedEnvelope;
    use pg_lakebase_core::customscan::state::CustomScanStateWrapper;
    use pg_lakebase_core::expr::nodes::{ParamKey, PgParamValue};
    use pg_lakebase_core::expr::split::{
        ColumnRef, PushdownContract, QualPushdownDecision,
    };

    /// Stub provider counters updated from `rescan`.
    #[derive(Default)]
    struct CountingState {
        rescan_call_count: usize,
        predicate_rebuilt: bool,
        resolved_param_count: usize,
        last_pushed_count: usize,
        last_scan_relid: core::ffi::c_int,
    }

    /// Empty private data; ReScan re-decodes the envelope but not provider-private cells.
    struct CountingPrivate;

    impl CustomScanPrivate for CountingPrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(CountingPrivate)
        }
    }

    struct CountingProvider;

    impl LakebaseCustomScanProvider for CountingProvider {
        const NAME: &'static core::ffi::CStr = c"chgparam-rescan-counting";
        type PrivateData = CountingPrivate;
        type State = CountingState;

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
            CountingState::default()
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("CP-8 tests drive rescan directly; begin is not invoked");
        }

        fn next_slot(
            _ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            unreachable!(
                "CP-8 tests drive rescan directly; next_slot is not invoked"
            );
        }

        /// Record rescan branch observations for post-trampoline assertions.
        fn rescan(ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            ctx.state.rescan_call_count += 1;
            if ctx.params_changed {
                ctx.state.predicate_rebuilt = true;
            }
            ctx.state.resolved_param_count = ctx.resolved_param_count();
            ctx.state.last_pushed_count = ctx.pushed_predicate_count();
            ctx.state.last_scan_relid = ctx.scan_relid();
            Ok(())
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("CP-8 tests drive rescan directly; end is not invoked");
        }
    }

    /// Synthetic rescan fixture from `synth_setup_chgparam_rescan`.
    struct ChgParamFixture {
        wrapper_ptr: *mut CustomScanStateWrapper<CountingProvider>,
        /// Same allocation as `wrapper_ptr` (`base` is the first field).
        node: *mut pg_sys::CustomScanState,
        /// Retained to assert the trampoline did not stomp cached ids.
        cached_ids: *mut pg_sys::Bitmapset,
    }

    /// `RelationData` stub with only `rd_id` populated.
    unsafe fn make_relation_stub(oid: pg_sys::Oid) -> pg_sys::Relation {
        unsafe {
            let rel = pg_sys::palloc0(core::mem::size_of::<pg_sys::RelationData>())
                as pg_sys::Relation;
            (*rel).rd_id = oid;
            rel
        }
    }

    /// `EState` stub with `es_param_list_info` and `es_snapshot` set.
    unsafe fn make_estate_stub(
        param_list_info: pg_sys::ParamListInfo,
    ) -> *mut pg_sys::EState {
        unsafe {
            let estate = pg_sys::palloc0(core::mem::size_of::<pg_sys::EState>())
                as *mut pg_sys::EState;
            (*estate).type_ = pg_sys::NodeTag::T_EState;
            (*estate).es_param_list_info = param_list_info;
            // Zeroed SnapshotData shell is sufficient; the stub provider never dereferences it.
            let snapshot =
                pg_sys::palloc0(core::mem::size_of::<pg_sys::SnapshotData>())
                    as pg_sys::Snapshot;
            (*estate).es_snapshot = snapshot;
            estate
        }
    }

    /// `ExprContext` stub; per-tuple memory is the test's current context.
    unsafe fn make_econtext_stub() -> *mut pg_sys::ExprContext {
        unsafe {
            let econtext = pg_sys::palloc0(core::mem::size_of::<pg_sys::ExprContext>())
                as *mut pg_sys::ExprContext;
            (*econtext).type_ = pg_sys::NodeTag::T_ExprContext;
            (*econtext).ecxt_per_tuple_memory = pg_sys::CurrentMemoryContext;
            (*econtext).ecxt_per_query_memory = pg_sys::CurrentMemoryContext;
            econtext
        }
    }

    /// `ParamListInfo` with one INT4 slot (paramid 1) holding value 42.
    unsafe fn make_param_list_int4_one() -> pg_sys::ParamListInfo {
        unsafe {
            let pli = pg_sys::makeParamList(1);
            let slot: *mut pg_sys::ParamExternData = (*pli).params.as_mut_ptr();
            (*slot).ptype = pg_sys::INT4OID;
            (*slot).value = pg_sys::Datum::from(42i32);
            (*slot).isnull = false;
            (*slot).pflags = 0;
            pli
        }
    }

    /// Synthetic `CustomScan` with one pushed EXTERN param and matching envelope.
    unsafe fn make_custom_scan_plan(
        relation_oid: pg_sys::Oid,
    ) -> *mut pg_sys::CustomScan {
        unsafe {
            let nodes = PgNodeBuilder::new(1);
            let v = nodes.int4_var(1);
            let p = nodes.int4_param(pg_sys::ParamKind::PARAM_EXTERN, 1);

            let op = nodes.op_expr(OpExprSpec::int4_eq_deparse(), &[v, p]);

            let mut custom_exprs: *mut pg_sys::List = ptr::null_mut();
            custom_exprs = pg_sys::lappend(custom_exprs, op.cast());

            // `name: None` exercises the runtime `get_attname` fallback path.
            let column_refs = vec![ColumnRef {
                expr_index: 0,
                rel_oid: relation_oid,
                attno: 1,
                atttypid: pg_sys::INT4OID,
                attcollation: pg_sys::Oid::INVALID,
                name: None,
            }];
            let pushed_contracts = vec![PushdownContract::ExactRowFilter];
            let custom_private = encode_split(
                CountingProvider::NAME,
                relation_oid,
                1,
                0,
                &pushed_contracts,
                &column_refs,
                ptr::null_mut(),
                1, // pre_setrefs_scan_rti — debug-only
            )
            .expect("encode_split must succeed for the CP-8 fixture's tiny counts");

            let cscan = pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomScan>())
                as *mut pg_sys::CustomScan;
            (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
            (*cscan).scan.scanrelid = 1;
            (*cscan).custom_exprs = custom_exprs;
            (*cscan).custom_private = custom_private;

            cscan
        }
    }

    /// Build wrapper/node/cached_ids triple for chgParam rescan tests.
    unsafe fn synth_setup_chgparam_rescan(
        cached_ids_members: &[c_int],
        chgparam_members: &[c_int],
    ) -> ChgParamFixture {
        unsafe {
            let relation_oid = pg_sys::Oid::from(50_500u32);

            let wrapper_ptr = pg_sys::palloc0(core::mem::size_of::<
                CustomScanStateWrapper<CountingProvider>,
            >())
                as *mut CustomScanStateWrapper<CountingProvider>;
            assert!(!wrapper_ptr.is_null());
            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;

            // SAFETY: mirrors BeginCustomScan — `ptr::write` before the trampoline reads state.
            (*wrapper_ptr)
                .provider_state
                .as_mut_ptr()
                .write(CountingState::default());
            // Mirror post-Begin init flags so the trampoline sees initialized provider state.
            (*wrapper_ptr).provider_state_initialized = true;
            (*wrapper_ptr).provider_began = true;

            // Populate the cached envelope that rescan reads (mirrors BeginCustomScan).
            let column_refs = vec![ColumnRef {
                expr_index: 0,
                rel_oid: relation_oid,
                attno: 1,
                atttypid: pg_sys::INT4OID,
                attcollation: pg_sys::Oid::INVALID,
                name: None,
            }];
            (*wrapper_ptr).cached_envelope = Some(CachedEnvelope {
                pushed_count: 1,
                recheck_count: 0,
                pushed_contracts: vec![PushdownContract::ExactRowFilter],
                column_refs,
            });

            let cached_ids = make_bms(cached_ids_members);
            (*wrapper_ptr).cached_pushed_param_ids = cached_ids;

            let cscan = make_custom_scan_plan(relation_oid);
            (*wrapper_ptr).base.ss.ps.plan = cscan.cast::<pg_sys::Plan>();

            let scan_rel = make_relation_stub(relation_oid);
            (*wrapper_ptr).base.ss.ss_currentRelation = scan_rel;

            let pli = make_param_list_int4_one();
            let estate = make_estate_stub(pli);
            let econtext = make_econtext_stub();
            (*wrapper_ptr).base.ss.ps.state = estate;
            (*wrapper_ptr).base.ss.ps.ps_ExprContext = econtext;

            let chg = make_bms(chgparam_members);
            (*wrapper_ptr).base.ss.ps.chgParam = chg;

            ChgParamFixture {
                wrapper_ptr,
                node: wrapper_ptr.cast::<pg_sys::CustomScanState>(),
                cached_ids,
            }
        }
    }

    /// Read provider state after rescan; sound because setup used `ptr::write`.
    unsafe fn read_state(
        wrapper_ptr: *mut CustomScanStateWrapper<CountingProvider>,
    ) -> &'static CountingState {
        unsafe { (*wrapper_ptr).provider_state.assume_init_ref() }
    }

    /// Overlapping chgParam triggers rebuild with re-resolved params.
    #[pg_test]
    fn rescan_chgparam_intersect_triggers_rebuild() {
        unsafe {
            let fx = synth_setup_chgparam_rescan(&[1, 2, 3], &[1, 7]);

            rescan_custom_scan_trampoline::<CountingProvider>(fx.node);

            let state = read_state(fx.wrapper_ptr);
            assert_eq!(
                state.rescan_call_count, 1,
                "rescan trampoline must invoke provider.rescan exactly once",
            );
            assert!(
                state.predicate_rebuilt,
                "intersecting chgParam must drive the rebuild branch \
                 ",
            );
            assert_eq!(
                state.resolved_param_count, 1,
                "rebuild branch must re-resolve every Param referenced by \
                 the pushed slice ",
            );
            assert_eq!(
                state.last_pushed_count, 1,
                "pushed_exprs slice must survive re-decoding ",
            );
            assert_eq!(
                state.last_scan_relid, 1,
                "scan_relid must be forwarded verbatim from \
                 cscan->scan.scanrelid ",
            );
            assert_eq!(
                (*fx.wrapper_ptr).cached_pushed_param_ids,
                fx.cached_ids,
                "cached_pushed_param_ids is computed once at Begin and must \
                 NOT be stomped by ReScan ",
            );
        }
    }

    /// Disjoint chgParam skips rebuild (reopen only, empty resolved params).
    #[pg_test]
    fn rescan_chgparam_disjoint_skips_rebuild() {
        unsafe {
            let fx = synth_setup_chgparam_rescan(&[1, 2, 3], &[7, 8]);

            rescan_custom_scan_trampoline::<CountingProvider>(fx.node);

            let state = read_state(fx.wrapper_ptr);
            assert_eq!(
                state.rescan_call_count, 1,
                "reopen-only branch must still invoke provider.rescan exactly \
                 once so the provider can reopen its cursor ",
            );
            assert!(
                !state.predicate_rebuilt,
                "disjoint chgParam must NOT drive the rebuild branch \
                 ",
            );
            assert_eq!(
                state.resolved_param_count, 0,
                "reopen-only branch must hand the provider the empty \
                 resolved-params slice (Requirement 11.3 — no re-resolution; \
                 Requirement 11.4 — no Datum comparison)",
            );
            assert_eq!(
                (*fx.wrapper_ptr).cached_pushed_param_ids,
                fx.cached_ids,
                "cached_pushed_param_ids must remain unchanged across the \
                 reopen-only branch ",
            );
        }
    }

    /// NULL chgParam skips rebuild even when cached param ids are non-empty.
    #[pg_test]
    fn rescan_chgparam_null_skips_rebuild() {
        unsafe {
            let fx = synth_setup_chgparam_rescan(&[1, 2, 3], &[]);
            assert!(
                (*fx.wrapper_ptr).base.ss.ps.chgParam.is_null(),
                "make_bms with no members must produce NULL — PG's \
                 empty-bitmapset convention",
            );

            rescan_custom_scan_trampoline::<CountingProvider>(fx.node);

            let state = read_state(fx.wrapper_ptr);
            assert_eq!(state.rescan_call_count, 1);
            assert!(
                !state.predicate_rebuilt,
                "NULL chgParam must NOT drive the rebuild branch \
                 ",
            );
            assert_eq!(
                state.resolved_param_count, 0,
                "NULL chgParam must NOT re-resolve params ",
            );
        }
    }

    /// `ParamListInfo` with one INT4 slot (paramid 1) holding `value`.
    unsafe fn make_param_list_int4_value(value: i32) -> pg_sys::ParamListInfo {
        unsafe {
            let pli = pg_sys::makeParamList(1);
            let slot: *mut pg_sys::ParamExternData = (*pli).params.as_mut_ptr();
            (*slot).ptype = pg_sys::INT4OID;
            (*slot).value = pg_sys::Datum::from(value);
            (*slot).isnull = false;
            (*slot).pflags = 0;
            pli
        }
    }

    /// Two-slot `es_param_exec_vals` array with slot 1 populated (PARAM_EXEC side).
    unsafe fn make_param_exec_vals_slot1(
        slot1_value: i32,
    ) -> *mut pg_sys::ParamExecData {
        unsafe {
            let vals =
                pg_sys::palloc0(2 * core::mem::size_of::<pg_sys::ParamExecData>())
                    as *mut pg_sys::ParamExecData;
            let slot1 = vals.add(1);
            (*slot1).execPlan = ptr::null_mut();
            (*slot1).value = pg_sys::Datum::from(slot1_value);
            (*slot1).isnull = false;
            vals
        }
    }

    /// Colliding EXTERN/EXEC ids resolve by ParamKey, not numeric id alone.
    #[pg_test]
    fn runtime_param_resolver_mixed_colliding_ids_resolve_by_param_key() {
        unsafe {
            let pli = make_param_list_int4_value(100);
            let estate = make_estate_stub(pli);
            let exec_vals = make_param_exec_vals_slot1(200);
            (*estate).es_param_exec_vals = exec_vals;
            let econtext = make_econtext_stub();

            let extern_refs = [ExternParamRef {
                param_id: 1,
                expected_type: pg_sys::INT4OID,
                collid: pg_sys::Oid::INVALID,
            }];
            let exec_refs = [ExecParamRef {
                param_id: 1,
                expected_type: pg_sys::INT4OID,
                collid: pg_sys::Oid::INVALID,
            }];

            let extern_key = ParamKey {
                paramkind: pg_sys::ParamKind::PARAM_EXTERN,
                param_id: 1,
            };
            let exec_key = ParamKey {
                paramkind: pg_sys::ParamKind::PARAM_EXEC,
                param_id: 1,
            };
            // Same numeric id, distinct ParamKey — kind must disambiguate.
            assert_ne!(
                extern_key, exec_key,
                "colliding numeric ids must produce distinct ParamKeys \
                 (the kind disambiguates them)",
            );

            fn lookup(
                values: &[PgParamValue],
                key: ParamKey,
            ) -> Option<&PgParamValue> {
                values.iter().find(|v| v.key() == key)
            }

            let resolved = RuntimeParamResolver::new(estate, econtext)
                .resolve(&extern_refs, &exec_refs)
                .report_unwrap();
            assert_eq!(
                resolved.len(),
                2,
                "exactly one value per ParamKey: one EXTERN + one EXEC \
                 ",
            );

            let extern_val = lookup(&resolved, extern_key)
                .expect("EXTERN $1 must resolve by its ParamKey ");
            let exec_val = lookup(&resolved, exec_key)
                .expect("EXEC slot 1 must resolve by its ParamKey ");

            assert_eq!(
                extern_val.paramkind,
                pg_sys::ParamKind::PARAM_EXTERN,
                "the EXTERN-keyed value must be stamped PARAM_EXTERN",
            );
            assert_eq!(
                exec_val.paramkind,
                pg_sys::ParamKind::PARAM_EXEC,
                "the EXEC-keyed value must be stamped PARAM_EXEC",
            );
            assert_eq!(
                extern_val.datum.value(),
                100,
                "EXTERN $1 must resolve to its own value (100), not the EXEC \
                 slot's value ",
            );
            assert_eq!(
                exec_val.datum.value(),
                200,
                "EXEC slot 1 must resolve to its own value (200), not the \
                 EXTERN $1 value ",
            );
            // Regression guard: pre-fix bug collapsed both ids onto the EXTERN value.
            assert_ne!(
                extern_val.datum.value(),
                exec_val.datum.value(),
                "colliding (EXTERN $1) and (EXEC slot 1) must resolve to \
                 DISTINCT values — collapsing them to one value is the \
                 param-kind-collision data-loss bug ",
            );

            (*exec_vals.add(1)).value = pg_sys::Datum::from(300i32);

            let resolved_rescan = RuntimeParamResolver::new(estate, econtext)
                .resolve(&extern_refs, &exec_refs)
                .report_unwrap();
            assert_eq!(
                resolved_rescan.len(),
                2,
                "re-resolution still yields exactly one value per ParamKey",
            );

            let extern_val2 = lookup(&resolved_rescan, extern_key)
                .expect("EXTERN $1 must still resolve after ReScan");
            let exec_val2 = lookup(&resolved_rescan, exec_key)
                .expect("EXEC slot 1 must still resolve after ReScan");

            assert_eq!(
                exec_val2.datum.value(),
                300,
                "the changed EXEC slot must re-resolve to its new value (300) \
                 by ParamKey after ReScan ",
            );
            assert_eq!(
                extern_val2.datum.value(),
                100,
                "the EXTERN $1 value must be unaffected by the EXEC change — \
                 the two ParamKeys never alias ",
            );
        }
    }

    use core::ffi::c_void;

    use pg_lakebase_core::customscan::exec::next_slot_wrapper;
    use pg_lakebase_core::handles::RelationHandle;
    use pg_lakebase_core::tuple::{Cell, Row};
    use pgrx::FromDatum;

    /// Text varlena payload for the tts_mcxt vs per-tuple context test.
    const HANDLE_EMIT_TEXT: &str =
        "lakebase-emit-row-varlena-must-survive-the-per-tuple-context-reset";

    /// Two-column (`int4`, `text`) tuple descriptor for handle and slot fixtures.
    unsafe fn make_two_col_tupdesc() -> pg_sys::TupleDesc {
        unsafe {
            let tupdesc = pg_sys::CreateTemplateTupleDesc(2);
            pg_sys::TupleDescInitEntry(
                tupdesc,
                1,
                c"c_int".as_ptr(),
                pg_sys::INT4OID,
                -1,
                0,
            );
            pg_sys::TupleDescInitEntry(
                tupdesc,
                2,
                c"c_text".as_ptr(),
                pg_sys::TEXTOID,
                -1,
                0,
            );
            tupdesc
        }
    }

    /// `RelationHandle::oid()` and `natts()` match `rd_id` and `rd_att->natts`.
    #[pg_test]
    fn relation_handle_accessors_match_tupdesc_and_oid() {
        unsafe {
            let rel_oid = pg_sys::Oid::from(50_701u32);
            let tupdesc = make_two_col_tupdesc();

            let rel = pg_sys::palloc0(core::mem::size_of::<pg_sys::RelationData>())
                as pg_sys::Relation;
            (*rel).rd_id = rel_oid;
            (*rel).rd_att = tupdesc;

            // SAFETY: `rel` is a live `RelationData` shell with `rd_id` and
            // `rd_att` populated, valid for the duration of this test.
            let handle = RelationHandle::from_raw(rel);

            assert_eq!(
                handle.oid(),
                rel_oid,
                "RelationHandle::oid() must equal the relation's rd_id ",
            );
            assert_eq!(
                handle.natts(),
                (*tupdesc).natts as usize,
                "RelationHandle::natts() must equal tupdesc->natts ",
            );
            assert_eq!(
                handle.natts(),
                2,
                "the fixture descriptor has exactly two attributes",
            );
        }
    }

    struct HandleAccessorProvider;

    struct HandleAccessorState {
        seen_natts: usize,
        seen_oid: pg_sys::Oid,
        emitted: bool,
    }

    /// Empty private data; `next_slot_wrapper` never decodes provider-private cells.
    struct HandleAccessorPrivate;

    impl CustomScanPrivate for HandleAccessorPrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(HandleAccessorPrivate)
        }
    }

    impl LakebaseCustomScanProvider for HandleAccessorProvider {
        const NAME: &'static core::ffi::CStr = c"handle-accessor-test-provider";
        type PrivateData = HandleAccessorPrivate;
        type State = HandleAccessorState;

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
            HandleAccessorState {
                seen_natts: 0,
                seen_oid: pg_sys::Oid::INVALID,
                emitted: false,
            }
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("this test drives next_slot directly; begin is not invoked");
        }

        /// Read handle accessors and emit a text varlena via `emit_row`.
        fn next_slot(
            mut ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            ctx.state.seen_natts = ctx.relation.natts();
            ctx.state.seen_oid = ctx.relation.oid();

            let mut row = Row::with_capacity(2);
            row.set_cell(0, Some(Cell::I32(7)));
            row.set_cell(1, Some(Cell::String(HANDLE_EMIT_TEXT.to_string())));
            ctx.emit_row(&mut row)?;

            ctx.state.emitted = true;
            Ok(true)
        }

        fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "this test drives next_slot directly; rescan is not invoked"
            );
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("this test drives next_slot directly; end is not invoked");
        }
    }

    /// Handle fixture with per-tuple ctx distinct from the slot's `tts_mcxt`.
    struct HandleFixture {
        wrapper_ptr: *mut CustomScanStateWrapper<HandleAccessorProvider>,
        scan_state: *mut pg_sys::ScanState,
        slot: *mut pg_sys::TupleTableSlot,
        per_tuple_ctx: pg_sys::MemoryContext,
        tupdesc: pg_sys::TupleDesc,
        rel_oid: pg_sys::Oid,
    }

    /// Build a [`HandleFixture`] for next_slot/emit_row tests.
    unsafe fn synth_handle_accessor_wrapper() -> HandleFixture {
        unsafe {
            let rel_oid = pg_sys::Oid::from(50_702u32);
            let tupdesc = make_two_col_tupdesc();

            // Slot `tts_mcxt` is the per-query context, not the per-tuple ctx below.
            let slot = pg_sys::MakeTupleTableSlot(tupdesc, &pg_sys::TTSOpsVirtual);

            let rel = pg_sys::palloc0(core::mem::size_of::<pg_sys::RelationData>())
                as pg_sys::Relation;
            (*rel).rd_id = rel_oid;
            (*rel).rd_att = tupdesc;

            let estate = pg_sys::palloc0(core::mem::size_of::<pg_sys::EState>())
                as *mut pg_sys::EState;
            (*estate).type_ = pg_sys::NodeTag::T_EState;

            let per_tuple_ctx = pg_sys::AllocSetContextCreateExtended(
                pg_sys::CurrentMemoryContext,
                c"lakebase test per-tuple ctx".as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            );

            let econtext = pg_sys::palloc0(core::mem::size_of::<pg_sys::ExprContext>())
                as *mut pg_sys::ExprContext;
            (*econtext).type_ = pg_sys::NodeTag::T_ExprContext;
            (*econtext).ecxt_per_tuple_memory = per_tuple_ctx;
            (*econtext).ecxt_per_query_memory = pg_sys::CurrentMemoryContext;

            let wrapper_ptr = pg_sys::palloc0(core::mem::size_of::<
                CustomScanStateWrapper<HandleAccessorProvider>,
            >())
                as *mut CustomScanStateWrapper<HandleAccessorProvider>;
            assert!(!wrapper_ptr.is_null());
            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;

            // SAFETY: mirrors BeginCustomScan — `ptr::write` before next_slot reads state.
            (*wrapper_ptr)
                .provider_state
                .as_mut_ptr()
                .write(HandleAccessorState {
                    seen_natts: 0,
                    seen_oid: pg_sys::Oid::INVALID,
                    emitted: false,
                });
            (*wrapper_ptr).provider_state_initialized = true;
            (*wrapper_ptr).provider_began = true;

            (*wrapper_ptr).base.ss.ss_ScanTupleSlot = slot;
            (*wrapper_ptr).base.ss.ss_currentRelation = rel;
            (*wrapper_ptr).base.ss.ps.state = estate;
            (*wrapper_ptr).base.ss.ps.ps_ExprContext = econtext;

            let scan_state: *mut pg_sys::ScanState =
                core::ptr::addr_of_mut!((*wrapper_ptr).base.ss);

            HandleFixture {
                wrapper_ptr,
                scan_state,
                slot,
                per_tuple_ctx,
                tupdesc,
                rel_oid,
            }
        }
    }

    /// `emit_row` varlena lives in `tts_mcxt` and survives per-tuple reset.
    #[pg_test]
    fn emit_row_targets_tts_mcxt_and_survives_per_tuple_reset() {
        unsafe {
            let fx = synth_handle_accessor_wrapper();

            let returned = next_slot_wrapper::<HandleAccessorProvider>(fx.scan_state);
            assert_eq!(
                returned, fx.slot,
                "next_slot_wrapper must return the filled scan slot on Ok(true)",
            );

            let state = (*fx.wrapper_ptr).provider_state.assume_init_ref();
            assert!(
                state.emitted,
                "the provider's next_slot must have run and emitted a row",
            );
            assert_eq!(
                state.seen_natts,
                (*fx.tupdesc).natts as usize,
                "ctx.relation.natts() (seen by the provider) must equal \
                 tupdesc->natts ",
            );
            assert_eq!(
                state.seen_natts, 2,
                "the fixture relation has exactly two attributes",
            );
            assert_eq!(
                state.seen_oid, fx.rel_oid,
                "ctx.relation.oid() (seen by the provider) must equal the \
                 relation's rd_id ",
            );

            let flags = (*fx.slot).tts_flags as u32;
            assert_eq!(
                flags & pg_sys::TTS_FLAG_EMPTY,
                0,
                "emit_row must mark the slot non-empty ",
            );

            let text_datum: pg_sys::Datum = *(*fx.slot).tts_values.add(1);
            let text_is_null: bool = *(*fx.slot).tts_isnull.add(1);
            assert!(!text_is_null, "the emitted text column must be non-NULL");

            let tts_mcxt = (*fx.slot).tts_mcxt;
            assert_ne!(
                tts_mcxt, fx.per_tuple_ctx,
                "the slot's tts_mcxt must differ from the per-tuple context \
                 for the per-tuple-reset proof to be meaningful",
            );

            let chunk_ctx =
                pg_sys::GetMemoryChunkContext(text_datum.cast_mut_ptr::<c_void>());
            assert_eq!(
                chunk_ctx, tts_mcxt,
                "emit_row must allocate the varlena in the slot's tts_mcxt, \
                 proving the framework owns the slot-lifetime context switch \
                 ",
            );
            assert_ne!(
                chunk_ctx, fx.per_tuple_ctx,
                "the emitted varlena must NOT live in the per-tuple context \
                 ",
            );

            pg_sys::MemoryContextReset(fx.per_tuple_ctx);

            let survived = String::from_datum(text_datum, false)
                .expect("the emitted text must survive a per-tuple context reset");
            assert_eq!(
                survived, HANDLE_EMIT_TEXT,
                "a varlena written via emit_row must survive a per-tuple \
                 context reset — proving the framework targeted tts_mcxt \
                 ",
            );
        }
    }
}
