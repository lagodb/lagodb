//! Backend tests for the ReScan trampoline's chgParam gating, plus the
//! `bms_overlap` semantics that gating relies on.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_int;
    use std::ptr;

    use crate::lakebase_core::customscan::exec::support::{
        make_econtext_stub, make_estate_stub,
    };
    use crate::lakebase_core::support::pg::{OpExprSpec, PgNodeBuilder};
    use pg_lakebase_core::customscan::ScanPurpose;
    use pg_lakebase_core::customscan::codec::{PrivateDataReader, PrivateDataWriter};
    use pg_lakebase_core::customscan::custom_private::{
        CustomScanPrivate, encode_split,
    };
    use pg_lakebase_core::customscan::exec::{
        CustomExprSections, RuntimeParamRefs, rescan_custom_scan_trampoline,
    };
    use pg_lakebase_core::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
        PathVariant, PlanTranslateContext, ReScanContext, RelPathContext,
        ScanTupleLayout,
    };
    use pg_lakebase_core::customscan::state::{
        CachedEnvelope, CustomScanStateWrapper,
    };
    use pg_lakebase_core::expr::split::{
        ColumnRef, PushdownContract, QualPushdownDecision,
    };
    use pgrx::pg_sys;
    use pgrx::pg_test;

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
            _predicate: &pg_lakebase_core::expr::predicate::PlanPredicate,
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

    /// Two-slot `PARAM_EXEC` array with slot 1 holding value 42.
    unsafe fn make_exec_params_int4_one() -> *mut pg_sys::ParamExecData {
        unsafe {
            let slots =
                pg_sys::palloc0(2 * core::mem::size_of::<pg_sys::ParamExecData>())
                    as *mut pg_sys::ParamExecData;
            let slot = slots.add(1);
            (*slot).value = pg_sys::Datum::from(42i32);
            (*slot).isnull = false;
            (*slot).execPlan = ptr::null_mut();
            slots
        }
    }

    /// Synthetic `CustomScan` with one pushed EXEC param and matching envelope.
    unsafe fn make_custom_scan_plan(
        relation_oid: pg_sys::Oid,
    ) -> *mut pg_sys::CustomScan {
        unsafe {
            let nodes = PgNodeBuilder::new(1);
            let v = nodes.int4_var(1);
            let p = nodes.int4_param(pg_sys::ParamKind::PARAM_EXEC, 1);

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
                purpose: ScanPurpose::Query,
                pushed_contracts: vec![PushdownContract::ExactRowFilter],
                column_refs,
                tuple_layout: ScanTupleLayout::default(),
            });

            let cscan = make_custom_scan_plan(relation_oid);
            (*wrapper_ptr).base.ss.ps.plan = cscan.cast::<pg_sys::Plan>();
            let expr_sections =
                CustomExprSections::from_custom_exprs((*cscan).custom_exprs, 1, 0)
                    .expect("fixture custom_exprs must have one pushed expression");
            let runtime_params =
                RuntimeParamRefs::collect_from_exprs(expr_sections.pushed());
            let cached_ids = runtime_params.exec_param_ids();
            (*wrapper_ptr).expr_sections = Some(expr_sections);
            (*wrapper_ptr).runtime_params = Some(runtime_params);

            let scan_rel = make_relation_stub(relation_oid);
            (*wrapper_ptr).base.ss.ss_currentRelation = scan_rel;

            let estate = make_estate_stub(ptr::null_mut());
            (*estate).es_param_exec_vals = make_exec_params_int4_one();
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
            let fx = synth_setup_chgparam_rescan(&[1, 7]);

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
                (*fx.wrapper_ptr)
                    .runtime_params
                    .as_ref()
                    .expect("runtime params cached")
                    .exec_param_ids(),
                fx.cached_ids,
                "runtime param ids are computed once at Begin and must \
                 NOT be stomped by ReScan ",
            );
        }
    }

    /// Disjoint chgParam skips rebuild (reopen only, empty resolved params).
    #[pg_test]
    fn rescan_chgparam_disjoint_skips_rebuild() {
        unsafe {
            let fx = synth_setup_chgparam_rescan(&[7, 8]);

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
                 resolved-params slice",
            );
            assert_eq!(
                (*fx.wrapper_ptr)
                    .runtime_params
                    .as_ref()
                    .expect("runtime params cached")
                    .exec_param_ids(),
                fx.cached_ids,
                "runtime param ids must remain unchanged across the \
                 reopen-only branch ",
            );
        }
    }

    /// NULL chgParam skips rebuild even when cached param ids are non-empty.
    #[pg_test]
    fn rescan_chgparam_null_skips_rebuild() {
        unsafe {
            let fx = synth_setup_chgparam_rescan(&[]);
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
}
