//! Backend tests for customscan provider registry and framework trampolines.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ptr;

    use crate::lakebase_core::support::pg::PlannerRelFixture;
    use pg_lakebase_core::customscan::codec::{PrivateDataReader, PrivateDataWriter};
    use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;
    use pg_lakebase_core::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
        PathVariant, PlanTranslateContext, ReScanContext, RelPathContext,
        find_matching_provider, register_provider,
    };
    use pg_lakebase_core::diag::ReportableError;
    use pg_lakebase_core::expr::split::QualPushdownDecision;
    use pgrx::pg_sys;
    use pgrx::pg_test;

    /// Smallest valid 1-based RTI; matches `expr::split` fixtures.
    const PSG_RELID: u32 = 1;

    /// Synthetic relation OID both registry test providers claim.
    const PSG_REL_OID: u32 = 50_500;

    /// Synthetic planner triple for [`RelPathContext`]; gate fields default to pass.
    unsafe fn make_psg_state() -> (
        *mut pg_sys::PlannerInfo,
        *mut pg_sys::RelOptInfo,
        *mut pg_sys::RangeTblEntry,
    ) {
        let fixture = unsafe { PlannerRelFixture::relation(PSG_RELID, PSG_REL_OID) };
        (fixture.root, fixture.baserel, fixture.rte)
    }

    /// Stub private data; encode/decode never run in registry tests.
    struct PsgPrivate;

    impl CustomScanPrivate for PsgPrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(PsgPrivate)
        }
    }

    struct PsgStateA;
    struct PsgStateB;

    /// Macro for two providers that both claim `PSG_REL_OID`.
    macro_rules! impl_psg_provider {
        ($ty:ty, $name:expr, $state:ty) => {
            impl LakebaseCustomScanProvider for $ty {
                const NAME: &'static core::ffi::CStr = $name;
                type PrivateData = PsgPrivate;
                type State = $state;

                fn supports_relation(ctx: &RelPathContext) -> bool {
                    ctx.rel_oid() == pg_sys::Oid::from(PSG_REL_OID)
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
                    unreachable!(
                        "create_state is not exercised by the registry uniqueness test"
                    );
                }

                fn begin(_ctx: BeginContext<Self>) -> Result<(), CustomScanError> {
                    unreachable!("begin is not exercised by the registry uniqueness test");
                }

                fn next_slot(_ctx: NextSlotContext<'_, Self>) -> Result<bool, CustomScanError> {
                    unreachable!("next_slot is not exercised by the registry uniqueness test");
                }

                fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
                    unreachable!("rescan is not exercised by the registry uniqueness test");
                }

                fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
                    unreachable!("end is not exercised by the registry uniqueness test");
                }
            }
        };
    }

    struct PsgProviderA;
    struct PsgProviderB;

    impl_psg_provider!(PsgProviderA, c"psg-uniqueness-test-a", PsgStateA);
    impl_psg_provider!(PsgProviderB, c"psg-uniqueness-test-b", PsgStateB);

    /// Two matching providers raise ERROR.
    #[pg_test(error = "multiple LakebaseCustomScanProviders match relation 50500")]
    fn registry_multiple_providers_raises_error() {
        register_provider::<PsgProviderA>();
        register_provider::<PsgProviderB>();

        unsafe {
            let (_root, _baserel, rte) = make_psg_state();
            // SAFETY: `make_psg_state` returns a live `RangeTblEntry` in
            // the test backend's per-query context.
            let ctx = RelPathContext::new(rte);

            let _ = find_matching_provider(&ctx).report_unwrap();

            panic!(
                "find_matching_provider returned instead of raising ereport(ERROR) \
                 for two providers matching the same relation",
            );
        }
    }

    use pg_lakebase_core::customscan::exec::next_slot_wrapper;
    use pg_lakebase_core::customscan::state::CustomScanStateWrapper;

    struct SlotInvariantPrivate;

    impl CustomScanPrivate for SlotInvariantPrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(SlotInvariantPrivate)
        }
    }

    struct SlotInvariantState;

    /// Returns `Ok(true)` without filling the scan slot.
    struct EmptySlotProvider;

    impl LakebaseCustomScanProvider for EmptySlotProvider {
        const NAME: &'static core::ffi::CStr = c"fake-slot-invariant-provider";
        type PrivateData = SlotInvariantPrivate;
        type State = SlotInvariantState;

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
            SlotInvariantState
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "Req 12 test drives next_slot directly; begin is not invoked"
            );
        }

        /// Claims a row without calling `emit_row`; slot stays empty after clear.
        fn next_slot(
            _ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            Ok(true)
        }

        fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "Req 12 test drives next_slot directly; rescan is not invoked"
            );
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("Req 12 test drives next_slot directly; end is not invoked");
        }
    }

    /// Virtual int4 slot; cleared by `next_slot_wrapper` before the provider runs.
    unsafe fn make_int4_slot() -> *mut pg_sys::TupleTableSlot {
        unsafe {
            let tupdesc = pg_sys::CreateTemplateTupleDesc(1);
            pg_sys::TupleDescInitEntry(
                tupdesc,
                1,
                c"c1".as_ptr(),
                pg_sys::INT4OID,
                -1,
                0,
            );
            // v1 slots are virtual (`TTSOpsVirtual`), matching what
            // `ExecInitCustomScan` produces for `ss_ScanTupleSlot`.
            pg_sys::MakeTupleTableSlot(tupdesc, &pg_sys::TTSOpsVirtual)
        }
    }

    /// Minimal `RelationData` shell; empty-slot path never reads it.
    unsafe fn make_relation_stub(oid: pg_sys::Oid) -> pg_sys::Relation {
        unsafe {
            let rel = pg_sys::palloc0(core::mem::size_of::<pg_sys::RelationData>())
                as pg_sys::Relation;
            (*rel).rd_id = oid;
            rel
        }
    }

    unsafe fn make_estate_stub() -> *mut pg_sys::EState {
        unsafe {
            let estate = pg_sys::palloc0(core::mem::size_of::<pg_sys::EState>())
                as *mut pg_sys::EState;
            (*estate).type_ = pg_sys::NodeTag::T_EState;
            estate
        }
    }

    /// Per-tuple/per-query contexts point at `CurrentMemoryContext` for test cleanup.
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

    unsafe fn synth_empty_slot_wrapper()
    -> *mut CustomScanStateWrapper<EmptySlotProvider> {
        unsafe {
            let wrapper_ptr = pg_sys::palloc0(core::mem::size_of::<
                CustomScanStateWrapper<EmptySlotProvider>,
            >())
                as *mut CustomScanStateWrapper<EmptySlotProvider>;
            assert!(!wrapper_ptr.is_null());

            // NodeTag required for cast-back through `CustomScanState`.
            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;

            // Mirror `BeginCustomScan`: initialize `MaybeUninit<P::State>`.
            (*wrapper_ptr)
                .provider_state
                .as_mut_ptr()
                .write(SlotInvariantState);
            (*wrapper_ptr).provider_state_initialized = true;
            (*wrapper_ptr).provider_began = true;

            (*wrapper_ptr).base.ss.ss_ScanTupleSlot = make_int4_slot();
            (*wrapper_ptr).base.ss.ss_currentRelation =
                make_relation_stub(pg_sys::Oid::from(50_600u32));
            (*wrapper_ptr).base.ss.ps.state = make_estate_stub();
            (*wrapper_ptr).base.ss.ps.ps_ExprContext = make_econtext_stub();

            wrapper_ptr
        }
    }

    /// `Ok(true)` with empty slot raises `SlotNotFilled`.
    #[pg_test(
        error = "customscan \"fake-slot-invariant-provider\" provider.next_slot failed: customscan provider \"fake-slot-invariant-provider\" returned Ok(true) without filling the scan slot (slot-non-empty invariant violated)"
    )]
    fn next_slot_empty_slot_raises_hard_error_independent_of_debug_assert() {
        unsafe {
            let wrapper_ptr = synth_empty_slot_wrapper();
            // `ScanState` is the first field of the `#[repr(C)]` wrapper.
            let scan_state: *mut pg_sys::ScanState =
                core::ptr::addr_of_mut!((*wrapper_ptr).base.ss);

            let _returned_slot = next_slot_wrapper::<EmptySlotProvider>(scan_state);

            panic!(
                "next_slot_wrapper returned instead of \
                 raising for an Ok(true) with an empty slot (silent truncation)",
            );
        }
    }

    use pg_lakebase_core::customscan::custom_private::encode_split;
    use pg_lakebase_core::customscan::exec::begin_custom_scan_trampoline;

    /// Reports whether the prior memory context was restored before raise.
    #[derive(Debug)]
    struct ContextProbeError {
        provider_ctx_addr: usize,
    }

    impl std::fmt::Display for ContextProbeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            // SAFETY: reading the backend-local `CurrentMemoryContext` global is
            // a plain pointer load; this runs on the same backend thread.
            let current = unsafe { pg_sys::CurrentMemoryContext } as usize;
            let verdict = if current == self.provider_ctx_addr {
                "still-in-provider-context"
            } else {
                "prior-context-restored"
            };
            write!(f, "context probe: {verdict}")
        }
    }

    impl std::error::Error for ContextProbeError {}

    impl pg_lakebase_core::diag::SqlStateError for ContextProbeError {
        fn sql_error_code(&self) -> pgrx::prelude::PgSqlErrorCode {
            pgrx::prelude::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
        }
    }

    struct ContextProbePrivate;

    impl CustomScanPrivate for ContextProbePrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            _reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            Ok(ContextProbePrivate)
        }
    }

    struct ContextProbeState;

    const PROBE_PER_TUPLE_CTX_NAME: &core::ffi::CStr =
        c"lakebase-ctx-probe-per-tuple";
    const PROBE_BEGIN_SCRATCH_CTX_NAME: &core::ffi::CStr =
        c"lakebase-ctx-probe-begin-scratch";

    /// Child context with a distinct pointer from the per-query context.
    unsafe fn make_child_context(
        name: &'static core::ffi::CStr,
    ) -> pg_sys::MemoryContext {
        unsafe {
            pg_sys::AllocSetContextCreateExtended(
                pg_sys::CurrentMemoryContext,
                name.as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            )
        }
    }

    struct ContextProbeProvider;

    impl LakebaseCustomScanProvider for ContextProbeProvider {
        const NAME: &'static core::ffi::CStr = c"fake-context-probe-provider";
        type PrivateData = ContextProbePrivate;
        type State = ContextProbeState;

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
            ContextProbeState
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unsafe {
                let scratch = make_child_context(PROBE_BEGIN_SCRATCH_CTX_NAME);
                let _ = pg_sys::MemoryContextSwitchTo(scratch);
                Err(CustomScanError::provider(ContextProbeError {
                    provider_ctx_addr: scratch as usize,
                }))
            }
        }

        fn next_slot(
            _ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            // SAFETY: plain pointer load of the backend-local global.
            let per_tuple_addr = unsafe { pg_sys::CurrentMemoryContext } as usize;
            Err(CustomScanError::provider(ContextProbeError {
                provider_ctx_addr: per_tuple_addr,
            }))
        }

        fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "this test drives begin/next_slot directly; rescan is not invoked"
            );
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "this test drives begin/next_slot directly; end is not invoked"
            );
        }
    }

    /// Per-tuple context is a fresh child so restore is observable by pointer.
    unsafe fn make_econtext_distinct_per_tuple() -> *mut pg_sys::ExprContext {
        unsafe {
            let econtext = pg_sys::palloc0(core::mem::size_of::<pg_sys::ExprContext>())
                as *mut pg_sys::ExprContext;
            (*econtext).type_ = pg_sys::NodeTag::T_ExprContext;
            (*econtext).ecxt_per_tuple_memory =
                make_child_context(PROBE_PER_TUPLE_CTX_NAME);
            (*econtext).ecxt_per_query_memory = pg_sys::CurrentMemoryContext;
            econtext
        }
    }

    unsafe fn synth_context_probe_wrapper()
    -> *mut CustomScanStateWrapper<ContextProbeProvider> {
        unsafe {
            let wrapper_ptr = pg_sys::palloc0(core::mem::size_of::<
                CustomScanStateWrapper<ContextProbeProvider>,
            >())
                as *mut CustomScanStateWrapper<ContextProbeProvider>;
            assert!(!wrapper_ptr.is_null());

            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
            (*wrapper_ptr)
                .provider_state
                .as_mut_ptr()
                .write(ContextProbeState);
            (*wrapper_ptr).provider_state_initialized = true;
            (*wrapper_ptr).provider_began = true;

            (*wrapper_ptr).base.ss.ss_ScanTupleSlot = make_int4_slot();
            (*wrapper_ptr).base.ss.ss_currentRelation =
                make_relation_stub(pg_sys::Oid::from(50_700u32));
            (*wrapper_ptr).base.ss.ps.state = make_estate_stub();
            (*wrapper_ptr).base.ss.ps.ps_ExprContext =
                make_econtext_distinct_per_tuple();

            wrapper_ptr
        }
    }

    /// `next_slot` `Err` restores prior context before raise (probe emits `prior-context-restored`).
    #[pg_test(
        error = "customscan \"fake-context-probe-provider\" provider.next_slot failed: customscan provider error: context probe: prior-context-restored"
    )]
    fn next_slot_err_restores_prior_context_before_raise() {
        unsafe {
            let wrapper_ptr = synth_context_probe_wrapper();
            let scan_state: *mut pg_sys::ScanState =
                core::ptr::addr_of_mut!((*wrapper_ptr).base.ss);

            let _ = next_slot_wrapper::<ContextProbeProvider>(scan_state);

            panic!(
                "next_slot_wrapper returned instead of \
                 raising the provider Err through the Error_Raise_Point",
            );
        }
    }

    /// `EState` with non-null `es_snapshot`; NULL param fields short-circuit param resolution.
    unsafe fn make_begin_estate_stub() -> *mut pg_sys::EState {
        unsafe {
            let estate = pg_sys::palloc0(core::mem::size_of::<pg_sys::EState>())
                as *mut pg_sys::EState;
            (*estate).type_ = pg_sys::NodeTag::T_EState;
            let snapshot =
                pg_sys::palloc0(core::mem::size_of::<pg_sys::SnapshotData>())
                    as pg_sys::Snapshot;
            (*estate).es_snapshot = snapshot;
            estate
        }
    }

    /// Begin-trampoline plan; encoded `relation_oid` must match stub `rd_id`.
    unsafe fn make_begin_custom_scan_plan(
        relation_oid: pg_sys::Oid,
    ) -> *mut pg_sys::CustomScan {
        unsafe {
            let custom_private = encode_split(
                ContextProbeProvider::NAME,
                relation_oid,
                0,               // pushed_count
                0,               // recheck_count
                &[],             // pushed_contracts (len must equal pushed_count = 0)
                &[],             // column_refs
                ptr::null_mut(), // provider_metadata -> decode(NULL) = Ok(unit)
                1,               // pre_setrefs_scan_rti — debug-only
            )
            .expect("encode_split must succeed for the begin fixture");

            let cscan = pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomScan>())
                as *mut pg_sys::CustomScan;
            (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
            (*cscan).scan.scanrelid = 1;
            (*cscan).custom_exprs = ptr::null_mut();
            (*cscan).custom_private = custom_private;
            cscan
        }
    }

    unsafe fn synth_begin_context_probe() -> (
        *mut CustomScanStateWrapper<ContextProbeProvider>,
        *mut pg_sys::EState,
    ) {
        unsafe {
            let relation_oid = pg_sys::Oid::from(50_701u32);

            let wrapper_ptr = pg_sys::palloc0(core::mem::size_of::<
                CustomScanStateWrapper<ContextProbeProvider>,
            >())
                as *mut CustomScanStateWrapper<ContextProbeProvider>;
            assert!(!wrapper_ptr.is_null());
            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;

            let cscan = make_begin_custom_scan_plan(relation_oid);
            (*wrapper_ptr).base.ss.ps.plan = cscan.cast::<pg_sys::Plan>();
            (*wrapper_ptr).base.ss.ss_currentRelation =
                make_relation_stub(relation_oid);
            (*wrapper_ptr).base.ss.ss_ScanTupleSlot = make_int4_slot();

            // Begin does not switch into `ps_ExprContext`; provider uses its own scratch ctx.
            (*wrapper_ptr).base.ss.ps.ps_ExprContext = make_econtext_stub();

            let estate = make_begin_estate_stub();

            (wrapper_ptr, estate)
        }
    }

    /// `begin` `Err` restores prior context before raise (probe emits `prior-context-restored`).
    #[pg_test(
        error = "customscan \"fake-context-probe-provider\" provider.begin failed: customscan provider error: context probe: prior-context-restored"
    )]
    fn begin_err_restores_prior_context_before_raise() {
        unsafe {
            let (wrapper_ptr, estate) = synth_begin_context_probe();
            let node: *mut pg_sys::CustomScanState =
                wrapper_ptr.cast::<pg_sys::CustomScanState>();

            begin_custom_scan_trampoline::<ContextProbeProvider>(node, estate, 0);

            panic!(
                "begin_custom_scan_trampoline returned instead \
                 of raising the provider Err through the Error_Raise_Point",
            );
        }
    }

    struct BoundaryDecodeFailPrivate;

    impl CustomScanPrivate for BoundaryDecodeFailPrivate {
        fn encode(
            &self,
            _writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            Ok(())
        }

        fn decode(
            reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            // NULL provider_metadata: real providers fail via codec `read_*`, not `internal`.
            let _oid = reader.read_oid()?;
            Ok(BoundaryDecodeFailPrivate)
        }
    }

    struct BoundaryDecodeFailState;

    struct BoundaryDecodeFailProvider;

    impl LakebaseCustomScanProvider for BoundaryDecodeFailProvider {
        const NAME: &'static core::ffi::CStr = c"boundary-decode-fail-provider";
        type PrivateData = BoundaryDecodeFailPrivate;
        type State = BoundaryDecodeFailState;

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
            unreachable!(
                "decode-fail test diverges at decode_provider_private; \
                 create_state is not reached"
            );
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "decode-fail test diverges at decode_provider_private; \
                 begin is not reached"
            );
        }

        fn next_slot(
            _ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            unreachable!(
                "decode-fail test diverges at decode_provider_private; \
                 next_slot is not reached"
            );
        }

        fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "decode-fail test diverges at decode_provider_private; \
                 rescan is not reached"
            );
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "decode-fail test diverges at decode_provider_private; \
                 end is not reached"
            );
        }
    }

    unsafe fn make_decode_fail_custom_scan_plan(
        relation_oid: pg_sys::Oid,
    ) -> *mut pg_sys::CustomScan {
        unsafe {
            let custom_private = encode_split(
                BoundaryDecodeFailProvider::NAME,
                relation_oid,
                0,               // pushed_count
                0,               // recheck_count
                &[],             // pushed_contracts (len must equal pushed_count = 0)
                &[],             // column_refs
                ptr::null_mut(), // provider_metadata -> decode(NULL) = Err(NullPayload)
                1,               // pre_setrefs_scan_rti — debug-only
            )
            .expect("encode_split must succeed for the task decode-fail fixture");

            let cscan = pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomScan>())
                as *mut pg_sys::CustomScan;
            (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
            (*cscan).scan.scanrelid = 1;
            (*cscan).custom_exprs = ptr::null_mut();
            (*cscan).custom_private = custom_private;
            cscan
        }
    }

    unsafe fn synth_begin_decode_fail() -> (
        *mut CustomScanStateWrapper<BoundaryDecodeFailProvider>,
        *mut pg_sys::EState,
    ) {
        unsafe {
            let relation_oid = pg_sys::Oid::from(50_801u32);

            let wrapper_ptr = pg_sys::palloc0(core::mem::size_of::<
                CustomScanStateWrapper<BoundaryDecodeFailProvider>,
            >())
                as *mut CustomScanStateWrapper<BoundaryDecodeFailProvider>;
            assert!(!wrapper_ptr.is_null());
            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;

            let cscan = make_decode_fail_custom_scan_plan(relation_oid);
            (*wrapper_ptr).base.ss.ps.plan = cscan.cast::<pg_sys::Plan>();
            (*wrapper_ptr).base.ss.ss_currentRelation =
                make_relation_stub(relation_oid);
            (*wrapper_ptr).base.ss.ps.ps_ExprContext = make_econtext_stub();

            let estate = make_begin_estate_stub();

            (wrapper_ptr, estate)
        }
    }

    /// Provider-private `decode` failure surfaces at the begin boundary with provider-name wrapping.
    #[pg_test(
        error = "customscan \"boundary-decode-fail-provider\" provider failed to decode custom_private payload: customscan custom_private codec error: custom_private read past end of payload: position 0, len 0"
    )]
    fn decode_provider_private_boundary_raises_on_decode_error() {
        unsafe {
            let (wrapper_ptr, estate) = synth_begin_decode_fail();
            let node: *mut pg_sys::CustomScanState =
                wrapper_ptr.cast::<pg_sys::CustomScanState>();

            begin_custom_scan_trampoline::<BoundaryDecodeFailProvider>(
                node, estate, 0,
            );

            panic!(
                "begin_custom_scan_trampoline returned instead of raising \
                 ereport(ERROR) for a failing provider-private decode",
            );
        }
    }

    /// Reads one OID; trailing cells rejected by `reader.finish()`.
    struct CodecMalformedPrivate {
        #[allow(dead_code)]
        tablespace_oid: pg_sys::Oid,
    }

    impl CustomScanPrivate for CodecMalformedPrivate {
        fn encode(
            &self,
            writer: &mut PrivateDataWriter,
        ) -> Result<(), CustomScanError> {
            writer.append_oid(self.tablespace_oid);
            Ok(())
        }

        fn decode(
            reader: &mut PrivateDataReader<'_>,
        ) -> Result<Self, CustomScanError> {
            let tablespace_oid = reader.read_oid()?;
            Ok(Self { tablespace_oid })
        }
    }

    struct CodecMalformedState;

    struct CodecMalformedProvider;

    impl LakebaseCustomScanProvider for CodecMalformedProvider {
        const NAME: &'static core::ffi::CStr = c"codec-malformed-provider";
        type PrivateData = CodecMalformedPrivate;
        type State = CodecMalformedState;

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
            unreachable!(
                "codec-malformed test diverges at decode_provider_private's \
                 chained reader.finish(); create_state is not reached"
            );
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "codec-malformed test diverges at decode_provider_private's \
                 chained reader.finish(); begin is not reached"
            );
        }

        fn next_slot(
            _ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            unreachable!(
                "codec-malformed test diverges at decode_provider_private's \
                 chained reader.finish(); next_slot is not reached"
            );
        }

        fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "codec-malformed test diverges at decode_provider_private's \
                 chained reader.finish(); rescan is not reached"
            );
        }

        fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!(
                "codec-malformed test diverges at decode_provider_private's \
                 chained reader.finish(); end is not reached"
            );
        }
    }

    unsafe fn make_codec_malformed_custom_scan_plan(
        relation_oid: pg_sys::Oid,
    ) -> *mut pg_sys::CustomScan {
        unsafe {
            // Length-2 metadata; decode reads one cell, `finish()` rejects the rest.
            let mut provider_metadata: *mut pg_sys::List = ptr::null_mut();
            provider_metadata =
                pg_sys::lappend(provider_metadata, pg_sys::makeInteger(7).cast());
            provider_metadata =
                pg_sys::lappend(provider_metadata, pg_sys::makeInteger(8).cast());
            assert_eq!(
                (*provider_metadata).length,
                2,
                "fixture provider_metadata must have length 2"
            );

            let custom_private = encode_split(
                CodecMalformedProvider::NAME,
                relation_oid,
                0,                 // pushed_count
                0,                 // recheck_count
                &[], // pushed_contracts (len must equal pushed_count = 0)
                &[], // column_refs
                provider_metadata, // malformed cell-6 payload
                1,   // pre_setrefs_scan_rti — debug-only
            )
            .expect("encode_split must succeed for the task codec-malformed fixture");

            let cscan = pg_sys::palloc0(core::mem::size_of::<pg_sys::CustomScan>())
                as *mut pg_sys::CustomScan;
            (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
            (*cscan).scan.scanrelid = 1;
            (*cscan).custom_exprs = ptr::null_mut();
            (*cscan).custom_private = custom_private;
            cscan
        }
    }

    unsafe fn synth_begin_codec_malformed() -> (
        *mut CustomScanStateWrapper<CodecMalformedProvider>,
        *mut pg_sys::EState,
    ) {
        unsafe {
            let relation_oid = pg_sys::Oid::from(50_802u32);

            let wrapper_ptr = pg_sys::palloc0(core::mem::size_of::<
                CustomScanStateWrapper<CodecMalformedProvider>,
            >())
                as *mut CustomScanStateWrapper<CodecMalformedProvider>;
            assert!(!wrapper_ptr.is_null());
            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;

            let cscan = make_codec_malformed_custom_scan_plan(relation_oid);
            (*wrapper_ptr).base.ss.ps.plan = cscan.cast::<pg_sys::Plan>();
            (*wrapper_ptr).base.ss.ss_currentRelation =
                make_relation_stub(relation_oid);
            (*wrapper_ptr).base.ss.ps.ps_ExprContext = make_econtext_stub();

            let estate = make_begin_estate_stub();

            (wrapper_ptr, estate)
        }
    }

    /// Over-long provider_metadata fails at `reader.finish()` on the begin boundary.
    #[pg_test(
        error = "customscan \"codec-malformed-provider\" provider failed to decode custom_private payload: customscan custom_private codec error: custom_private payload has unexpected trailing cells: read 1, len 2"
    )]
    fn decode_provider_private_boundary_raises_on_malformed_codec_payload() {
        unsafe {
            let (wrapper_ptr, estate) = synth_begin_codec_malformed();
            let node: *mut pg_sys::CustomScanState =
                wrapper_ptr.cast::<pg_sys::CustomScanState>();

            begin_custom_scan_trampoline::<CodecMalformedProvider>(node, estate, 0);

            panic!(
                "begin_custom_scan_trampoline returned instead of raising \
                 ereport(ERROR) for a malformed (over-long) codec payload",
            );
        }
    }
}
