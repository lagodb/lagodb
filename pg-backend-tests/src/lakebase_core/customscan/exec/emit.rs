//! Backend tests for `RelationHandle` accessors and the emit paths
//! (`emit_row` row-world, `emit_columns` columnar) memory-context behavior.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use core::ffi::c_void;

    use pg_lakebase_core::api::AmResult;
    use pg_lakebase_core::batch::ScanBatchDriver;
    use pg_lakebase_core::customscan::codec::{PrivateDataReader, PrivateDataWriter};
    use pg_lakebase_core::customscan::custom_private::CustomScanPrivate;
    use pg_lakebase_core::customscan::exec::next_slot_wrapper;
    use pg_lakebase_core::customscan::provider::{
        BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
        CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
        PathVariant, PlanTranslateContext, ReScanContext, RelPathContext,
    };
    use pg_lakebase_core::customscan::state::CustomScanStateWrapper;
    use pg_lakebase_core::expr::split::QualPushdownDecision;
    use pg_lakebase_core::handles::RelationHandle;
    use pg_lakebase_core::tuple::{Cell, Row, SlotColumns};
    use pgrx::pg_sys;
    use pgrx::pg_test;
    use pgrx::{FromDatum, IntoDatum};

    /// Text varlena payload for the emit-path memory-context test.
    const HANDLE_EMIT_TEXT: &str =
        "lakebase-emit-row-varlena-lands-in-the-per-tuple-context";

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

    /// Synthetic CustomScan wrapper with a per-tuple ctx distinct from the
    /// slot's `tts_mcxt`, for driving `next_slot_wrapper` directly. Generic over
    /// the provider so both the `emit_row` and `emit_columns` paths share one setup.
    struct ScanFixture<P: LakebaseCustomScanProvider> {
        wrapper_ptr: *mut CustomScanStateWrapper<P>,
        scan_state: *mut pg_sys::ScanState,
        slot: *mut pg_sys::TupleTableSlot,
        per_tuple_ctx: pg_sys::MemoryContext,
        tupdesc: pg_sys::TupleDesc,
        rel_oid: pg_sys::Oid,
    }

    /// Build a [`ScanFixture`] holding `state` as the provider's per-scan state.
    unsafe fn synth_scan_wrapper<P: LakebaseCustomScanProvider>(
        state: P::State,
    ) -> ScanFixture<P> {
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
                CustomScanStateWrapper<P>,
            >()) as *mut CustomScanStateWrapper<P>;
            assert!(!wrapper_ptr.is_null());
            (*wrapper_ptr).base.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;

            // SAFETY: mirrors BeginCustomScan — `ptr::write` before next_slot reads state.
            (*wrapper_ptr).provider_state.as_mut_ptr().write(state);
            (*wrapper_ptr).provider_state_initialized = true;
            (*wrapper_ptr).provider_began = true;

            (*wrapper_ptr).base.ss.ss_ScanTupleSlot = slot;
            (*wrapper_ptr).base.ss.ss_currentRelation = rel;
            (*wrapper_ptr).base.ss.ps.state = estate;
            (*wrapper_ptr).base.ss.ps.ps_ExprContext = econtext;

            let scan_state: *mut pg_sys::ScanState =
                core::ptr::addr_of_mut!((*wrapper_ptr).base.ss);

            ScanFixture {
                wrapper_ptr,
                scan_state,
                slot,
                per_tuple_ctx,
                tupdesc,
                rel_oid,
            }
        }
    }

    /// Build a [`ScanFixture`] for the `emit_row` (row-world) test.
    unsafe fn synth_handle_accessor_wrapper() -> ScanFixture<HandleAccessorProvider> {
        unsafe {
            synth_scan_wrapper::<HandleAccessorProvider>(HandleAccessorState {
                seen_natts: 0,
                seen_oid: pg_sys::Oid::INVALID,
                emitted: false,
            })
        }
    }

    /// `emit_row` materializes slot datums in the scan node's per-tuple memory
    /// context (reclaimed by `ExecScan`'s per-cycle `ResetExprContext`), not the
    /// slot's per-query `tts_mcxt`. This mirrors PG's own `ForeignNext`, which
    /// runs `IterateForeignScan` inside `ecxt_per_tuple_memory`. Driving the real
    /// `next_slot_wrapper` over many rows then proves `tts_mcxt` does not grow
    /// per row — the regression guard for the columnar-scan varlena leak.
    #[pg_test]
    fn emit_row_targets_per_tuple_context_and_does_not_grow_tts_mcxt() {
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

            // Read the value back before any reset, so the context assertion
            // below is about a real, live value.
            let value = String::from_datum(text_datum, false)
                .expect("the emitted text datum must be readable");
            assert_eq!(value, HANDLE_EMIT_TEXT);

            let tts_mcxt = (*fx.slot).tts_mcxt;
            assert_ne!(
                tts_mcxt, fx.per_tuple_ctx,
                "the slot's tts_mcxt must differ from the per-tuple context \
                 for this test to distinguish the two",
            );

            // The varlena must live in the per-tuple context, NOT the slot's
            // per-query tts_mcxt. The old behavior allocated into tts_mcxt and
            // leaked one varlena per scanned row for the lifetime of the query.
            let chunk_ctx =
                pg_sys::GetMemoryChunkContext(text_datum.cast_mut_ptr::<c_void>());
            assert_eq!(
                chunk_ctx, fx.per_tuple_ctx,
                "emit_row must allocate the varlena in the scan node's \
                 per-tuple context (mirroring ForeignNext), so ExecScan's \
                 per-cycle reset reclaims it",
            );
            assert_ne!(
                chunk_ctx, tts_mcxt,
                "the emitted varlena must NOT live in the slot's per-query \
                 tts_mcxt — that is the leak this fix removes",
            );

            // Drive many rows the way ExecScan does — fetch, then reset the
            // per-tuple context each cycle — and prove tts_mcxt stays flat
            // instead of accumulating one varlena per row.
            pg_sys::MemoryContextReset(fx.per_tuple_ctx);
            let baseline = pg_sys::MemoryContextMemAllocated(tts_mcxt, true);

            const ROWS: usize = 512;
            for _ in 0..ROWS {
                let slot = next_slot_wrapper::<HandleAccessorProvider>(fx.scan_state);
                assert_eq!(slot, fx.slot, "each fetch must return the scan slot");
                // ExecScan resets the per-tuple context at the start of the
                // next cycle, after the consumer has read the prior row.
                pg_sys::MemoryContextReset(fx.per_tuple_ctx);
            }

            let after = pg_sys::MemoryContextMemAllocated(tts_mcxt, true);
            assert_eq!(
                after,
                baseline,
                "tts_mcxt grew by {} bytes across {ROWS} rows; emit must not \
                 allocate into the slot's per-query context per row",
                after - baseline,
            );
        }
    }

    /// Text payload for the `emit_columns` (columnar slot-first) memory-context test.
    const EMIT_COLUMNS_TEXT: &str =
        "lakebase-emit-columns-varlena-lands-in-the-per-tuple-context";

    /// Per-scan state for [`EmitColumnsProvider`].
    struct EmitColumnsState {
        emitted: bool,
    }

    /// A `ScanBatchDriver` that writes one `(int4, text)` row per fetch, always
    /// producing a row so the multi-row growth loop keeps emitting. The text
    /// varlena is palloc'd in whatever context `emit_columns` made current, so it
    /// witnesses the per-tuple-vs-`tts_mcxt` decision for the columnar path.
    struct EmitColumnsDriver;

    impl ScanBatchDriver for EmitColumnsDriver {
        fn next_into_slot(&mut self, out: &mut SlotColumns<'_>) -> AmResult<bool> {
            out.set_datum(0, Some(pg_sys::Datum::from(7usize)));
            let text = EMIT_COLUMNS_TEXT.into_datum().expect("text datum");
            out.set_datum(1, Some(text));
            Ok(true)
        }
    }

    /// Provider whose `next_slot` drives the slot-first [`emit_columns`] path —
    /// the one the Iceberg CustomScan actually uses — so the columnar emit is
    /// covered alongside the row-world `emit_row` test above.
    struct EmitColumnsProvider;

    impl LakebaseCustomScanProvider for EmitColumnsProvider {
        const NAME: &'static core::ffi::CStr = c"emit-columns-test-provider";
        type PrivateData = HandleAccessorPrivate;
        type State = EmitColumnsState;

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
            EmitColumnsState { emitted: false }
        }

        fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
            unreachable!("this test drives next_slot directly; begin is not invoked");
        }

        fn next_slot(
            mut ctx: NextSlotContext<'_, Self>,
        ) -> Result<bool, CustomScanError> {
            let natts = ctx.relation.natts();
            let produced = ctx.emit_columns(&mut EmitColumnsDriver, natts)?;
            ctx.state.emitted = produced;
            Ok(produced)
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

    /// The columnar `emit_columns` path must target the per-tuple context (not
    /// the slot's per-query `tts_mcxt`) exactly like `emit_row`. The Iceberg
    /// CustomScan only ever uses `emit_columns`, so this guards the path that the
    /// row-world test above does not exercise.
    #[pg_test]
    fn emit_columns_targets_per_tuple_context_and_does_not_grow_tts_mcxt() {
        unsafe {
            let fx = synth_scan_wrapper::<EmitColumnsProvider>(EmitColumnsState {
                emitted: false,
            });

            let returned = next_slot_wrapper::<EmitColumnsProvider>(fx.scan_state);
            assert_eq!(
                returned, fx.slot,
                "next_slot_wrapper must return the filled scan slot on Ok(true)",
            );
            assert!(
                (*fx.wrapper_ptr).provider_state.assume_init_ref().emitted,
                "the provider's next_slot must have run emit_columns",
            );

            let text_datum: pg_sys::Datum = *(*fx.slot).tts_values.add(1);
            assert!(
                !*(*fx.slot).tts_isnull.add(1),
                "the emitted text column must be non-NULL",
            );
            let value = String::from_datum(text_datum, false)
                .expect("the emitted text datum must be readable");
            assert_eq!(value, EMIT_COLUMNS_TEXT);

            let tts_mcxt = (*fx.slot).tts_mcxt;
            assert_ne!(
                tts_mcxt, fx.per_tuple_ctx,
                "the slot's tts_mcxt must differ from the per-tuple context \
                 for this test to distinguish the two",
            );

            // The decoded varlena must live in the per-tuple context, not the
            // slot's per-query tts_mcxt.
            let chunk_ctx =
                pg_sys::GetMemoryChunkContext(text_datum.cast_mut_ptr::<c_void>());
            assert_eq!(
                chunk_ctx, fx.per_tuple_ctx,
                "emit_columns must allocate the varlena in the scan node's \
                 per-tuple context",
            );
            assert_ne!(
                chunk_ctx, tts_mcxt,
                "the emitted varlena must NOT live in the slot's per-query \
                 tts_mcxt",
            );

            // Drive many rows the ExecScan way and prove tts_mcxt stays flat.
            pg_sys::MemoryContextReset(fx.per_tuple_ctx);
            let baseline = pg_sys::MemoryContextMemAllocated(tts_mcxt, true);

            const ROWS: usize = 512;
            for _ in 0..ROWS {
                let slot = next_slot_wrapper::<EmitColumnsProvider>(fx.scan_state);
                assert_eq!(slot, fx.slot, "each fetch must return the scan slot");
                pg_sys::MemoryContextReset(fx.per_tuple_ctx);
            }

            let after = pg_sys::MemoryContextMemAllocated(tts_mcxt, true);
            assert_eq!(
                after,
                baseline,
                "tts_mcxt grew by {} bytes across {ROWS} rows; emit_columns must \
                 not allocate into the slot's per-query context per row",
                after - baseline,
            );
        }
    }
}
