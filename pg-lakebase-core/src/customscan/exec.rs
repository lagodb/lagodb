//! Executor trampolines: Begin / ReScan / Exec (`ExecScan` + `next_slot`) /
//! recheck / End. Decode `custom_private`, slice `custom_exprs`, cache pushed
//! param bitmap, resolve params, `ExecInitQual` for recheck; provider builds
//! predicates at runtime. Provider errors report via [`CustomScanError`].

use core::ffi::c_int;
use core::ptr;

use crate::customscan::codec::PrivateDataReader;
pub use crate::customscan::custom_exprs::{CustomExprSections, slice_pushed_recheck};
use crate::customscan::custom_private::{EncodedPrivate, decode_private};
use crate::customscan::error::{CustomScanError, CustomScanPhase};
use crate::customscan::exec_params::RuntimeParamRefs;
pub use crate::customscan::exec_params::collect_param_refs;
use crate::customscan::provider::{
    BeginContext, CreateStateContext, EndContext, LakebaseCustomScanProvider,
    NextSlotContext, ReScanContext,
};
use crate::customscan::state::CustomScanStateWrapper;
use crate::diag::ReportableError;
use crate::handles::{RelationHandle, SnapshotHandle};
use pgrx::pg_guard;
use pgrx::pg_sys;

#[cfg(test)]
use crate::customscan::custom_exprs::validate_custom_expr_section_counts;

/// BeginCustomScan trampoline.
#[doc(hidden)]
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
) {
    debug_assert!(!node.is_null(), "BeginCustomScan: node must be non-null");
    debug_assert!(
        !estate.is_null(),
        "BeginCustomScan: estate must be non-null"
    );

    // SAFETY: wrapper from our CreateCustomScanState; repr(C) with base first.
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };

    let prior_ctx: pg_sys::MemoryContext = unsafe { pg_sys::CurrentMemoryContext };

    let cscan = unsafe { (*node).ss.ps.plan } as *mut pg_sys::CustomScan;
    debug_assert!(
        !cscan.is_null(),
        "BeginCustomScan: ss.ps.plan must reference a CustomScan node",
    );
    let priv_payload: EncodedPrivate =
        unsafe { decode_private((*cscan).custom_private) }.report_unwrap();

    // Defense in depth: encoded name vs P::NAME.
    crate::customscan::custom_private::assert_provider_name_matches(
        priv_payload.provider_id_or_name.as_c_str(),
        P::NAME,
    )
    .report_unwrap();

    // Plan-time relation OID vs executor-opened relation.
    let scan_rel: pg_sys::Relation = unsafe { (*node).ss.ss_currentRelation };
    debug_assert!(
        !scan_rel.is_null(),
        "BeginCustomScan: ss_currentRelation must be open by ExecInitCustomScan",
    );
    let scan_relid_index = unsafe { (*cscan).scan.scanrelid };
    let scan_relid: c_int = scan_relid_index as c_int;

    // SAFETY: `scan_rel` is open and locked by PG's executor setup,
    // and `(*scan_rel).rd_id` is the `pg_class` OID.
    let opened_relid = unsafe { (*scan_rel).rd_id };
    check_scan_relation_oid(priv_payload.relation_oid, opened_relid).report_unwrap();

    // Decode provider payload before writing MaybeUninit fields.
    let provider_private =
        unsafe { decode_provider_private::<P>(priv_payload.provider_metadata_raw) }
            .report_unwrap();

    // Cache framework envelope fields that rescan needs, avoiding repeated
    // decode_private on every ReScanCustomScan invocation. Move (not clone)
    // the Vecs from the decoded payload into the cache.
    wrapper.cached_envelope = Some(crate::customscan::state::CachedEnvelope {
        pushed_count: priv_payload.pushed_count,
        recheck_count: priv_payload.recheck_count,
        pushed_contracts: priv_payload.pushed_contracts,
        column_refs: priv_payload.column_refs,
    });

    // SAFETY: uninit → write decoded private; End drops when initialized.
    unsafe {
        wrapper.decoded_private.as_mut_ptr().write(provider_private);
    }
    wrapper.decoded_private_initialized = true;

    let provider_state = P::create_state(CreateStateContext::<P>::new());
    // SAFETY: uninit → write provider state.
    unsafe {
        wrapper.provider_state.as_mut_ptr().write(provider_state);
    }
    wrapper.provider_state_initialized = true;

    // EXPLAIN_ONLY: skip cursor, recheck qual, and P::begin.
    if (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return;
    }

    let expr_sections = unsafe {
        CustomExprSections::from_custom_exprs(
            (*cscan).custom_exprs,
            priv_payload.pushed_count,
            priv_payload.recheck_count,
        )
    }
    .report_unwrap();

    let mut param_refs =
        unsafe { RuntimeParamRefs::collect_from_exprs(expr_sections.pushed()) };

    // Copy param bitmap into per-query context (survives rescans).
    let estate_query_ctx: pg_sys::MemoryContext = unsafe { (*estate).es_query_cxt };
    unsafe { param_refs.relocate_exec_param_ids_to(estate_query_ctx) };
    wrapper.cached_pushed_param_ids = param_refs.exec_param_ids();

    let econtext: *mut pg_sys::ExprContext = unsafe { (*node).ss.ps.ps_ExprContext };
    debug_assert!(
        !econtext.is_null(),
        "BeginCustomScan: ps_ExprContext must be set by ExecInitCustomScan",
    );
    let resolved_params: Vec<crate::expr::nodes::PgParamValue> =
        unsafe { param_refs.resolve(estate, econtext) }.report_unwrap();

    // Compile recheck qual BEFORE P::begin: ExecInitQual errors skip EndCustomScan,
    // so provider_began must stay false until begin succeeds.
    if priv_payload.recheck_count > 0 {
        let recheck_list = unsafe { expr_sections.recheck_list() };
        let parent: *mut pg_sys::PlanState = unsafe { &mut (*node).ss.ps };
        wrapper.recheck_state = unsafe { pg_sys::ExecInitQual(recheck_list, parent) };
    } else {
        wrapper.recheck_state = ptr::null_mut();
    }

    // SAFETY: distinct MaybeUninit fields; both initialized.
    let decoded_private_ref: &P::PrivateData =
        unsafe { &*wrapper.decoded_private.as_ptr() };
    let provider_state_ref: &mut P::State =
        unsafe { wrapper.provider_state.assume_init_mut() };
    let snapshot: pg_sys::Snapshot = unsafe { (*estate).es_snapshot };
    let per_tuple_memory_context: pg_sys::MemoryContext =
        unsafe { (*econtext).ecxt_per_tuple_memory };

    // Borrow column_refs and pushed_contracts from the cached envelope (already
    // moved there above) for the BeginContext.
    let envelope = wrapper
        .cached_envelope
        .as_ref()
        .expect("BeginCustomScan: cached_envelope must be populated above");

    let begin_ctx = BeginContext::<P>::new(
        provider_state_ref,
        decoded_private_ref,
        expr_sections.pushed(),
        &envelope.column_refs,
        &envelope.pushed_contracts,
        &resolved_params,
        scan_relid,
        unsafe { RelationHandle::from_raw(scan_rel) },
        unsafe { SnapshotHandle::from_raw(snapshot) },
        estate,
        per_tuple_memory_context,
        eflags,
        cscan,
        expr_sections.recheck(),
    );
    if let Err(err) = P::begin(begin_ctx) {
        err.with_provider_phase::<P>(CustomScanPhase::Begin)
            .report_after_switch(prior_ctx);
    }
    wrapper.provider_began = true;
}

/// ReScanCustomScan: re-resolve params when `chgParam` overlaps cached ids.
#[doc(hidden)]
#[pg_guard]
pub unsafe extern "C-unwind" fn rescan_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
) {
    debug_assert!(!node.is_null(), "ReScanCustomScan: node must be non-null");

    // SAFETY: PG passes the same wrapper allocation that
    // `CreateCustomScanState` produced; the cast is sound under
    // `#[repr(C)]` with `base` as the first field.
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };

    let prior_ctx: pg_sys::MemoryContext = unsafe { pg_sys::CurrentMemoryContext };

    if !wrapper.provider_began {
        debug_assert!(
            false,
            "ReScanCustomScan: invoked before BeginCustomScan completed \
             (provider_began == false). PG should never schedule a rescan \
             against a node whose Begin did not run.",
        );
        return;
    }

    let chg_param: *mut pg_sys::Bitmapset = unsafe { (*node).ss.ps.chgParam };
    let cached_ids: *mut pg_sys::Bitmapset = wrapper.cached_pushed_param_ids;

    let params_changed: bool = unsafe { pg_sys::bms_overlap(chg_param, cached_ids) };

    let cscan = unsafe { (*node).ss.ps.plan } as *mut pg_sys::CustomScan;
    debug_assert!(
        !cscan.is_null(),
        "ReScanCustomScan: ss.ps.plan must reference a CustomScan node",
    );

    // Use the cached envelope populated during BeginCustomScan instead of
    // re-decoding the immutable custom_private list on every rescan.
    let envelope = wrapper.cached_envelope.as_ref().expect(
        "ReScanCustomScan: cached_envelope must be populated by BeginCustomScan",
    );

    let expr_sections = unsafe {
        CustomExprSections::from_custom_exprs(
            (*cscan).custom_exprs,
            envelope.pushed_count,
            envelope.recheck_count,
        )
    }
    .report_unwrap();

    let scan_relid: c_int = unsafe { (*cscan).scan.scanrelid as c_int };
    let scan_rel: pg_sys::Relation = unsafe { (*node).ss.ss_currentRelation };
    debug_assert!(
        !scan_rel.is_null(),
        "ReScanCustomScan: ss_currentRelation must be open",
    );
    let estate: *mut pg_sys::EState = unsafe { (*node).ss.ps.state };
    debug_assert!(
        !estate.is_null(),
        "ReScanCustomScan: PlanState.state must be a live EState",
    );
    let econtext: *mut pg_sys::ExprContext = unsafe { (*node).ss.ps.ps_ExprContext };
    debug_assert!(
        !econtext.is_null(),
        "ReScanCustomScan: ps_ExprContext must be set by ExecInitCustomScan",
    );
    let snapshot: pg_sys::Snapshot = unsafe { (*estate).es_snapshot };
    let per_tuple_memory_context: pg_sys::MemoryContext =
        unsafe { (*econtext).ecxt_per_tuple_memory };

    let resolved_params: Vec<crate::expr::nodes::PgParamValue> = if params_changed {
        let mut param_refs =
            unsafe { RuntimeParamRefs::collect_from_exprs(expr_sections.pushed()) };
        unsafe { param_refs.free_exec_param_ids() };
        unsafe { param_refs.resolve(estate, econtext) }.report_unwrap()
    } else {
        Vec::new()
    };

    let provider_state_ref: &mut P::State =
        unsafe { wrapper.provider_state.assume_init_mut() };
    let rescan_ctx = ReScanContext::<P>::new(
        provider_state_ref,
        params_changed,
        expr_sections.pushed(),
        &envelope.column_refs,
        &envelope.pushed_contracts,
        &resolved_params,
        scan_relid,
        unsafe { RelationHandle::from_raw(scan_rel) },
        unsafe { SnapshotHandle::from_raw(snapshot) },
        estate,
        per_tuple_memory_context,
    );
    if let Err(err) = P::rescan(rescan_ctx) {
        err.with_provider_phase::<P>(CustomScanPhase::ReScan)
            .report_after_switch(prior_ctx);
    }
}

/// `ExecCustomScan`: delegate to PG `ExecScan` with our access/recheck callbacks.
/// PG runs plan.qual and projection; we do not auto-execute `plan.qual` ourselves.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn exec_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    debug_assert!(!node.is_null(), "ExecCustomScan: node must be non-null",);

    unsafe {
        pg_sys::ExecScan(
            &mut (*node).ss,
            Some(next_slot_wrapper::<P>),
            Some(recheck_exact_pushed_quals::<P>),
        )
    }
}

/// Access callback for `ExecScan` (`P::next_slot`).
#[doc(hidden)]
#[pg_guard]
pub unsafe extern "C-unwind" fn next_slot_wrapper<P: LakebaseCustomScanProvider>(
    scan_state: *mut pg_sys::ScanState,
) -> *mut pg_sys::TupleTableSlot {
    debug_assert!(
        !scan_state.is_null(),
        "next_slot_wrapper: scan_state must be non-null",
    );

    let cscan_state = scan_state as *mut pg_sys::CustomScanState;
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(cscan_state) };

    let slot: *mut pg_sys::TupleTableSlot = wrapper.base.ss.ss_ScanTupleSlot;
    debug_assert!(
        !slot.is_null(),
        "next_slot_wrapper: ss_ScanTupleSlot must be initialized by ExecInitCustomScan",
    );
    let _ = unsafe { pg_sys::ExecClearTuple(slot) };

    let scan_rel: pg_sys::Relation = wrapper.base.ss.ss_currentRelation;
    debug_assert!(
        !scan_rel.is_null(),
        "next_slot_wrapper: ss_currentRelation must be open",
    );
    let econtext: *mut pg_sys::ExprContext = wrapper.base.ss.ps.ps_ExprContext;
    debug_assert!(
        !econtext.is_null(),
        "next_slot_wrapper: ps_ExprContext must be set by ExecInitCustomScan",
    );
    let estate: *mut pg_sys::EState = wrapper.base.ss.ps.state;
    debug_assert!(
        !estate.is_null(),
        "next_slot_wrapper: PlanState.state must be a live EState",
    );
    // SAFETY: `econtext` is live by the assertion above.
    let per_tuple_ctx: pg_sys::MemoryContext =
        unsafe { (*econtext).ecxt_per_tuple_memory };

    let prior_ctx = unsafe { pg_sys::MemoryContextSwitchTo(per_tuple_ctx) };

    let provider_state_ref: &mut P::State =
        unsafe { wrapper.provider_state.assume_init_mut() };
    let ctx = NextSlotContext::<P>::new(
        provider_state_ref,
        unsafe { RelationHandle::from_raw(scan_rel) },
        slot,
        estate,
        econtext,
        per_tuple_ctx,
    );

    let row_produced = match P::next_slot(ctx) {
        Ok(produced) => produced,
        Err(err) => {
            err.with_provider_phase::<P>(CustomScanPhase::NextSlot)
                .report_after_switch(prior_ctx);
        }
    };
    let _ = unsafe { pg_sys::MemoryContextSwitchTo(prior_ctx) };

    let slot_empty = unsafe { is_slot_empty(slot) };
    match decide(row_produced, slot_empty) {
        SlotOutcome::Return => unsafe {
            (*slot).tts_tableOid = (*scan_rel).rd_id;
        },
        SlotOutcome::RaiseEmptyProduced => {
            CustomScanError::slot_not_filled(P::NAME)
                .with_provider_phase::<P>(CustomScanPhase::NextSlot)
                .report_after_switch(prior_ctx);
        }
        SlotOutcome::RaiseFilledEof => {
            let _ = unsafe { pg_sys::ExecClearTuple(slot) };
            CustomScanError::slot_filled_at_eof(P::NAME)
                .with_provider_phase::<P>(CustomScanPhase::NextSlot)
                .report_after_switch(prior_ctx);
        }
        SlotOutcome::Eof => {}
    }

    slot
}

/// Post-`next_slot` outcome from `(produced, slot_empty)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotOutcome {
    Return,
    Eof,
    /// `Ok(true)` but empty slot — invariant violation.
    RaiseEmptyProduced,
    /// `Ok(false)` but filled slot — invariant violation.
    RaiseFilledEof,
}

/// Maps `(produced, slot_empty)` → [`SlotOutcome`].
#[inline]
fn decide(produced: bool, slot_empty: bool) -> SlotOutcome {
    match (produced, slot_empty) {
        (true, false) => SlotOutcome::Return,
        (true, true) => SlotOutcome::RaiseEmptyProduced,
        (false, true) => SlotOutcome::Eof,
        (false, false) => SlotOutcome::RaiseFilledEof,
    }
}

/// Recheck callback for EPQ: set scantuple, reset per-tuple context, `ExecQual`.
#[pg_guard]
unsafe extern "C-unwind" fn recheck_exact_pushed_quals<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::ScanState,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    debug_assert!(
        !node.is_null(),
        "recheck_exact_pushed_quals: node must be non-null",
    );
    debug_assert!(
        !slot.is_null(),
        "recheck_exact_pushed_quals: slot must be non-null",
    );

    let cscan_state = node as *mut pg_sys::CustomScanState;
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(cscan_state) };

    let econtext: *mut pg_sys::ExprContext = wrapper.base.ss.ps.ps_ExprContext;
    debug_assert!(
        !econtext.is_null(),
        "recheck_exact_pushed_quals: ps_ExprContext must be set by ExecInitCustomScan",
    );

    unsafe {
        (*econtext).ecxt_scantuple = slot;
    }

    let per_tuple_ctx: pg_sys::MemoryContext =
        unsafe { (*econtext).ecxt_per_tuple_memory };
    if !per_tuple_ctx.is_null() {
        unsafe {
            pg_sys::MemoryContextReset(per_tuple_ctx);
        }
    }

    // ExecQual(NULL) → true (no Exact recheck quals).
    unsafe { pg_sys::ExecQual(wrapper.recheck_state, econtext) }
}

/// True when slot is NULL or empty.
#[inline]
unsafe fn is_slot_empty(slot: *mut pg_sys::TupleTableSlot) -> bool {
    if slot.is_null() {
        return true;
    }
    let flags = unsafe { (*slot).tts_flags } as u32;
    (flags & pg_sys::TTS_FLAG_EMPTY) != 0
}

/// `EndCustomScan`: `P::end` (if begin ran), drop typed payloads, free bitmap.
/// `recheck_state` is owned by PG's query context — null only, do not free.
/// Drop `provider_state` before `decoded_private`.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn end_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
) {
    debug_assert!(!node.is_null(), "EndCustomScan: node must be non-null");

    // SAFETY: PG passes the same wrapper allocation that
    // `CreateCustomScanState` produced; the cast is sound under
    // `#[repr(C)]` with `base` as the first field.
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };

    if wrapper.provider_began {
        debug_assert!(
            wrapper.provider_state_initialized,
            "EndCustomScan: provider_began implies provider_state_initialized",
        );
        let provider_state_ref: &mut P::State =
            unsafe { wrapper.provider_state.assume_init_mut() };
        let scan_rel: pg_sys::Relation = unsafe { (*node).ss.ss_currentRelation };
        // SAFETY: `node->ss.ps.state` is the plan's `EState`, set
        // by `ExecInitNode` and stable for the scan's lifetime —
        // including through End.
        let estate: *mut pg_sys::EState = unsafe { (*node).ss.ps.state };
        let end_ctx = EndContext::<P>::new(
            provider_state_ref,
            unsafe { RelationHandle::from_raw(scan_rel) },
            estate,
        );
        if let Err(err) = P::end(end_ctx) {
            // Log only — raising during End would skip framework teardown.
            pgrx::log!(
                "customscan {:?} provider.end failed during teardown: {}",
                P::NAME,
                err,
            );
        }
        wrapper.provider_began = false;
    }

    // Drop State before PrivateData (State may borrow from decoded payload).
    if wrapper.provider_state_initialized {
        unsafe {
            ptr::drop_in_place(wrapper.provider_state.as_mut_ptr());
        }
        wrapper.provider_state_initialized = false;
    }
    if wrapper.decoded_private_initialized {
        unsafe {
            ptr::drop_in_place(wrapper.decoded_private.as_mut_ptr());
        }
        wrapper.decoded_private_initialized = false;
    }

    wrapper.recheck_state = ptr::null_mut();

    // Drop the cached envelope (owned Vecs of contracts/column_refs).
    wrapper.cached_envelope = None;

    if !wrapper.cached_pushed_param_ids.is_null() {
        unsafe {
            pg_sys::bms_free(wrapper.cached_pushed_param_ids);
        }
        wrapper.cached_pushed_param_ids = ptr::null_mut();
    }
}

/// Compare plan-time vs executor-opened scan relation OID.
#[doc(hidden)]
pub fn check_scan_relation_oid(
    expected: pg_sys::Oid,
    opened: pg_sys::Oid,
) -> Result<(), CustomScanError> {
    if opened != expected {
        return Err(CustomScanError::scan_relation_oid_mismatch(
            expected.to_u32(),
            opened.to_u32(),
        ));
    }
    Ok(())
}

/// Decode provider `PrivateData`; `metadata` is NULL or from this provider's encode.
unsafe fn decode_provider_private<P: LakebaseCustomScanProvider>(
    metadata: *mut pg_sys::List,
) -> Result<P::PrivateData, CustomScanError> {
    use crate::customscan::custom_private::CustomScanPrivate;

    let mut reader = unsafe { PrivateDataReader::from_list(metadata) };

    let result = P::PrivateData::decode(&mut reader)
        .and_then(|pd| reader.finish().map(|()| pd));

    match result {
        Ok(payload) => Ok(payload),
        Err(err) => Err(CustomScanError::provider_private_decode::<P>(err)),
    }
}

#[cfg(test)]
mod tests {
    //! Unit-level tests that do NOT require a live PostgreSQL backend.
    //!
    //! End-to-end coverage of [`begin_custom_scan_trampoline`] requires
    //! `ExecInitCustomScan` plumbing and lives in `pg_test`-style
    //! regression tests. The tests here verify only the framework-side
    //! invariants that can be checked in isolation.

    use super::*;
    use std::collections::HashSet;

    use pgrx::prelude::PgSqlErrorCode;
    use proptest::prelude::*;

    use crate::customscan::provider::{CreateStateContext, CustomScanError};
    use crate::customscan::test_support::{NoopProvider, NoopProviderSpec};
    use crate::diag::{SqlStateError, error_source_chain_detail};

    use crate::customscan::custom_private::DecodeError;

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

    /// `CreateStateContext::<P>::new()` returns a value of the right
    /// type without panicking. The constructor initializes every field
    /// explicitly rather than relying on a zeroed bit pattern.
    #[test]
    fn create_state_context_new_compiles() {
        let _ctx: CreateStateContext<DummyProvider> =
            CreateStateContext::<DummyProvider>::new();
    }

    /// Pure model: PARAM_EXEC ids that enter the rescan bitmap.
    fn exec_ids_for_bitmap(refs: &[(pg_sys::ParamKind::Type, c_int)]) -> Vec<c_int> {
        refs.iter()
            .filter(|(kind, _)| *kind == pg_sys::ParamKind::PARAM_EXEC)
            .map(|(_, id)| *id)
            .collect()
    }

    /// De-duplicated EXEC id set (bitmap model).
    fn exec_id_set(refs: &[(pg_sys::ParamKind::Type, c_int)]) -> HashSet<c_int> {
        exec_ids_for_bitmap(refs).into_iter().collect()
    }

    /// Pure model: rescan re-translate when chgParam ∩ exec_ids is non-empty.
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

            let verdict = params_changed(&chgparam, &exec_ids);
            prop_assert_eq!(verdict, !chgparam.is_disjoint(&exec_ids));

            let extern_ids: HashSet<c_int> = refs
                .iter()
                .filter(|(kind, _)| *kind == pg_sys::ParamKind::PARAM_EXTERN)
                .map(|(_, id)| *id)
                .collect();
            for &e in extern_ids.difference(&exec_ids) {
                prop_assert!(!exec_ids.contains(&e));

                let mut without_e = chgparam.clone();
                without_e.remove(&e);
                let mut with_e = without_e.clone();
                with_e.insert(e);

                prop_assert_eq!(
                    params_changed(&with_e, &exec_ids),
                    params_changed(&without_e, &exec_ids),
                );
            }
        }
    }

    #[test]
    fn decide_covers_all_four_combinations() {
        assert_eq!(decide(true, false), SlotOutcome::Return);
        assert_eq!(decide(true, true), SlotOutcome::RaiseEmptyProduced);
        assert_eq!(decide(false, false), SlotOutcome::RaiseFilledEof);
        assert_eq!(decide(false, true), SlotOutcome::Eof);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn decide_never_truncates(
            produced in any::<bool>(),
            slot_empty in any::<bool>(),
        ) {
            let outcome = decide(produced, slot_empty);
            match (produced, slot_empty) {
                (true, false) => prop_assert_eq!(outcome, SlotOutcome::Return),
                (true, true) => {
                    prop_assert_eq!(outcome, SlotOutcome::RaiseEmptyProduced)
                }
                (false, false) => {
                    prop_assert_eq!(outcome, SlotOutcome::RaiseFilledEof)
                }
                (false, true) => prop_assert_eq!(outcome, SlotOutcome::Eof),
            }

            if produced {
                prop_assert_ne!(outcome, SlotOutcome::Eof);
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
        let result = check_scan_relation_oid(expected, opened);

        let err = result.unwrap_err();
        assert_eq!(err.sql_error_code(), PgSqlErrorCode::ERRCODE_INTERNAL_ERROR);
        assert!(
            err.to_string().contains("relation_oid=50500")
                && err.to_string().contains("rd_id=50501")
        );
        assert_eq!(
            format!("{err}"),
            "customscan BeginCustomScan: scan relation OID mismatch \
             (custom_private.relation_oid=50500, ss_currentRelation->rd_id=50501)"
        );
    }

    #[test]
    fn custom_expr_section_counts_null_branch_returns_err() {
        let result = validate_custom_expr_section_counts(None, 1, 0);
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("pushed_count=1")
                && err.to_string().contains("recheck_count=0")
        );
        assert_eq!(
            format!("{err}"),
            "customscan BeginCustomScan: custom_exprs is NULL but \
             pushed_count=1 recheck_count=0"
        );
    }

    #[test]
    fn custom_expr_section_counts_zero_counts_returns_zero() {
        let result = validate_custom_expr_section_counts(None, 0, 0);
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn provider_private_decode_wraps_decode_error_with_provider_name() {
        let report = CustomScanError::provider_private_decode::<DummyProvider>(
            CustomScanError::private_codec(DecodeError::NullPayload),
        );
        assert!(
            report.to_string().contains("exec-test-dummy"),
            "message: {}",
            report
        );
        let detail = error_source_chain_detail(&report);
        assert!(
            detail.is_some_and(|d| d.contains("custom_private payload is NULL")),
            "detail: {:?}",
            error_source_chain_detail(&report)
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn custom_expr_section_counts_null_branch_property(
            pushed_count in 0usize..=10_000,
            recheck_count in 0usize..=10_000,
        ) {
            let result =
                validate_custom_expr_section_counts(None, pushed_count, recheck_count);

            if pushed_count + recheck_count > 0 {
                let err = result.unwrap_err();
                let text = err.to_string();
                prop_assert!(
                    text.contains(&format!("pushed_count={pushed_count}"))
                        && text.contains(&format!("recheck_count={recheck_count}")),
                    "got {text}"
                );
            } else {
                match result {
                    Ok(total) => prop_assert_eq!(total, 0),
                    other => prop_assert!(
                        false,
                        "expected Ok(0), got {:?}",
                        other
                    ),
                }
            }
        }
    }
}
