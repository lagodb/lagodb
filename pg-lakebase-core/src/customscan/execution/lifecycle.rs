//! Begin, ReScan, and End CustomScan lifecycle callbacks.

use core::ffi::c_int;
use core::ptr;

use crate::customscan::ScanPurpose;
use crate::customscan::error::{CustomScanError, CustomScanPhase};
use crate::customscan::execution::exec_params::RuntimeParamRefs;
use crate::customscan::execution::state::{CachedEnvelope, CustomScanStateWrapper};
use crate::customscan::plan_data::custom_exprs::CustomExprSections;
use crate::customscan::plan_data::custom_private::{
    EncodedPrivate, assert_provider_name_matches, decode_private,
};
use crate::customscan::provider::{
    BeginContext, CreateStateContext, CustomScanPrivate, EndContext,
    LakebaseCustomScanProvider, PrivateDataReader, PushedPredicates, ReScanContext,
    method_tables_for,
};
use crate::diag::{ReportableError, report_warning};
use crate::expr::execution::params::ResolvedParam;
use crate::handles::{RelationHandle, SnapshotHandle};
use pgrx::{pg_guard, pg_sys};

/// Return the semantic purpose when `plan` is this provider's base CustomScan.
///
/// # Safety
///
/// `plan` must be NULL or a live planner/executor-owned plan node.
pub unsafe fn provider_scan_purpose<P: LakebaseCustomScanProvider>(
    plan: *mut pg_sys::Plan,
) -> Result<Option<ScanPurpose>, CustomScanError> {
    if plan.is_null() || unsafe { (*plan).type_ } != pg_sys::NodeTag::T_CustomScan {
        return Ok(None);
    }
    let scan = plan.cast::<pg_sys::CustomScan>();
    if unsafe { (*scan).methods } != method_tables_for::<P>().scan() {
        return Ok(None);
    }
    let private = unsafe { decode_private((*scan).custom_private) }?;
    assert_provider_name_matches(private.provider_id_or_name.as_c_str(), P::NAME)?;
    Ok(Some(private.purpose))
}

/// BeginCustomScan trampoline.
///
/// # Safety
///
/// PostgreSQL calls this callback after `ExecInitCustomScan` has initialized
/// the CustomScan plan state, relation, expression context, and scan slot.
/// The generic provider planner emits only relation-backed CustomScans, so
/// `scanrelid` and `ss_currentRelation` are non-zero/non-NULL here. The
/// relation-less `scanrelid = 0` form belongs to the separate ModifyTable
/// wrapper and does not use this trampoline.
#[doc(hidden)]
#[pg_guard]
pub unsafe extern "C-unwind" fn begin_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
) {
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let cscan = unsafe { (*node).ss.ps.plan }.cast::<pg_sys::CustomScan>();
    let priv_payload: EncodedPrivate =
        unsafe { decode_private((*cscan).custom_private) }.report_unwrap();

    assert_provider_name_matches(
        priv_payload.provider_id_or_name.as_c_str(),
        P::NAME,
    )
    .report_unwrap();

    let scan_rel: pg_sys::Relation = unsafe { (*node).ss.ss_currentRelation };
    let scan_relid = unsafe { (*cscan).scan.scanrelid as c_int };
    let opened_relid = unsafe { (*scan_rel).rd_id };
    check_scan_relation_oid(priv_payload.relation_oid, opened_relid).report_unwrap();

    let provider_private =
        unsafe { decode_provider_private::<P>(priv_payload.provider_metadata_raw) }
            .report_unwrap();

    let scan_slot = unsafe { (*node).ss.ss_ScanTupleSlot };
    unsafe {
        priv_payload
            .tuple_layout
            .validate_executor(cscan, scan_slot)
    }
    .report_unwrap();
    let scan_tuple_desc = unsafe { (*scan_slot).tts_tupleDescriptor };

    wrapper.cached_envelope = Some(CachedEnvelope {
        purpose: priv_payload.purpose,
        pushed_contracts: priv_payload.pushed_contracts,
        column_refs: priv_payload.column_refs,
        tuple_layout: priv_payload.tuple_layout,
    });

    wrapper.decoded_private = Some(provider_private);

    let provider_state = P::create_state(CreateStateContext::<P>::new());
    wrapper.provider_state = Some(provider_state);

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
    wrapper.expr_sections = Some(expr_sections);
    let expr_sections = wrapper
        .expr_sections
        .as_ref()
        .expect("BeginCustomScan: expression sections were just cached");

    let mut param_refs =
        unsafe { RuntimeParamRefs::collect_from_exprs(expr_sections.pushed()) };
    let estate_query_ctx = unsafe { (*estate).es_query_cxt };
    unsafe { param_refs.relocate_exec_param_ids_to(estate_query_ctx) };

    let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
    let resolved_params: Vec<ResolvedParam> =
        match unsafe { param_refs.resolve(estate, econtext) } {
            Ok(params) => params,
            Err(err) => {
                CustomScanError::runtime_parameter(err).report_after_switch(prior_ctx)
            }
        };
    wrapper.runtime_params = Some(param_refs);

    if priv_payload.recheck_count > 0 {
        let recheck_list = unsafe { expr_sections.recheck_list() };
        let parent = unsafe { &mut (*node).ss.ps };
        wrapper.recheck_state = unsafe { pg_sys::ExecInitQual(recheck_list, parent) };
    } else {
        wrapper.recheck_state = ptr::null_mut();
    }

    // These fields are initialized above and are disjoint, so borrow them
    // directly instead of borrowing the whole wrapper through accessors.
    let decoded_private_ref: &P::PrivateData =
        unsafe { wrapper.decoded_private.as_ref().unwrap_unchecked() };
    let provider_state_ref: &mut P::State =
        unsafe { wrapper.provider_state.as_mut().unwrap_unchecked() };
    let snapshot = unsafe { (*estate).es_snapshot };
    let envelope = wrapper
        .cached_envelope
        .as_ref()
        .expect("BeginCustomScan: cached_envelope must be populated above");
    let pushed_predicates = PushedPredicates::new(
        expr_sections.pushed(),
        &envelope.column_refs,
        &envelope.pushed_contracts,
        &resolved_params,
        scan_relid,
        &envelope.tuple_layout,
    );

    let begin_ctx = BeginContext::<P>::new(
        provider_state_ref,
        decoded_private_ref,
        envelope.purpose,
        pushed_predicates,
        scan_tuple_desc,
        unsafe { RelationHandle::from_raw(scan_rel) },
        unsafe { SnapshotHandle::from_raw(snapshot) },
    );
    if let Err(err) = P::begin(begin_ctx) {
        err.with_provider_phase(P::NAME, CustomScanPhase::Begin)
            .report_after_switch(prior_ctx);
    }
    wrapper.provider_began = true;
}

/// ReScanCustomScan: re-resolve params when `chgParam` overlaps cached ids.
///
/// # Safety
///
/// PostgreSQL calls this callback for the initialized CustomScan state whose
/// plan, relation, `EState`, and expression context remain live. The state was
/// emitted by the generic relation-backed provider planner, so its relation is
/// still open; the relation-less ModifyTable wrapper uses a separate callback.
#[doc(hidden)]
#[pg_guard]
pub unsafe extern "C-unwind" fn rescan_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
) {
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };

    if !wrapper.provider_began {
        debug_assert!(
            false,
            "ReScanCustomScan invoked before BeginCustomScan completed",
        );
        return;
    }

    let chg_param = unsafe { (*node).ss.ps.chgParam };
    let runtime_params = wrapper.runtime_params.as_ref().expect(
        "ReScanCustomScan: runtime params must be collected by BeginCustomScan",
    );
    let params_changed = unsafe { runtime_params.changed(chg_param) };
    let cscan = unsafe { (*node).ss.ps.plan }.cast::<pg_sys::CustomScan>();
    let envelope = wrapper.cached_envelope.as_ref().expect(
        "ReScanCustomScan: cached_envelope must be populated by BeginCustomScan",
    );
    let expr_sections = wrapper.expr_sections.as_ref().expect(
        "ReScanCustomScan: expression sections must be cached by BeginCustomScan",
    );
    let scan_relid = unsafe { (*cscan).scan.scanrelid as c_int };
    let scan_rel = unsafe { (*node).ss.ss_currentRelation };
    let estate = unsafe { (*node).ss.ps.state };
    let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
    let snapshot = unsafe { (*estate).es_snapshot };
    let resolved_params = if params_changed {
        match unsafe { runtime_params.resolve(estate, econtext) } {
            Ok(params) => params,
            Err(err) => {
                CustomScanError::runtime_parameter(err).report_after_switch(prior_ctx)
            }
        }
    } else {
        Vec::new()
    };

    let provider_state_ref: &mut P::State =
        unsafe { wrapper.provider_state.as_mut().unwrap_unchecked() };
    let pushed_predicates = PushedPredicates::new(
        expr_sections.pushed(),
        &envelope.column_refs,
        &envelope.pushed_contracts,
        &resolved_params,
        scan_relid,
        &envelope.tuple_layout,
    );
    let rescan_ctx = ReScanContext::<P>::new(
        provider_state_ref,
        params_changed,
        envelope.purpose,
        pushed_predicates,
        unsafe { RelationHandle::from_raw(scan_rel) },
        unsafe { SnapshotHandle::from_raw(snapshot) },
    );
    if let Err(err) = P::rescan(rescan_ctx) {
        err.with_provider_phase(P::NAME, CustomScanPhase::ReScan)
            .report_after_switch(prior_ctx);
    }
}

/// `EndCustomScan`: close the provider and drop framework-owned state.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
) {
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };

    if wrapper.provider_began {
        let provider_state_ref: &mut P::State =
            unsafe { wrapper.provider_state_mut_unchecked() };
        let scan_rel = unsafe { (*node).ss.ss_currentRelation };
        let end_ctx = EndContext::<P>::new(provider_state_ref, unsafe {
            RelationHandle::from_raw(scan_rel)
        });
        if let Err(err) = P::end(end_ctx) {
            report_warning(format_args!(
                "customscan {:?} provider.end failed during teardown: {}",
                P::NAME,
                err,
            ));
        }
        wrapper.provider_began = false;
    }

    let _ = wrapper.provider_state.take();
    let _ = wrapper.decoded_private.take();

    wrapper.recheck_state = ptr::null_mut();
    wrapper.cached_envelope = None;
    wrapper.expr_sections = None;
    wrapper.runtime_params = None;
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

unsafe fn decode_provider_private<P: LakebaseCustomScanProvider>(
    metadata: *mut pg_sys::List,
) -> Result<P::PrivateData, CustomScanError> {
    let mut reader = unsafe { PrivateDataReader::from_list(metadata) };
    let result = P::PrivateData::decode(&mut reader)
        .and_then(|private| reader.finish().map(|()| private));
    match result {
        Ok(payload) => Ok(payload),
        Err(error) => Err(CustomScanError::provider_private_decode(P::NAME, error)),
    }
}
