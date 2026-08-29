//! Begin, ReScan, and End CustomScan lifecycle callbacks.

use core::ffi::c_int;
use core::ptr;

use crate::customscan::ScanPurpose;
use crate::customscan::error::{CustomScanError, CustomScanPhase};
use crate::customscan::execution::state::{CachedEnvelope, CustomScanStateWrapper};
use crate::customscan::filter::CustomScanFilters;
use crate::customscan::plan_data::custom_exprs::CustomExprSections;
use crate::customscan::plan_data::custom_private::{
    EncodedPrivate, assert_provider_name_matches, decode_private,
};
use crate::customscan::provider::{
    BeginContext, CreateStateContext, CustomScanPrivate, EndContext,
    LagodbCustomScanProvider, PrivateDataReader, ReScanContext, method_tables_for,
};
use crate::diag::report_warning;
use crate::handles::{RelationHandle, SnapshotHandle};
use pgrx::{pg_guard, pg_sys};

/// Return the semantic purpose when `plan` is this provider's base CustomScan.
///
/// # Safety
///
/// `plan` must be NULL or a live planner/executor-owned plan node.
pub unsafe fn provider_scan_purpose<P: LagodbCustomScanProvider>(
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
    P: LagodbCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
) {
    if let Err(error) = unsafe { begin_custom_scan::<P>(node, estate, eflags) } {
        error
            .with_callback_phase(P::NAME, CustomScanPhase::Begin)
            .report();
    }
}

unsafe fn begin_custom_scan<P: LagodbCustomScanProvider>(
    node: *mut pg_sys::CustomScanState,
    estate: *mut pg_sys::EState,
    eflags: c_int,
) -> Result<(), CustomScanError> {
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };
    let cscan = unsafe { (*node).ss.ps.plan }.cast::<pg_sys::CustomScan>();
    let priv_payload: EncodedPrivate =
        unsafe { decode_private((*cscan).custom_private) }?;

    assert_provider_name_matches(
        priv_payload.provider_id_or_name.as_c_str(),
        P::NAME,
    )?;

    let scan_rel: pg_sys::Relation = unsafe { (*node).ss.ss_currentRelation };
    let opened_relid = unsafe { (*scan_rel).rd_id };
    check_scan_relation_oid(priv_payload.relation_oid, opened_relid)?;

    let provider_private = unsafe {
        PrivateDataReader::decode_list(
            priv_payload.provider_metadata_raw,
            P::PrivateData::decode,
        )
    }?;

    let scan_slot = unsafe { (*node).ss.ss_ScanTupleSlot };
    unsafe {
        priv_payload
            .tuple_layout
            .validate_executor(cscan, scan_slot)
    }?;
    let scan_tuple_desc = unsafe { (*scan_slot).tts_tupleDescriptor };

    wrapper.decoded_private = Some(provider_private);

    let provider_state = P::create_state(CreateStateContext::<P>::new());
    wrapper.provider_state = Some(provider_state);

    if (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(());
    }

    let expr_sections = unsafe {
        CustomExprSections::from_custom_exprs(
            (*cscan).custom_exprs,
            priv_payload.binding_count,
            priv_payload.planned_filter_count,
        )
    }?;
    wrapper.expr_sections = Some(expr_sections);
    let expr_sections = wrapper
        .expr_sections
        .as_ref()
        .expect("BeginCustomScan: expression sections were just cached");

    let econtext = unsafe { (*node).ss.ps.ps_ExprContext };

    let parent = unsafe { &mut (*node).ss.ps } as *mut pg_sys::PlanState;
    let binding_exprs = unsafe { expr_sections.binding_list() };
    let mut filters = unsafe {
        CustomScanFilters::<P>::initialize(&priv_payload, binding_exprs, parent)
    }?;
    unsafe { filters.bind_initial(econtext) }?;
    let recheck_list = unsafe { filters.recheck_list(expr_sections.pushed()) };
    wrapper.recheck_state = if recheck_list.is_null() {
        ptr::null_mut()
    } else {
        unsafe { pg_sys::ExecInitQual(recheck_list, parent) }
    };
    wrapper.filters = Some(filters);

    wrapper.cached_envelope = Some(CachedEnvelope {
        purpose: priv_payload.purpose,
        tuple_layout: priv_payload.tuple_layout,
    });

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
    let filters = wrapper
        .filters
        .as_ref()
        .expect("BeginCustomScan: filters must be initialized above")
        .bound();

    let begin_ctx = BeginContext::<P>::new(
        provider_state_ref,
        decoded_private_ref,
        envelope.purpose,
        filters,
        scan_tuple_desc,
        &envelope.tuple_layout,
        unsafe { RelationHandle::from_raw(scan_rel) },
        unsafe { SnapshotHandle::from_raw(snapshot) },
    );
    P::begin(begin_ctx)?;
    wrapper.provider_began = true;
    Ok(())
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
    P: LagodbCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
) {
    if let Err(error) = unsafe { rescan_custom_scan::<P>(node) } {
        error
            .with_callback_phase(P::NAME, CustomScanPhase::ReScan)
            .report();
    }
}

unsafe fn rescan_custom_scan<P: LagodbCustomScanProvider>(
    node: *mut pg_sys::CustomScanState,
) -> Result<(), CustomScanError> {
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(node) };

    if !wrapper.provider_began {
        debug_assert!(
            false,
            "ReScanCustomScan invoked before BeginCustomScan completed",
        );
        return Ok(());
    }

    let chg_param = unsafe { (*node).ss.ps.chgParam };
    let envelope = wrapper.cached_envelope.as_ref().expect(
        "ReScanCustomScan: cached_envelope must be populated by BeginCustomScan",
    );
    let scan_rel = unsafe { (*node).ss.ss_currentRelation };
    let estate = unsafe { (*node).ss.ps.state };
    let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
    let snapshot = unsafe { (*estate).es_snapshot };

    let filters = wrapper
        .filters
        .as_mut()
        .expect("ReScanCustomScan: filters must be initialized by BeginCustomScan");
    let filters_changed = unsafe { filters.filters_changed(chg_param) };
    if filters_changed {
        unsafe { filters.rebind_dynamic(econtext) }?;
    }

    let provider_state_ref: &mut P::State =
        unsafe { wrapper.provider_state.as_mut().unwrap_unchecked() };
    let rescan_ctx = ReScanContext::<P>::new(
        provider_state_ref,
        filters_changed,
        envelope.purpose,
        filters.bound(),
        unsafe { RelationHandle::from_raw(scan_rel) },
        unsafe { SnapshotHandle::from_raw(snapshot) },
    );
    P::rescan(rescan_ctx)?;
    Ok(())
}

/// `EndCustomScan`: close the provider and drop framework-owned state.
#[pg_guard]
pub unsafe extern "C-unwind" fn end_custom_scan_trampoline<
    P: LagodbCustomScanProvider,
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
    wrapper.filters = None;
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
