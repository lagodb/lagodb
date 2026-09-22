//! ExecScan delegation, provider row production, and EPQ recheck.

use crate::customscan::error::{CustomScanError, CustomScanPhase};
use crate::customscan::execution::state::CustomScanStateWrapper;
use crate::customscan::provider::{LagodbCustomScanProvider, NextSlotContext};
use crate::handles::{RelationHandle, ScanDirection};
use pgrx::{pg_guard, pg_sys};

/// `ExecCustomScan`: delegate to PostgreSQL's `ExecScan` with framework
/// access and exact planned-filter recheck callbacks.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_custom_scan_trampoline<
    P: LagodbCustomScanProvider,
>(
    node: *mut pg_sys::CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        pg_sys::ExecScan(
            &mut (*node).ss,
            Some(next_slot_wrapper::<P>),
            Some(recheck_exact_filters::<P>),
        )
    }
}

/// Access callback for `ExecScan` (`P::next_slot`).
///
/// # Safety
///
/// PostgreSQL invokes this callback with the live ScanState, relation, slot,
/// and expression context initialized by `ExecInitCustomScan`. The generic
/// provider planner emits a relation-backed CustomScan, so the relation and
/// slot are non-NULL; the relation-less ModifyTable wrapper uses its own exec
/// callback.
#[doc(hidden)]
#[pg_guard]
pub unsafe extern "C-unwind" fn next_slot_wrapper<P: LagodbCustomScanProvider>(
    scan_state: *mut pg_sys::ScanState,
) -> *mut pg_sys::TupleTableSlot {
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    match unsafe { next_slot::<P>(scan_state, prior_ctx) } {
        Ok(slot) => slot,
        Err(error) => error
            .with_callback_phase(P::NAME, CustomScanPhase::NextSlot)
            .report(),
    }
}

#[inline]
unsafe fn next_slot<P: LagodbCustomScanProvider>(
    scan_state: *mut pg_sys::ScanState,
    prior_ctx: pg_sys::MemoryContext,
) -> Result<*mut pg_sys::TupleTableSlot, CustomScanError> {
    let cscan_state = scan_state.cast::<pg_sys::CustomScanState>();
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(cscan_state) };

    let slot = wrapper.base.ss.ss_ScanTupleSlot;
    let _ = unsafe { pg_sys::ExecClearTuple(slot) };
    let scan_rel = wrapper.base.ss.ss_currentRelation;
    let econtext = wrapper.base.ss.ps.ps_ExprContext;
    let estate = wrapper.base.ss.ps.state;
    let per_tuple_ctx = unsafe { (*econtext).ecxt_per_tuple_memory };
    let scan_direction =
        ScanDirection::try_from_raw(unsafe { (*estate).es_direction })
            .map_err(CustomScanError::internal)?;

    let _ = unsafe { pg_sys::MemoryContextSwitchTo(per_tuple_ctx) };
    let provider_state_ref: &mut P::State =
        unsafe { wrapper.provider_state_mut_unchecked() };
    let ctx = NextSlotContext::<P>::new(
        provider_state_ref,
        unsafe { RelationHandle::from_raw(scan_rel) },
        slot,
        scan_direction,
        per_tuple_ctx,
    );

    let outcome = P::next_slot(ctx);
    let _ = unsafe { pg_sys::MemoryContextSwitchTo(prior_ctx) };
    let outcome = outcome?;

    if outcome.is_produced() {
        unsafe {
            (*slot).tts_tableOid = (*scan_rel).rd_id;
        }
    }

    Ok(slot)
}

/// Recheck callback for EPQ: set scantuple, reset per-tuple context, and run
/// the framework-owned exact filter recheck expression.
#[pg_guard]
unsafe extern "C-unwind" fn recheck_exact_filters<P: LagodbCustomScanProvider>(
    node: *mut pg_sys::ScanState,
    slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    let cscan_state = node.cast::<pg_sys::CustomScanState>();
    let wrapper = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(cscan_state) };
    let econtext = wrapper.base.ss.ps.ps_ExprContext;

    unsafe {
        (*econtext).ecxt_scantuple = slot;
    }

    let per_tuple_ctx = unsafe { (*econtext).ecxt_per_tuple_memory };
    unsafe { pg_sys::MemoryContextReset(per_tuple_ctx) };

    // ExecQual(NULL) is true, which is the no-recheck case.
    unsafe { pg_sys::ExecQual(wrapper.recheck_state, econtext) }
}
