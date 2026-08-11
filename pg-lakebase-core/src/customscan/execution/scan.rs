//! ExecScan delegation, provider row production, and EPQ recheck.

use crate::customscan::error::{CustomScanError, CustomScanPhase};
use crate::customscan::execution::state::CustomScanStateWrapper;
use crate::customscan::provider::{LakebaseCustomScanProvider, NextSlotContext};
use crate::handles::{RelationHandle, ScanDirection};
use pgrx::{pg_guard, pg_sys};

/// `ExecCustomScan`: delegate to PostgreSQL's `ExecScan` with framework
/// access and exact planned-filter recheck callbacks.
#[pg_guard]
pub unsafe extern "C-unwind" fn exec_custom_scan_trampoline<
    P: LakebaseCustomScanProvider,
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
pub unsafe extern "C-unwind" fn next_slot_wrapper<P: LakebaseCustomScanProvider>(
    scan_state: *mut pg_sys::ScanState,
) -> *mut pg_sys::TupleTableSlot {
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    match unsafe { next_slot::<P>(scan_state, prior_ctx) } {
        Ok(slot) => slot,
        Err(error) => error
            .with_callback_phase(P::NAME, CustomScanPhase::NextSlot)
            .report_after_switch(prior_ctx),
    }
}

#[inline]
unsafe fn next_slot<P: LakebaseCustomScanProvider>(
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

    let row_produced = P::next_slot(ctx);
    let _ = unsafe { pg_sys::MemoryContextSwitchTo(prior_ctx) };
    let row_produced = row_produced?;

    let slot_empty = unsafe { is_slot_empty(slot) };
    match decide(row_produced, slot_empty) {
        SlotOutcome::Return => unsafe {
            (*slot).tts_tableOid = (*scan_rel).rd_id;
        },
        SlotOutcome::RaiseEmptyProduced => {
            return Err(CustomScanError::slot_not_filled(P::NAME));
        }
        SlotOutcome::RaiseFilledEof => {
            let _ = unsafe { pg_sys::ExecClearTuple(slot) };
            return Err(CustomScanError::slot_filled_at_eof(P::NAME));
        }
        SlotOutcome::Eof => {}
    }

    Ok(slot)
}

/// Post-`next_slot` outcome from `(produced, slot_empty)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SlotOutcome {
    Return,
    Eof,
    /// `Ok(true)` but empty slot — invariant violation.
    RaiseEmptyProduced,
    /// `Ok(false)` but filled slot — invariant violation.
    RaiseFilledEof,
}

/// Maps `(produced, slot_empty)` to the framework outcome.
#[inline]
pub(crate) fn decide(produced: bool, slot_empty: bool) -> SlotOutcome {
    match (produced, slot_empty) {
        (true, false) => SlotOutcome::Return,
        (true, true) => SlotOutcome::RaiseEmptyProduced,
        (false, true) => SlotOutcome::Eof,
        (false, false) => SlotOutcome::RaiseFilledEof,
    }
}

/// Recheck callback for EPQ: set scantuple, reset per-tuple context, and run
/// the framework-owned exact filter recheck expression.
#[pg_guard]
unsafe extern "C-unwind" fn recheck_exact_filters<P: LakebaseCustomScanProvider>(
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

/// True when the live executor slot is empty.
///
/// # Safety
///
/// `slot` must be the non-null scan slot created by `ExecInitCustomScan`.
#[inline]
unsafe fn is_slot_empty(slot: *mut pg_sys::TupleTableSlot) -> bool {
    let flags = unsafe { (*slot).tts_flags } as u32;
    (flags & pg_sys::TTS_FLAG_EMPTY) != 0
}
