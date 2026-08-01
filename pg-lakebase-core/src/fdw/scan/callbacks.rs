//! PostgreSQL FDW executor callback trampolines.

use core::ffi::c_int;
use core::ptr;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::handles::{RelationHandle, SnapshotHandle};

use super::contract::FdwScan;
use super::error::{ForeignScanError, ForeignScanPhase};
use super::executor::{list_len, slot_is_empty, validate_executor_layout};
use super::private::decode_scan_private;
use super::pushdown::{ForeignExprList, ForeignPushdown, ReScanForeignScanContext};
use super::state::ForeignScanStateWrapper;

#[pg_guard]
/// # Safety
///
/// PostgreSQL must invoke this callback with a live `ForeignScanState` and the
/// executor-owned plan, relation, slot, memory contexts, and snapshot expected
/// by the FDW callback contract.
pub(crate) unsafe extern "C-unwind" fn begin_foreign_scan<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
    eflags: c_int,
) {
    // SAFETY: PostgreSQL invokes this callback with a live executor context.
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        if node.is_null() {
            return Err(ForeignScanError::framework(
                "BeginForeignScan received a NULL ForeignScanState",
            ));
        }
        if (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0 {
            // PostgreSQL will not call Iterate for EXPLAIN_ONLY.  Leaving
            // fdw_state NULL also mirrors postgres_fdw's no-resource path.
            return Ok(());
        }
        if !unsafe { (*node).fdw_state }.is_null() {
            return Err(ForeignScanError::framework(
                "BeginForeignScan received an already initialized fdw_state",
            ));
        }

        let plan = unsafe { (*node).ss.ps.plan } as *mut pg_sys::ForeignScan;
        if plan.is_null()
            || unsafe { (*plan).scan.plan.type_ } != pg_sys::NodeTag::T_ForeignScan
        {
            return Err(ForeignScanError::framework(
                "BeginForeignScan plan is not a ForeignScan node",
            ));
        }
        if unsafe { (*plan).operation } != pg_sys::CmdType::CMD_SELECT {
            return Err(ForeignScanError::unsupported(
                "FDW framework v1 supports SELECT scans only",
            ));
        }
        let relation = unsafe { (*node).ss.ss_currentRelation };
        if relation.is_null() {
            return Err(ForeignScanError::unsupported(
                "FDW framework v1 supports base-relation scans only",
            ));
        }
        let opened_oid = unsafe { (*relation).rd_id };
        // SAFETY: plan is a validated ForeignScan and fdw_private is its live
        // framework-owned plan data.
        let decoded = unsafe { decode_scan_private::<P>((*plan).fdw_private) }?;
        if decoded.relation_oid != opened_oid {
            return Err(ForeignScanError::framework(
                "FDW plan relation OID does not match the executor-opened relation",
            ));
        }

        let slot = unsafe { (*node).ss.ss_ScanTupleSlot };
        // SAFETY: plan, relation, and slot are live executor objects; the
        // helper validates their TupleDesc and projection layout synchronously.
        let write_layout = unsafe {
            validate_executor_layout(
                plan,
                relation,
                &decoded.projection,
                &decoded.write_plan,
                decoded.row_identity,
                &decoded.requirements,
                &decoded.column_refs,
                slot,
            )?
        };
        let exact_contract_count = decoded
            .contracts
            .iter()
            .filter(|contract| contract.requires_recheck())
            .count();
        if exact_contract_count != unsafe { list_len((*plan).fdw_recheck_quals) } {
            return Err(ForeignScanError::framework(
                "FDW exact recheck-expression and pushdown-contract counts differ",
            ));
        }

        let estate = unsafe { (*node).ss.ps.state };
        let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
        if estate.is_null() || econtext.is_null() {
            return Err(ForeignScanError::framework(
                "BeginForeignScan has no executor state or expression context",
            ));
        }
        let query_context = unsafe { (*estate).es_query_cxt };
        let per_tuple_context = unsafe { (*econtext).ecxt_per_tuple_memory };
        let snapshot = unsafe { (*estate).es_snapshot };
        if query_context.is_null()
            || per_tuple_context.is_null()
            || snapshot.is_null()
        {
            return Err(ForeignScanError::framework(
                "BeginForeignScan has no query/per-tuple memory context or snapshot",
            ));
        }

        let parent = unsafe { &mut (*node).ss.ps as *mut pg_sys::PlanState };
        let fdw_expr_states =
            unsafe { pg_sys::ExecInitExprList((*plan).fdw_exprs, parent) };
        let wrapper = ForeignScanStateWrapper::<P>::new(
            decoded,
            fdw_expr_states,
            write_layout,
            eflags,
        );
        let wrapper_ptr = wrapper.leak();
        // SAFETY: wrapper_ptr was allocated in the executor query context and
        // node is the live ForeignScanState supplied by PostgreSQL.
        unsafe {
            (*node).fdw_state = wrapper_ptr.cast();
        }
        Ok::<(), ForeignScanError>(())
    })();

    if let Err(error) = result {
        error
            .with_provider_phase::<P>(ForeignScanPhase::Begin)
            .report_after_switch(prior_ctx);
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL must invoke this callback with the live `ForeignScanState` whose
/// `fdw_state` was initialized by `begin_foreign_scan` for the same provider.
pub(crate) unsafe extern "C-unwind" fn iterate_foreign_scan<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    // SAFETY: PostgreSQL invokes this callback with a live executor context.
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        if node.is_null() {
            return Err(ForeignScanError::framework(
                "IterateForeignScan received a NULL ForeignScanState",
            ));
        }
        let state_raw = unsafe { (*node).fdw_state };
        if state_raw.is_null() {
            return Err(ForeignScanError::framework(
                "IterateForeignScan has no initialized fdw_state",
            ));
        }
        // SAFETY: BeginForeignScan publishes only this wrapper type in
        // fdw_state, and the non-null check above protects the cast.
        let wrapper = unsafe { &mut *(state_raw as *mut ForeignScanStateWrapper<P>) };
        if !wrapper.payload.provider_state_initialized() {
            // PARAM_EXEC values are valid by the time PostgreSQL requests the
            // first row, even when no explicit ReScan preceded it.
            unsafe { wrapper.begin_provider(node) }.map_err(|error| {
                error.with_provider_phase::<P>(ForeignScanPhase::ProviderStart)
            })?;
        }
        let slot = unsafe { (*node).ss.ss_ScanTupleSlot };
        let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
        if slot.is_null() || econtext.is_null() {
            return Err(ForeignScanError::framework(
                "IterateForeignScan has no scan slot or expression context",
            ));
        }
        let per_tuple_context = unsafe { (*econtext).ecxt_per_tuple_memory };
        if per_tuple_context.is_null() {
            return Err(ForeignScanError::framework(
                "IterateForeignScan has no per-tuple memory context",
            ));
        }
        let entry_context = unsafe { pg_sys::CurrentMemoryContext };
        let mut run = || {
            let state_ptr =
                wrapper.payload.provider_state_ptr().ok_or_else(|| {
                    ForeignScanError::framework(
                        "IterateForeignScan has no initialized provider state",
                    )
                })?;
            // SAFETY: Begin validated the slot and write-layout invariants
            // required by ScanSlotWriter::new.
            let mut writer = unsafe { wrapper.output_writer(slot) };
            // SAFETY: provider_state_initialized proves state_ptr contains a
            // live P::State for this scan.
            let produced = P::next_slot(unsafe { &mut *state_ptr }, &mut writer)?;
            if produced {
                writer.complete()?;
            }
            Ok::<bool, ForeignScanError>(produced)
        };
        let produced_result = if entry_context == per_tuple_context {
            run()
        } else {
            // SAFETY: per_tuple_context is a live executor context and the
            // closure does not let borrowed state escape the switch.
            unsafe { PgMemoryContexts::For(per_tuple_context).switch_to(|_| run()) }
        };
        // SAFETY: PostgreSQL owns the current context for this callback.
        if unsafe { pg_sys::CurrentMemoryContext } != entry_context {
            // SAFETY: entry_context was captured from PostgreSQL above.
            unsafe { pg_sys::MemoryContextSwitchTo(entry_context) };
        }
        let produced = produced_result?;

        if produced {
            if unsafe { slot_is_empty(slot) } {
                return Err(ForeignScanError::slot_not_filled(P::NAME));
            }
        } else {
            // The writer defers clearing until the output representation is
            // known.  EOF has no representation, so return an empty slot here.
            // SAFETY: slot is the live scan slot validated above.
            unsafe { pg_sys::ExecClearTuple(slot) };
        }
        Ok::<*mut pg_sys::TupleTableSlot, ForeignScanError>(slot)
    })();

    match result {
        Ok(slot) => slot,
        Err(error) => error
            .with_provider_phase::<P>(ForeignScanPhase::Iterate)
            .report_after_switch(prior_ctx),
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL must invoke this callback with the live `ForeignScanState` whose
/// wrapper and executor contexts are still initialized for the scan.
pub(crate) unsafe extern "C-unwind" fn rescan_foreign_scan<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
) {
    // SAFETY: PostgreSQL invokes this callback with a live executor context.
    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let result = (|| {
        if node.is_null() {
            return Err(ForeignScanError::framework(
                "ReScanForeignScan received a NULL ForeignScanState",
            ));
        }
        let state_raw = unsafe { (*node).fdw_state };
        if state_raw.is_null() {
            return Err(ForeignScanError::framework(
                "ReScanForeignScan has no initialized fdw_state",
            ));
        }
        // SAFETY: BeginForeignScan publishes only this wrapper type in
        // fdw_state, and the non-null check above protects the cast.
        let wrapper = unsafe { &mut *(state_raw as *mut ForeignScanStateWrapper<P>) };
        let plan = unsafe { (*node).ss.ps.plan } as *mut pg_sys::ForeignScan;
        let relation = unsafe { (*node).ss.ss_currentRelation };
        let slot = unsafe { (*node).ss.ss_ScanTupleSlot };
        let estate = unsafe { (*node).ss.ps.state };
        let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
        if plan.is_null()
            || unsafe { (*plan).scan.plan.type_ } != pg_sys::NodeTag::T_ForeignScan
            || relation.is_null()
            || slot.is_null()
            || estate.is_null()
            || econtext.is_null()
        {
            return Err(ForeignScanError::framework(
                "ReScanForeignScan has incomplete executor state",
            ));
        }
        let per_tuple_context = unsafe { (*econtext).ecxt_per_tuple_memory };
        let snapshot = unsafe { (*estate).es_snapshot };
        if per_tuple_context.is_null() || snapshot.is_null() {
            return Err(ForeignScanError::framework(
                "ReScanForeignScan has no per-tuple memory context or snapshot",
            ));
        }

        // PostgreSQL's ReScanExprContext has already reset the per-tuple
        // context before this FDW callback.  ExecReScanForeignScan calls
        // ExecScanReScan after the callback, which clears the scan slot.
        // Only the framework-owned datum-default state needs resetting here.
        wrapper.reset_datum_defaults_for_rescan();

        if !wrapper.payload.provider_state_initialized() {
            // A parameterized inner scan reaches ReScan after Nested Loop has
            // populated its PARAM_EXEC values.  Start directly from that
            // current tuple instead of treating it as a provider rescan.
            unsafe { wrapper.begin_provider(node) }.map_err(|error| {
                error.with_provider_phase::<P>(ForeignScanPhase::ProviderStart)
            })?;
            return Ok::<(), ForeignScanError>(());
        }

        // SAFETY: PostgreSQL's expression evaluator switches to the live
        // ecxt_per_tuple_memory context for each expression and restores the
        // caller's current context before returning.
        unsafe { wrapper.refresh_runtime_values(econtext) }?;

        let params_changed = unsafe { (*node).ss.ps.chgParam };
        // SAFETY: the recheck list is live plan data and the wrapper slices are
        // borrowed for the duration of this synchronous provider call.
        let pushdown = unsafe {
            ForeignPushdown::new(
                ForeignExprList::from_raw((*plan).fdw_recheck_quals),
                &wrapper.contracts,
                &wrapper.column_refs,
            )
        };
        let state_ptr = wrapper.payload.provider_state_ptr().ok_or_else(|| {
            ForeignScanError::framework(
                "ReScanForeignScan has no initialized provider state",
            )
        })?;
        let context = ReScanForeignScanContext {
            // SAFETY: relation was checked non-null and remains executor-owned.
            relation: unsafe { RelationHandle::from_raw(relation) },
            // SAFETY: snapshot was checked non-null and remains executor-owned.
            snapshot: unsafe { SnapshotHandle::from_raw(snapshot) },
            projection: &wrapper.projection,
            required_columns: &wrapper.required_columns,
            pushdown,
            expressions: wrapper.runtime_values(),
            params_changed: !params_changed.is_null(),
            estate,
            econtext,
        };
        // SAFETY: provider_state_initialized proves state_ptr contains a live
        // P::State for this scan.
        let result = unsafe { P::rescan(&mut *state_ptr, context) };
        // SAFETY: prior_ctx was captured from PostgreSQL at callback entry.
        unsafe { pg_sys::MemoryContextSwitchTo(prior_ctx) };
        result?;
        Ok::<(), ForeignScanError>(())
    })();

    if let Err(error) = result {
        error
            .with_provider_phase::<P>(ForeignScanPhase::ReScan)
            .report_after_switch(prior_ctx);
    }
}

#[pg_guard]
/// # Safety
///
/// PostgreSQL must invoke this callback at most once for a live scan state; the
/// wrapper pointer must have been published by `begin_foreign_scan`.
pub(crate) unsafe extern "C-unwind" fn end_foreign_scan<P: FdwScan>(
    node: *mut pg_sys::ForeignScanState,
) {
    if node.is_null() {
        return;
    }
    // Remove the PostgreSQL-visible pointer before calling provider teardown.
    // SAFETY: node is the live ForeignScanState supplied by PostgreSQL.
    let state_raw = unsafe { (*node).fdw_state };
    // SAFETY: node remains live; clearing the published pointer prevents reuse.
    unsafe { (*node).fdw_state = ptr::null_mut() };
    if state_raw.is_null() {
        return;
    }
    // SAFETY: BeginForeignScan stored exactly this wrapper type in fdw_state.
    let wrapper = unsafe { &mut *(state_raw as *mut ForeignScanStateWrapper<P>) };

    let prior_ctx = unsafe { pg_sys::CurrentMemoryContext };
    let end_error = if wrapper.payload.provider_state_initialized() {
        let state_ptr = wrapper
            .payload
            .provider_state_ptr()
            .expect("provider state flag must have a pointer");
        // SAFETY: provider_state_initialized proves state_ptr contains a live
        // P::State for this scan.
        unsafe { P::end(&mut *state_ptr) }.err()
    } else {
        None
    };
    // SAFETY: prior_ctx was captured from PostgreSQL before provider teardown.
    unsafe { pg_sys::MemoryContextSwitchTo(prior_ctx) };
    wrapper.cleanup_payloads();

    if let Some(error) = end_error {
        error
            .with_provider_phase::<P>(ForeignScanPhase::End)
            .report_warning();
    }
}
