//! PostgreSQL FDW executor callback trampolines.

use core::ffi::c_int;
use core::ptr;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::handles::{RelationHandle, SnapshotHandle};

use super::contract::FdwScan;
use super::error::{ForeignScanError, ForeignScanPhase};
use super::executor::{compile_executor_layout, list_len, slot_is_empty};
use super::filter::{ForeignFilterExprs, ForeignScanFilters};
use super::private::decode_scan_private;
use super::pushdown::ReScanForeignScanContext;
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
    let result = (|| {
        if (eflags as u32) & pg_sys::EXEC_FLAG_EXPLAIN_ONLY != 0 {
            // PostgreSQL will not call Iterate for EXPLAIN_ONLY.  Leaving
            // fdw_state NULL also mirrors postgres_fdw's no-resource path.
            return Ok(());
        }
        let plan = unsafe { (*node).ss.ps.plan } as *mut pg_sys::ForeignScan;
        if unsafe { (*plan).operation } != pg_sys::CmdType::CMD_SELECT {
            return Err(ForeignScanError::unsupported(
                "FDW framework v1 supports SELECT scans only",
            ));
        }
        let relation = unsafe { (*node).ss.ss_currentRelation };
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
        // SAFETY: PostgreSQL initialized the live plan, base relation, and
        // HeapTuple scan slot before invoking BeginForeignScan.
        let write_layout = unsafe {
            compile_executor_layout(
                plan,
                relation,
                &decoded.projection,
                &decoded.write_plan,
                decoded.row_identity,
                &decoded.requirements,
                slot,
            )?
        };

        let parent = unsafe { &mut (*node).ss.ps as *mut pg_sys::PlanState };
        let expression_sections = unsafe {
            ForeignFilterExprs::split((*plan).fdw_exprs, decoded.binding_count)
        }?;
        let filters = unsafe {
            ForeignScanFilters::<P>::initialize(
                &decoded,
                expression_sections.bindings,
                parent,
            )
        }?;
        if filters.recheck_count() != unsafe { list_len((*plan).fdw_recheck_quals) } {
            return Err(ForeignScanError::framework(
                "FDW exact planned-filter and recheck-expression counts differ",
            ));
        }
        let fdw_expr_states =
            unsafe { pg_sys::ExecInitExprList(expression_sections.provider, parent) };
        let mut wrapper = ForeignScanStateWrapper::<P>::new(
            decoded,
            filters,
            fdw_expr_states,
            write_layout,
            eflags,
        );
        unsafe { wrapper.initialize_provider(node) }?;
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
            .with_callback_phase::<P>(ForeignScanPhase::Begin)
            .report();
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
    let result = (|| {
        let state_raw = unsafe { (*node).fdw_state };
        // SAFETY: ExecInitForeignScan calls BeginForeignScan before Iterate,
        // and Begin publishes this wrapper. EXPLAIN_ONLY never reaches Iterate.
        let wrapper = unsafe { &mut *(state_raw as *mut ForeignScanStateWrapper<P>) };
        if !wrapper.provider_started() {
            // PARAM_EXEC values are valid by the time PostgreSQL requests the
            // first row, even when no explicit ReScan preceded it.
            unsafe { wrapper.start_provider(node) }?;
        }
        let slot = unsafe { (*node).ss.ss_ScanTupleSlot };
        // SAFETY: Begin installed provider state, and the delayed start above
        // activated its first parameter set. It remains initialized until
        // EndForeignScan cleanup.
        let state_ptr = unsafe { wrapper.payload.provider_state_ptr_unchecked() };
        // SAFETY: Begin compiled the write layout for this PostgreSQL-owned slot.
        // PostgreSQL's ForeignNext has already switched
        // to the executor's per-tuple memory context for this callback.
        let mut writer = unsafe { wrapper.output_writer(slot) };
        let produced = P::next_slot(unsafe { &mut *state_ptr }, &mut writer)?;
        if produced {
            writer.complete()?;
        }

        if produced {
            if unsafe { slot_is_empty(slot) } {
                return Err(ForeignScanError::slot_not_filled(P::NAME));
            }
        } else {
            // The writer defers clearing until the output representation is
            // known.  EOF has no representation, so return an empty slot here.
            // SAFETY: slot is the live scan slot supplied by PostgreSQL.
            unsafe { pg_sys::ExecClearTuple(slot) };
        }
        Ok::<*mut pg_sys::TupleTableSlot, ForeignScanError>(slot)
    })();

    match result {
        Ok(slot) => slot,
        Err(error) => error
            .with_callback_phase::<P>(ForeignScanPhase::Iterate)
            .report(),
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
        let state_raw = unsafe { (*node).fdw_state };
        // SAFETY: BeginForeignScan publishes this wrapper before PostgreSQL can
        // invoke ReScanForeignScan. EXPLAIN_ONLY never reaches this callback.
        let wrapper = unsafe { &mut *(state_raw as *mut ForeignScanStateWrapper<P>) };
        let relation = unsafe { (*node).ss.ss_currentRelation };
        let estate = unsafe { (*node).ss.ps.state };
        let econtext = unsafe { (*node).ss.ps.ps_ExprContext };
        let snapshot = unsafe { (*estate).es_snapshot };

        // PostgreSQL's ReScanExprContext has already reset the per-tuple
        // context before this FDW callback.  ExecReScanForeignScan calls
        // ExecScanReScan after the callback, which clears the scan slot.
        // Only the framework-owned datum-default state needs resetting here.
        wrapper.reset_datum_defaults_for_rescan();

        if !wrapper.provider_started() {
            // A parameterized inner scan reaches ReScan after Nested Loop has
            // populated its PARAM_EXEC values. Start the first parameter set
            // directly instead of treating it as a provider rescan.
            unsafe { wrapper.start_provider(node) }?;
            return Ok::<(), ForeignScanError>(());
        }

        // SAFETY: PostgreSQL's expression evaluator switches to the live
        // ecxt_per_tuple_memory context for each expression and restores the
        // caller's current context before returning.
        let chg_param = unsafe { (*node).ss.ps.chgParam };
        let filters_changed = unsafe {
            wrapper
                .filters
                .as_ref()
                .expect("FDW filters must remain installed until EndForeignScan")
                .filters_changed(chg_param)
        };
        if filters_changed {
            unsafe {
                wrapper
                    .filters
                    .as_mut()
                    .expect("FDW filters must remain installed until EndForeignScan")
                    .rebind_dynamic(econtext)
            }?;
        }
        unsafe { wrapper.refresh_runtime_values(econtext) }?;

        // SAFETY: provider_started is set only after the state has been
        // installed and the provider start callback succeeds.
        let state_ptr = unsafe { wrapper.payload.provider_state_ptr_unchecked() };
        let context = ReScanForeignScanContext {
            // SAFETY: PostgreSQL keeps the scan relation live through End.
            relation: unsafe { RelationHandle::from_raw(relation) },
            // SAFETY: PostgreSQL keeps the executor snapshot live for the query.
            snapshot: unsafe { SnapshotHandle::from_raw(snapshot) },
            projection: &wrapper.projection,
            required_columns: &wrapper.required_columns,
            filters: wrapper
                .filters
                .as_ref()
                .expect("FDW filters must remain installed until EndForeignScan")
                .bound(),
            expressions: wrapper.runtime_values(),
            filters_changed,
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
            .with_callback_phase::<P>(ForeignScanPhase::ReScan)
            .report();
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
            .with_callback_phase::<P>(ForeignScanPhase::End)
            .report_warning();
    }
}
