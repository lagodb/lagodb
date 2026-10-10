//! One-time binding of exact target scans to their owning ModifyTable.

use std::{ffi::c_void, ptr};

use pgrx::pg_sys;

use crate::access::scan::TableAmScanDesc;
use crate::api::TableAccessMethod;
use crate::customscan::ScanPurpose;
use crate::customscan::error::CustomScanError;
use crate::customscan::execution::exec::provider_scan_purpose;
use crate::customscan::execution::state::CustomScanStateWrapper;
use crate::customscan::provider::NeededColumns;
use crate::expr::inspect::{RelationExprAnalyzer, RelationScope};
use crate::expr::relation::RelationVarsByAttno;
use crate::handles::{OwnedScanKeys, RelationHandle, SnapshotHandle};

use super::LagodbCustomModifyProvider;
use super::binding::bind_modify_scan;
use super::bridge::ModifyNodeCell;
use super::scan_filters::ModifyScanFilters;

pub(super) struct ModifyScanBinder<'a, P: LagodbCustomModifyProvider> {
    execution: &'a ModifyNodeCell<P>,
    outer_plan: *mut pg_sys::CustomScan,
    // Like EState's relation arrays, indexed by RTI. Aliases of the same table
    // have different RTIs and must not acquire the target's mutation binding.
    targets: Vec<Option<i32>>,
    result: Result<(), CustomScanError>,
}

impl<'a, P: LagodbCustomModifyProvider> ModifyScanBinder<'a, P> {
    /// # Safety
    /// The outer and inner nodes are initialized in the same live EState; no
    /// target scan has produced rows yet.
    pub(super) unsafe fn bind(
        execution: &'a ModifyNodeCell<P>,
        outer_plan: *mut pg_sys::CustomScan,
        inner: *mut pg_sys::ModifyTableState,
    ) -> Result<(), CustomScanError> {
        let estate = unsafe { (*inner).ps.state };
        let modify = unsafe { (*inner).ps.plan.cast::<pg_sys::ModifyTable>() };
        let relations = unsafe { (*modify).resultRelations };
        let range_table_len =
            unsafe { pg_sys::list_length((*estate).es_range_table) } as usize;
        let mut targets = vec![None; range_table_len + 1];
        for index in 0..unsafe { pg_sys::list_length(relations) } {
            let rti = unsafe { pg_sys::list_nth_int(relations, index) } as usize;
            targets[rti] = Some(index);
        }
        let mut binder = Self {
            execution,
            outer_plan,
            targets,
            result: Ok(()),
        };
        unsafe { binder.walk((*inner).ps.lefttree) }?;
        binder.result
    }

    /// # Safety
    /// A non-null relation must be a live executor relation.
    pub(super) unsafe fn owns_relation(relation: pg_sys::Relation) -> bool {
        !relation.is_null()
            && P::AccessMethod::access_method_oid()
                .is_some_and(|oid| unsafe { (*(*relation).rd_rel).relam } == oid)
    }

    unsafe fn walk(
        &mut self,
        state: *mut pg_sys::PlanState,
    ) -> Result<(), CustomScanError> {
        if state.is_null() || self.result.is_err() {
            return Ok(());
        }
        if let Some(scan) = unsafe { Self::scan_state(state) } {
            let rti = unsafe { (*(*scan).ps.plan.cast::<pg_sys::Scan>()).scanrelid };
            if let Some(index) = self.targets[rti as usize]
                && unsafe { Self::owns_relation((*scan).ss_currentRelation) }
            {
                match unsafe { (*state).type_ } {
                    pg_sys::NodeTag::T_CustomScanState => {
                        unsafe { self.bind_custom(state.cast()) }?
                    }
                    pg_sys::NodeTag::T_SeqScanState => {
                        unsafe { self.bind_seq_scan(scan, index) }?
                    }
                    _ => {
                        return Err(CustomScanError::modify_binding(
                            "Modify target uses an unsupported native scan",
                        ));
                    }
                }
            }
        }
        unsafe {
            pg_sys::planstate_tree_walker_impl(
                state,
                Some(Self::walker),
                ptr::from_mut(self).cast(),
            );
        }
        Ok(())
    }

    unsafe fn bind_custom(
        &self,
        scan: *mut pg_sys::CustomScanState,
    ) -> Result<(), CustomScanError> {
        if unsafe { provider_scan_purpose::<P>((*scan).ss.ps.plan) }?
            != Some(ScanPurpose::ModifyTarget)
        {
            return Err(CustomScanError::modify_binding(
                "required Modify relation was planned with an unbindable CustomScan",
            ));
        }
        let context = unsafe { CustomScanStateWrapper::<P>::from_node_ptr(scan) }
            .active_provider_state_mut()
            .and_then(|state| P::modify_scan_context(state))
            .ok_or_else(|| {
                CustomScanError::modify_binding(
                    "ModifyTarget CustomScan provider state is not active",
                )
            })?;
        let relation = unsafe { (*scan).ss.ss_currentRelation };
        let binding = unsafe {
            self.execution
                .with_mut(|execution| execution.bind_scan((*relation).rd_id, context))
        }?;
        unsafe { bind_modify_scan::<P>(scan, binding) }
    }

    unsafe fn bind_seq_scan(
        &self,
        scan: *mut pg_sys::ScanState,
        index: i32,
    ) -> Result<(), CustomScanError> {
        // ExecInitSeqScan opens the relation and slot but leaves serial scan
        // creation to SeqNext. Install our explicitly prepared AM session here;
        // PG retains ownership of qualification, projection, rescan and end.
        debug_assert!(unsafe { (*scan).ss_currentScanDesc.is_null() });
        let state = unsafe { &mut (*scan).ps };
        let estate = unsafe { &*state.state };
        let relation =
            unsafe { RelationHandle::from_raw((*scan).ss_currentRelation) };
        let snapshot = unsafe { SnapshotHandle::from_raw(estate.es_snapshot) };
        let filters =
            unsafe { ModifyScanFilters::<P>::bind(self.outer_plan, index, state) }?;
        // SAFETY: walk dispatches here only for an initialized SeqScanState.
        let columns =
            unsafe { Self::required_columns(&*state.plan.cast::<pg_sys::SeqScan>()) };
        let needed_columns = columns
            .as_deref()
            .map_or(NeededColumns::All, NeededColumns::Subset);
        let prepared = P::prepare_modify_seq_scan(
            &relation,
            &snapshot,
            filters.bound(),
            needed_columns,
        )?;
        let flags = pg_sys::ScanOptions::SO_TYPE_SEQSCAN
            | pg_sys::ScanOptions::SO_ALLOW_STRAT
            | pg_sys::ScanOptions::SO_ALLOW_SYNC
            | pg_sys::ScanOptions::SO_ALLOW_PAGEMODE;
        let descriptor = unsafe {
            TableAmScanDesc::<<P::AccessMethod as TableAccessMethod>::ScanSession>::create(
                estate.es_query_cxt,
                relation.as_raw(),
                estate.es_snapshot,
                ptr::null_mut(),
                flags,
                OwnedScanKeys::empty(),
                prepared.session,
            )
        };
        unsafe { (*scan).ss_currentScanDesc = descriptor.cast() };
        let binding = unsafe {
            self.execution.with_mut(|execution| {
                execution.bind_scan(relation.oid(), prepared.context)
            })
        }?;
        P::bind_modify_seq_scan(unsafe { (*descriptor).instance_mut() }, binding)
    }

    /// # Safety
    /// The SeqScan and its final expression trees must remain live for this call.
    unsafe fn required_columns(
        scan: &pg_sys::SeqScan,
    ) -> Option<Vec<pg_sys::AttrNumber>> {
        // PG's setrefs adjusts SeqScan Vars and scanrelid together. Analyze the
        // final output and all PG quals, including those also pushed to storage.
        // Exact pushdown does not make qual-only columns removable: native
        // SeqScan retains and evaluates those quals under the EPQ contract
        // documented on LagodbCustomModifyProvider::prepare_modify_seq_scan.
        let analyzer =
            RelationExprAnalyzer::new(RelationScope::exact(scan.scan.scanrelid));
        // SAFETY: the caller supplies live target entries and qual expressions;
        // PostgreSQL's expression walker descends through TargetEntry nodes.
        let mut usage =
            unsafe { analyzer.collect_expr_list(scan.scan.plan.targetlist) };
        usage.extend(unsafe { analyzer.collect_expr_list(scan.scan.plan.qual) });
        if usage.has_whole_row() {
            return None;
        }
        let mut columns = RelationVarsByAttno::default();
        for var in usage.user_vars() {
            columns.insert(var.raw);
        }
        // System columns come from slot metadata; an empty user-column set is
        // valid for identity-only DELETE. No plan pointers escape this call.
        Some(columns.attnos().collect())
    }

    unsafe fn scan_state(
        state: *mut pg_sys::PlanState,
    ) -> Option<*mut pg_sys::ScanState> {
        matches!(
            unsafe { (*state).type_ },
            pg_sys::NodeTag::T_SeqScanState
                | pg_sys::NodeTag::T_SampleScanState
                | pg_sys::NodeTag::T_IndexScanState
                | pg_sys::NodeTag::T_IndexOnlyScanState
                | pg_sys::NodeTag::T_BitmapHeapScanState
                | pg_sys::NodeTag::T_TidScanState
                | pg_sys::NodeTag::T_TidRangeScanState
                | pg_sys::NodeTag::T_ForeignScanState
                | pg_sys::NodeTag::T_CustomScanState
        )
        .then_some(state.cast())
    }

    unsafe extern "C-unwind" fn walker(
        state: *mut pg_sys::PlanState,
        raw: *mut c_void,
    ) -> bool {
        let binder = unsafe { &mut *raw.cast::<Self>() };
        if let Err(error) = unsafe { binder.walk(state) } {
            binder.result = Err(error);
        }
        binder.result.is_err()
    }
}
