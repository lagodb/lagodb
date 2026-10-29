//! Binding of a ModifyTable state to its exact initialized target ForeignScan.

use core::ffi::c_void;
use core::marker::PhantomData;

use pgrx::pg_sys;

use super::contract::FdwModify;
use super::error::ForeignModifyError;
use crate::fdw::scan::ForeignScanStateWrapper;

pub(super) enum ForeignModifyTargetScanMatch<T> {
    /// The initialized modify subtree contains no target scan for this result
    /// relation, so it cannot produce a row callback in this execution.
    NotInitialized,
    Initialized(Option<T>),
}

pub(super) struct ForeignModifyTargetScan<P: FdwModify> {
    relation_oid: pg_sys::Oid,
    range_table_index: pg_sys::Index,
    matched: bool,
    found: Option<P::TargetScanContext>,
    error: Option<ForeignModifyError>,
    _provider: PhantomData<fn() -> P>,
}

impl<P: FdwModify> ForeignModifyTargetScan<P> {
    pub(super) unsafe fn find(
        root: *mut pg_sys::PlanState,
        relation_oid: pg_sys::Oid,
        range_table_index: pg_sys::Index,
    ) -> Result<ForeignModifyTargetScanMatch<P::TargetScanContext>, ForeignModifyError>
    {
        let mut finder = Self {
            relation_oid,
            range_table_index,
            matched: false,
            found: None,
            error: None,
            _provider: PhantomData,
        };
        unsafe { finder.walk(root) };
        match finder.error {
            Some(error) => Err(error),
            None if finder.matched => {
                Ok(ForeignModifyTargetScanMatch::Initialized(finder.found))
            }
            None => Ok(ForeignModifyTargetScanMatch::NotInitialized),
        }
    }

    unsafe fn walk(&mut self, plan_state: *mut pg_sys::PlanState) {
        if plan_state.is_null() || self.error.is_some() {
            return;
        }
        if unsafe { (*plan_state).type_ } == pg_sys::NodeTag::T_ForeignScanState {
            unsafe { self.inspect_foreign_scan(plan_state.cast()) };
            if self.error.is_some() {
                return;
            }
        }
        unsafe {
            pg_sys::planstate_tree_walker_impl(
                plan_state,
                Some(Self::walker),
                core::ptr::from_mut(self).cast(),
            );
        }
    }

    unsafe fn inspect_foreign_scan(&mut self, scan: *mut pg_sys::ForeignScanState) {
        let relation = unsafe { (*scan).ss.ss_currentRelation };
        let plan = unsafe { (*scan).ss.ps.plan } as *mut pg_sys::ForeignScan;
        if unsafe { (*relation).rd_id } != self.relation_oid
            || unsafe { (*plan).scan.scanrelid } != self.range_table_index
        {
            return;
        }
        let raw = unsafe { (*scan).fdw_state };
        // SAFETY: a scan of the result relation is owned by the same provider
        // as ResultRelInfo, and core's BeginForeignScan stores this exact
        // wrapper type in fdw_state before BeginForeignModify is called.
        let wrapper = unsafe { &*(raw as *const ForeignScanStateWrapper<P>) };
        if self.matched {
            self.error = Some(ForeignModifyError::framework(
                "ModifyTable has more than one matching target ForeignScan",
            ));
            return;
        }
        self.matched = true;
        // SAFETY: BeginForeignScan installs provider state before
        // BeginForeignModify walks the initialized target plan tree.
        let state = unsafe { wrapper.payload.provider_state_unchecked() };
        self.found = P::target_scan_context(state);
    }

    unsafe extern "C-unwind" fn walker(
        plan_state: *mut pg_sys::PlanState,
        raw: *mut c_void,
    ) -> bool {
        let finder = unsafe { &mut *raw.cast::<Self>() };
        unsafe { finder.walk(plan_state) };
        finder.error.is_some()
    }
}
