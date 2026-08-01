//! Parameterized base-relation path candidate enumeration.

use core::ffi::c_void;
use core::ptr;

use pgrx::pg_guard;
use pgrx::pg_sys;

use crate::expr::split::PlannerClauseGate;

use super::error::ForeignScanError;

pub(super) struct ParameterizedCandidates {
    rel: *mut pg_sys::RelOptInfo,
    lateral_relids: pg_sys::Relids,
    relids: pg_sys::Relids,
    gate: PlannerClauseGate,
    values: Vec<pg_sys::Relids>,
}

impl ParameterizedCandidates {
    /// # Safety
    ///
    /// `rel` must be the live base relation supplied by PostgreSQL during its
    /// `GetForeignPaths` callback.
    pub(super) unsafe fn new(rel: *mut pg_sys::RelOptInfo) -> Self {
        Self {
            rel,
            lateral_relids: unsafe { (*rel).lateral_relids },
            relids: unsafe { (*rel).relids },
            gate: PlannerClauseGate::for_relation(rel),
            values: Vec::new(),
        }
    }

    /// # Safety
    ///
    /// `self.rel`, `root`, and all planner lists reachable from them must be
    /// live for the synchronous candidate-enumeration traversal.
    pub(super) unsafe fn enumerate(
        mut self,
        root: *mut pg_sys::PlannerInfo,
    ) -> Result<Vec<pg_sys::Relids>, ForeignScanError> {
        let joininfo = unsafe { (*self.rel).joininfo };
        if !joininfo.is_null() {
            let length = unsafe { pg_sys::list_length(joininfo) };
            for index in 0..length {
                let rinfo = unsafe { pg_sys::list_nth(joininfo, index) }
                    as *mut pg_sys::RestrictInfo;
                unsafe { self.consider_clause(rinfo) };
            }
        }

        if unsafe { (*self.rel).has_eclass_joins } {
            let mut selection = EcMemberSelection {
                current: ptr::null_mut(),
                already_used: ptr::null_mut(),
            };
            loop {
                selection.current = ptr::null_mut();
                let implied = unsafe {
                    pg_sys::generate_implied_equalities_for_column(
                        root,
                        self.rel,
                        Some(ec_member_is_scan_var),
                        (&mut selection as *mut EcMemberSelection).cast(),
                        (*self.rel).lateral_referencers,
                    )
                };
                if selection.current.is_null() {
                    break;
                }
                if !implied.is_null() {
                    let length = unsafe { pg_sys::list_length(implied) };
                    for index in 0..length {
                        let rinfo = unsafe { pg_sys::list_nth(implied, index) }
                            as *mut pg_sys::RestrictInfo;
                        unsafe { self.consider_clause(rinfo) };
                    }
                }
                selection.already_used = unsafe {
                    pg_sys::lappend(selection.already_used, selection.current.cast())
                };
            }
        }
        Ok(self.values)
    }

    /// # Safety
    ///
    /// `rinfo` must be NULL or a live planner `RestrictInfo` reachable from
    /// `self.rel`'s join information or implied-equality list.
    unsafe fn consider_clause(&mut self, rinfo: *mut pg_sys::RestrictInfo) {
        if rinfo.is_null()
            || unsafe { (*rinfo).pseudoconstant }
            || !unsafe { self.gate.is_securely_promotable(rinfo) }
            || !unsafe { self.gate.is_movable_to_relation(rinfo) }
        {
            return;
        }
        let clause_outer =
            unsafe { pg_sys::bms_difference((*rinfo).clause_relids, self.relids) };
        let lateral = unsafe { pg_sys::bms_copy(self.lateral_relids) };
        let candidate = unsafe { pg_sys::bms_union(lateral, clause_outer) };
        if unsafe { pg_sys::bms_equal(candidate, self.lateral_relids) }
            || self
                .values
                .iter()
                .any(|value| unsafe { pg_sys::bms_equal(*value, candidate) })
        {
            return;
        }
        self.values.push(candidate);
    }
}

/// Stateful callback context matching PostgreSQL's repeated EC enumeration.
struct EcMemberSelection {
    current: *mut pg_sys::Expr,
    already_used: *mut pg_sys::List,
}

/// # Safety
///
/// PostgreSQL invokes this callback with live `rel` and `em` planner objects.
/// If `em_expr` is a RelabelType or Var, its node layout and referenced
/// relation bitmap must remain valid for the synchronous callback.
#[pg_guard]
unsafe extern "C-unwind" fn ec_member_is_scan_var(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    _ec: *mut pg_sys::EquivalenceClass,
    em: *mut pg_sys::EquivalenceMember,
    arg: *mut c_void,
) -> bool {
    if rel.is_null() || em.is_null() || arg.is_null() {
        return false;
    }
    let selection = unsafe { &mut *(arg.cast::<EcMemberSelection>()) };
    let expr = unsafe { (*em).em_expr };
    if expr.is_null() {
        return false;
    }
    if !selection.current.is_null() {
        return unsafe { pg_sys::equal(expr.cast(), selection.current.cast()) };
    }
    if !selection.already_used.is_null()
        && unsafe { pg_sys::list_member(selection.already_used, expr.cast()) }
    {
        return false;
    }
    let mut node = expr.cast::<pg_sys::Node>();
    while !node.is_null()
        && unsafe { (*node).type_ } == pg_sys::NodeTag::T_RelabelType
    {
        node = unsafe { (*node.cast::<pg_sys::RelabelType>()).arg }.cast();
    }
    if node.is_null() || unsafe { (*node).type_ } != pg_sys::NodeTag::T_Var {
        return false;
    }
    let var = node.cast::<pg_sys::Var>();
    unsafe {
        if (*var).varattno <= 0
            || (*var).varlevelsup != 0
            || !pg_sys::bms_is_member((*var).varno, (*rel).relids)
        {
            return false;
        }
    }
    selection.current = expr;
    true
}
