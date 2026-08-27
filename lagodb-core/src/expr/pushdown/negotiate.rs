//! Core-owned filter negotiation state machine.

use pgrx::pg_sys;

use crate::expr::contract::{PushdownContract, PushdownCosting};
use crate::expr::inspect::subtree_is_unsafe_to_push;
use crate::expr::pg::{PgBoolExpr, PgExprRef};

use super::{
    EffectiveFilterContract, FilterNormalizer, FilterPlan, FilterPushdownPlanner,
    NegotiatedFilterSet, NormalizedFilter,
};

/// Origin of a PostgreSQL planner clause list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScanClauseSource {
    BaseRestriction,
    Movable,
}

impl ScanClauseSource {
    #[inline]
    const fn requires_movability_gate(self) -> bool {
        matches!(self, Self::Movable)
    }
}

/// Single owner of provider negotiation and residual/recheck decisions.
pub(crate) struct FilterNegotiator<'a, P: FilterPushdownPlanner> {
    planner: &'a mut P,
    normalizer: FilterNormalizer,
    baserel: *mut pg_sys::RelOptInfo,
}

impl<'a, P: FilterPushdownPlanner> FilterNegotiator<'a, P> {
    pub(crate) fn new(
        planner: &'a mut P,
        rel_oid: pg_sys::Oid,
        baserel: *mut pg_sys::RelOptInfo,
    ) -> Self {
        let scan_relid = unsafe { (*baserel).relid as core::ffi::c_int };
        Self {
            planner,
            normalizer: FilterNormalizer::new(rel_oid, scan_relid),
            baserel,
        }
    }

    /// # Safety
    ///
    /// `clauses` must be NIL or a live planner-owned `List<RestrictInfo>`.
    pub(crate) unsafe fn negotiate(
        &mut self,
        clauses: *mut pg_sys::List,
        source: ScanClauseSource,
    ) -> Result<NegotiatedFilterSet<P::PlannedPredicate>, P::Error> {
        unsafe { self.negotiate_with_source(clauses, |_| source) }
    }

    /// # Safety
    ///
    /// `clauses` and every returned `RestrictInfo` must remain planner-owned
    /// and live for the duration of this call.
    pub(crate) unsafe fn negotiate_with_source<S>(
        &mut self,
        clauses: *mut pg_sys::List,
        mut source_for: S,
    ) -> Result<NegotiatedFilterSet<P::PlannedPredicate>, P::Error>
    where
        S: FnMut(*mut pg_sys::RestrictInfo) -> ScanClauseSource,
    {
        let mut out = NegotiatedFilterSet::new();
        let length = if clauses.is_null() {
            0
        } else {
            unsafe { pg_sys::list_length(clauses) }
        };
        for index in 0..length {
            let rinfo = unsafe { pg_sys::list_nth(clauses, index) }
                as *mut pg_sys::RestrictInfo;
            // PostgreSQL's planner owns a typed List<RestrictInfo> here.
            let clause = unsafe { (*rinfo).clause };
            // Match extract_actual_clauses() and postgres_fdw: pseudoconstants
            // are evaluated by the gating Result node, not as scan quals.
            if unsafe { (*rinfo).pseudoconstant } {
                continue;
            }
            let source = source_for(rinfo);
            if !unsafe {
                pg_sys::restriction_is_securely_promotable(rinfo, self.baserel)
            } || (source.requires_movability_gate()
                && !unsafe { pg_sys::join_clause_is_movable_to(rinfo, self.baserel) })
                || unsafe { subtree_is_unsafe_to_push(clause) }
            {
                out.push_residual(clause);
                continue;
            }
            unsafe { self.negotiate_expr(clause, &mut out) }?;
        }
        Ok(out)
    }

    unsafe fn negotiate_expr(
        &mut self,
        original: *mut pg_sys::Expr,
        out: &mut NegotiatedFilterSet<P::PlannedPredicate>,
    ) -> Result<(), P::Error> {
        if let Some(normalized) = unsafe { self.normalizer.normalize(original) } {
            match self.planner.try_plan_filter(&normalized.fragment)? {
                FilterPlan::Exact(planned) => {
                    out.accept(
                        original,
                        normalized,
                        planned.predicate,
                        EffectiveFilterContract {
                            contract: PushdownContract::ExactRowFilter,
                            costing: planned.costing,
                        },
                    );
                    return Ok(());
                }
                FilterPlan::Conservative(planned) => {
                    out.accept(
                        original,
                        normalized,
                        planned.predicate,
                        EffectiveFilterContract {
                            contract: PushdownContract::ConservativePruning,
                            costing: planned.costing,
                        },
                    );
                    return Ok(());
                }
                FilterPlan::Unsupported => {}
            }
        }

        let Some(boolean) = (unsafe { bool_expr(original) }) else {
            out.push_residual(original);
            return Ok(());
        };
        match boolean.boolop() {
            pg_sys::BoolExprType::AND_EXPR => {
                for child in bool_children(boolean) {
                    unsafe { self.negotiate_expr(child, out) }?;
                }
            }
            pg_sys::BoolExprType::OR_EXPR => {
                let mut candidates = Vec::new();
                for child in bool_children(boolean) {
                    let Some(candidate) = (unsafe { self.candidate_for(child) })?
                    else {
                        out.push_residual(original);
                        return Ok(());
                    };
                    candidates.push(candidate);
                }
                let is_widened =
                    candidates.iter().any(|candidate| candidate.is_widened);
                let Some(candidate) = (unsafe {
                    NormalizedFilter::combine_or(
                        candidates
                            .into_iter()
                            .map(|candidate| candidate.filter)
                            .collect(),
                    )
                }) else {
                    out.push_residual(original);
                    return Ok(());
                };
                if !is_widened {
                    out.push_residual(original);
                    return Ok(());
                }
                match self.planner.try_plan_filter(&candidate.fragment)? {
                    FilterPlan::Exact(planned)
                    | FilterPlan::Conservative(planned) => {
                        out.accept(
                            original,
                            candidate,
                            planned.predicate,
                            EffectiveFilterContract {
                                contract: PushdownContract::ConservativePruning,
                                costing: PushdownCosting::UncostedBestEffort,
                            },
                        );
                    }
                    FilterPlan::Unsupported => out.push_residual(original),
                }
            }
            pg_sys::BoolExprType::NOT_EXPR => out.push_residual(original),
            _ => out.push_residual(original),
        }
        Ok(())
    }

    unsafe fn candidate_for(
        &mut self,
        expr: *mut pg_sys::Expr,
    ) -> Result<Option<WideningCandidate>, P::Error> {
        if let Some(normalized) = unsafe { self.normalizer.normalize(expr) }
            && !matches!(
                self.planner.try_plan_filter(&normalized.fragment)?,
                FilterPlan::Unsupported
            )
        {
            return Ok(Some(WideningCandidate {
                filter: normalized,
                is_widened: false,
            }));
        }

        let Some(boolean) = (unsafe { bool_expr(expr) }) else {
            return Ok(None);
        };
        match boolean.boolop() {
            pg_sys::BoolExprType::AND_EXPR => {
                let children = bool_children(boolean);
                let mut candidates = Vec::new();
                for child in &children {
                    if let Some(candidate) = unsafe { self.candidate_for(*child) }? {
                        candidates.push(candidate);
                    }
                }
                let is_widened = candidates.len() != children.len()
                    || candidates.iter().any(|candidate| candidate.is_widened);
                Ok((unsafe {
                    NormalizedFilter::combine_and(
                        candidates
                            .into_iter()
                            .map(|candidate| candidate.filter)
                            .collect(),
                    )
                })
                .map(|filter| WideningCandidate { filter, is_widened }))
            }
            pg_sys::BoolExprType::OR_EXPR => {
                let mut candidates = Vec::new();
                for child in bool_children(boolean) {
                    let Some(candidate) = (unsafe { self.candidate_for(child) })?
                    else {
                        return Ok(None);
                    };
                    candidates.push(candidate);
                }
                let is_widened =
                    candidates.iter().any(|candidate| candidate.is_widened);
                Ok((unsafe {
                    NormalizedFilter::combine_or(
                        candidates
                            .into_iter()
                            .map(|candidate| candidate.filter)
                            .collect(),
                    )
                })
                .map(|filter| WideningCandidate { filter, is_widened }))
            }
            pg_sys::BoolExprType::NOT_EXPR => Ok(None),
            _ => Ok(None),
        }
    }
}

struct WideningCandidate {
    filter: NormalizedFilter,
    is_widened: bool,
}

/// # Safety
///
/// `expr` must remain live for the returned borrowed view.
unsafe fn bool_expr<'a>(expr: *mut pg_sys::Expr) -> Option<PgBoolExpr<'a>> {
    let expr = unsafe { PgExprRef::from_raw_opt(expr) }?;
    PgBoolExpr::try_from_expr(expr.without_relabels())
}

fn bool_children(boolean: PgBoolExpr<'_>) -> Vec<*mut pg_sys::Expr> {
    let args = boolean.args_list();
    let length = if args.is_null() {
        0
    } else {
        unsafe { pg_sys::list_length(args) }
    };
    let mut children = Vec::with_capacity(length as usize);
    for index in 0..length {
        children.push(unsafe { pg_sys::list_nth(args, index) } as *mut pg_sys::Expr);
    }
    children
}
