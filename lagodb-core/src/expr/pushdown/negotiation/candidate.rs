//! Shared conservative-candidate construction for relation and query scans.

use pgrx::pg_sys;

use super::super::{
    FilterPlan, FilterPushdownPlanner, NormalizedPredicate, PredicateFragment,
    QueryExpressionNormalizer,
};
use crate::expr::inspect::subtree_is_unsafe_to_push;
use crate::expr::pg::{PgBoolExpr, PgExprRef};

pub(crate) struct ConservativeCandidate {
    pub(crate) filter: NormalizedPredicate,
    pub(crate) is_widened: bool,
}

/// Build one safe candidate. `accepts` is the provider capability probe used
/// by relation negotiation; query planning passes all structurally normalized
/// leaves and lets the table-scan provider negotiate them independently.
pub(crate) unsafe fn conservative_candidate<E>(
    expression: *mut pg_sys::Expr,
    normalize: &mut impl FnMut(*mut pg_sys::Expr) -> Option<NormalizedPredicate>,
    accepts: &mut impl FnMut(&PredicateFragment) -> Result<bool, E>,
) -> Result<Option<ConservativeCandidate>, E> {
    if let Some(normalized) = normalize(expression)
        && accepts(normalized.fragment())?
    {
        return Ok(Some(ConservativeCandidate {
            filter: normalized,
            is_widened: false,
        }));
    }

    let Some(boolean) = (unsafe { bool_expr(expression) }) else {
        return Ok(None);
    };
    unsafe { candidate_from_boolean(boolean, normalize, accepts) }
}

unsafe fn candidate_from_boolean<E>(
    boolean: PgBoolExpr<'_>,
    normalize: &mut impl FnMut(*mut pg_sys::Expr) -> Option<NormalizedPredicate>,
    accepts: &mut impl FnMut(&PredicateFragment) -> Result<bool, E>,
) -> Result<Option<ConservativeCandidate>, E> {
    match boolean.boolop() {
        pg_sys::BoolExprType::AND_EXPR => {
            let children = bool_children(boolean);
            let mut candidates = Vec::new();
            for child in &children {
                if let Some(candidate) =
                    unsafe { conservative_candidate(*child, normalize, accepts) }?
                {
                    candidates.push(candidate);
                }
            }
            let is_widened = candidates.len() != children.len()
                || candidates.iter().any(|candidate| candidate.is_widened);
            Ok((unsafe {
                NormalizedPredicate::combine_and(
                    candidates
                        .into_iter()
                        .map(|candidate| candidate.filter)
                        .collect(),
                )
            })
            .map(|filter| ConservativeCandidate { filter, is_widened }))
        }
        pg_sys::BoolExprType::OR_EXPR => {
            let mut candidates = Vec::new();
            for child in bool_children(boolean) {
                let Some(candidate) =
                    (unsafe { conservative_candidate(child, normalize, accepts) })?
                else {
                    return Ok(None);
                };
                candidates.push(candidate);
            }
            let is_widened = candidates.iter().any(|candidate| candidate.is_widened);
            Ok((unsafe {
                NormalizedPredicate::combine_or(
                    candidates
                        .into_iter()
                        .map(|candidate| candidate.filter)
                        .collect(),
                )
            })
            .map(|filter| ConservativeCandidate { filter, is_widened }))
        }
        pg_sys::BoolExprType::NOT_EXPR => Ok(None),
        _ => Ok(None),
    }
}

/// One provider-approved conservative pruning plan for a query scan.
pub struct QueryPruningPlan<P> {
    normalized: NormalizedPredicate,
    planned: P,
}

impl<P> QueryPruningPlan<P> {
    pub fn into_parts(self) -> (NormalizedPredicate, P) {
        (self.normalized, self.planned)
    }
}

/// Query-side façade applying the same provider-aware AND extraction and OR
/// widening used by relation scans. Query execution always retains the exact
/// predicate, so every accepted result is conservative pruning regardless of
/// the provider's row-filter contract.
pub struct QueryPruningPlanner<'a> {
    normalizer: &'a QueryExpressionNormalizer,
}

impl<'a> QueryPruningPlanner<'a> {
    pub fn new(normalizer: &'a QueryExpressionNormalizer) -> Self {
        Self { normalizer }
    }

    /// # Safety
    ///
    /// `expression` must be a live planner-owned expression node or implicit-AND
    /// list for the relation scope used to construct `self`.
    pub unsafe fn negotiate<P: FilterPushdownPlanner>(
        &self,
        expression: *mut pg_sys::Node,
        planner: &mut P,
    ) -> Result<Option<QueryPruningPlan<P::PlannedPredicate>>, P::Error> {
        let is_implicit_and =
            unsafe { (*expression).type_ } == pg_sys::NodeTag::T_List;
        if !is_implicit_and && unsafe { subtree_is_unsafe_to_push(expression.cast()) }
        {
            return Ok(None);
        }
        if let Some(normalized) =
            unsafe { self.normalizer.normalize_predicate(expression) }
        {
            match planner.try_plan_filter(normalized.fragment())? {
                FilterPlan::Exact(planned) | FilterPlan::Conservative(planned) => {
                    return Ok(Some(QueryPruningPlan {
                        normalized,
                        planned: planned.predicate,
                    }));
                }
                FilterPlan::Unsupported => {}
            }
        }

        let candidate = {
            let mut normalize = |node: *mut pg_sys::Expr| unsafe {
                self.normalizer.normalize_predicate(node.cast())
            };
            let mut accepts = |fragment: &PredicateFragment| {
                planner
                    .try_plan_filter(fragment)
                    .map(|plan| !matches!(plan, FilterPlan::Unsupported))
            };
            if is_implicit_and {
                let list = expression.cast::<pg_sys::List>();
                let count = unsafe { pg_sys::list_length(list) };
                let mut candidates = Vec::new();
                for index in 0..count {
                    let child = unsafe { pg_sys::list_nth(list, index) }.cast();
                    if unsafe { subtree_is_unsafe_to_push(child) } {
                        continue;
                    }
                    if let Some(candidate) = unsafe {
                        conservative_candidate(child, &mut normalize, &mut accepts)
                    }? {
                        candidates.push(candidate);
                    }
                }
                let is_widened = candidates.len() != count as usize
                    || candidates.iter().any(|candidate| candidate.is_widened);
                (unsafe {
                    NormalizedPredicate::combine_and(
                        candidates
                            .into_iter()
                            .map(|candidate| candidate.filter)
                            .collect(),
                    )
                })
                .map(|filter| ConservativeCandidate { filter, is_widened })
            } else {
                let Some(boolean) = (unsafe { bool_expr(expression.cast()) }) else {
                    return Ok(None);
                };
                unsafe {
                    candidate_from_boolean(boolean, &mut normalize, &mut accepts)
                }?
            }
        };
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let planned = match planner.try_plan_filter(candidate.filter.fragment())? {
            FilterPlan::Exact(planned) | FilterPlan::Conservative(planned) => {
                planned.predicate
            }
            FilterPlan::Unsupported => return Ok(None),
        };
        Ok(Some(QueryPruningPlan {
            normalized: candidate.filter,
            planned,
        }))
    }
}

/// # Safety
/// `expression` must remain a live planner-owned expression.
pub(crate) unsafe fn bool_expr<'a>(
    expression: *mut pg_sys::Expr,
) -> Option<PgBoolExpr<'a>> {
    let expression = unsafe { PgExprRef::from_raw_opt(expression) }?;
    PgBoolExpr::try_from_expr(expression.without_relabels())
}

pub(crate) fn bool_children(boolean: PgBoolExpr<'_>) -> Vec<*mut pg_sys::Expr> {
    let arguments = boolean.args_list();
    let count = unsafe { pg_sys::list_length(arguments) };
    (0..count)
        .map(|index| unsafe { pg_sys::list_nth(arguments, index) }.cast())
        .collect()
}
