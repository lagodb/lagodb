//! Shared conservative-candidate construction for relation and query scans.

use pgrx::pg_sys;

use super::super::{
    FilterPlan, FilterPushdownPlanner, NormalizedPredicate, PredicateFragment,
    QueryExpressionNormalizer,
};
use crate::expr::PushdownCosting;
use crate::expr::inspect::subtree_is_unsafe_to_push;
use crate::expr::pg::{PgBoolExpr, PgExprRef};

pub(crate) struct ConservativeCandidate {
    pub(crate) filter: NormalizedPredicate,
    pub(crate) is_widened: bool,
}

/// Build one safe candidate. `accepts` is the provider capability probe used
/// by relation negotiation; query planning additionally rejects lifecycle-
/// unstable leaves before the table-scan provider negotiates them.
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
    costing: PushdownCosting,
}

impl<P> QueryPruningPlan<P> {
    pub fn into_parts(self) -> (NormalizedPredicate, P, PushdownCosting) {
        (self.normalized, self.planned, self.costing)
    }
}

/// Query-side façade applying the same provider-aware AND extraction and OR
/// widening used by relation scans. Only statement-stable fragments can enter
/// the bound-scan task cache. Query execution always retains the exact
/// predicate, so every accepted result is conservative pruning regardless of
/// the provider's row-filter contract.
pub struct QueryPruningPlanner<'a> {
    normalizer: &'a QueryExpressionNormalizer,
}

impl<'a> QueryPruningPlanner<'a> {
    pub fn new(normalizer: &'a QueryExpressionNormalizer) -> Self {
        Self { normalizer }
    }

    fn is_rescan_stable(fragment: &PredicateFragment) -> bool {
        fragment
            .values()
            .iter()
            .all(|value| value.source_kind.is_rescan_stable())
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
            && Self::is_rescan_stable(normalized.fragment())
        {
            match planner.try_plan_filter(normalized.fragment())? {
                FilterPlan::Exact(planned) | FilterPlan::Conservative(planned) => {
                    return Ok(Some(QueryPruningPlan {
                        normalized,
                        planned: planned.predicate,
                        costing: planned.costing,
                    }));
                }
                FilterPlan::Partial(_) | FilterPlan::Unsupported => {}
            }
        }

        let candidate = {
            let mut normalize = |node: *mut pg_sys::Expr| unsafe {
                self.normalizer.normalize_predicate(node.cast())
            };
            let mut accepts = |fragment: &PredicateFragment| {
                if !Self::is_rescan_stable(fragment) {
                    return Ok(false);
                }
                planner.try_plan_filter(fragment).map(|plan| {
                    !matches!(plan, FilterPlan::Partial(_) | FilterPlan::Unsupported)
                })
            };
            let source_conjuncts = unsafe { Self::source_conjuncts(expression) };
            let source_conjunct_count = source_conjuncts.len();
            let mut candidates = Vec::new();
            for child in source_conjuncts {
                if unsafe { subtree_is_unsafe_to_push(child) } {
                    continue;
                }
                if let Some(candidate) = unsafe {
                    conservative_candidate(child, &mut normalize, &mut accepts)
                }? {
                    candidates.push(candidate);
                }
            }
            let is_widened = candidates.len() != source_conjunct_count
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
        };
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let (planned, costing) =
            match planner.try_plan_filter(candidate.filter.fragment())? {
                FilterPlan::Exact(planned) | FilterPlan::Conservative(planned) => {
                    (planned.predicate, planned.costing)
                }
                FilterPlan::Partial(_) | FilterPlan::Unsupported => return Ok(None),
            };
        Ok(Some(QueryPruningPlan {
            normalized: candidate.filter,
            planned,
            costing,
        }))
    }

    /// Flatten the same top-level conjunction that DataFusion presents to
    /// `supports_filters_pushdown`. PostgreSQL normally supplies an implicit-AND
    /// restriction list, while a direct Boolean AND can occur in nested query
    /// shapes.
    unsafe fn source_conjuncts(
        expression: *mut pg_sys::Node,
    ) -> Vec<*mut pg_sys::Expr> {
        if unsafe { (*expression).type_ } == pg_sys::NodeTag::T_List {
            let list = expression.cast::<pg_sys::List>();
            let count = unsafe { pg_sys::list_length(list) };
            let mut conjuncts = Vec::with_capacity(count as usize);
            for index in 0..count {
                let child = unsafe { pg_sys::list_nth(list, index) }.cast();
                unsafe { Self::append_source_conjuncts(child, &mut conjuncts) };
            }
            return conjuncts;
        }
        let mut conjuncts = Vec::new();
        unsafe { Self::append_source_conjuncts(expression.cast(), &mut conjuncts) };
        conjuncts
    }

    unsafe fn append_source_conjuncts(
        expression: *mut pg_sys::Expr,
        conjuncts: &mut Vec<*mut pg_sys::Expr>,
    ) {
        let Some(boolean) = (unsafe { bool_expr(expression) }) else {
            conjuncts.push(expression);
            return;
        };
        if boolean.boolop() != pg_sys::BoolExprType::AND_EXPR {
            conjuncts.push(expression);
            return;
        }
        for child in bool_children(boolean) {
            unsafe { Self::append_source_conjuncts(child, conjuncts) };
        }
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
