//! Runtime DML conflict-filter predicate extraction.
//!
//! DML target scans do not receive PostgreSQL `ScanKey`s for plain `WHERE`
//! clauses, so table-AM code has to read the executor plan's target scan qual
//! list. This module intentionally mirrors the CustomScan splitter's semantics:
//! classify PG quals through the same provider leaf policy, keep only safe
//! pushed fragments, and drop anything unsupported. Dropping a conjunct widens
//! the conflict filter, which is safe for row-delta validation.

use pgrx::pg_sys;

use crate::access::dml::DmlTargetPlan;
use crate::expr::nodes::PgParamValue;
use crate::expr::predicate::PlanPredicate;
use crate::expr::relation::{ColumnRefCollector, PlanScanRelation};
use crate::expr::split::QualPushdownDecision;
use crate::expr::translator::{PgPredicateTranslator, PredicateBuilder};
use crate::expr::walker::{
    ClassifiedPushedPart, ClauseClassification, ClauseClassifier, rewrite_not,
    subtree_is_unsafe_to_push,
};

/// Builds provider-owned conflict predicates from a target scan `plan->qual`.
pub struct DmlConflictPredicateBuilder<'frame, 'classifier, F> {
    target_plan: DmlTargetPlan<'frame>,
    relation: PlanScanRelation,
    classify_leaf: &'classifier mut F,
}

impl<'frame, 'classifier, F> DmlConflictPredicateBuilder<'frame, 'classifier, F>
where
    F: FnMut(&PlanPredicate<'_>) -> QualPushdownDecision,
{
    #[inline]
    pub fn new(
        target_plan: DmlTargetPlan<'frame>,
        classify_leaf: &'classifier mut F,
    ) -> Option<Self> {
        let relation = PlanScanRelation::from_runtime(
            target_plan.rel_oid(),
            target_plan.scan_relid(),
        )?;
        Some(Self {
            target_plan,
            relation,
            classify_leaf,
        })
    }

    /// Split and translate the target `Plan.qual` into provider-owned fragments.
    ///
    /// Unsupported clauses and translation failures are dropped. Because the
    /// top-level qual list is conjunctive, dropping a fragment can only widen
    /// the resulting conflict filter.
    ///
    /// # Safety
    ///
    /// The target plan must still belong to the active executor frame.
    /// PostgreSQL-owned expression pointers remain internal to this method and
    /// are never returned to the caller.
    pub unsafe fn build_predicates<T>(
        &mut self,
        translator: &mut T,
        resolved_params: &[PgParamValue],
    ) -> Vec<T::Predicate>
    where
        T: PgPredicateTranslator,
    {
        let scan_relid = self.relation.predicate_context().scan_relid;
        let predicate_ctx = self.relation.predicate_context();
        let mut classifier =
            ClauseClassifier::new(&predicate_ctx, &mut *self.classify_leaf);
        let mut accumulator = ConflictInputAccumulator::new();

        let qual = self.target_plan.qual();
        if qual.is_null() {
            return Vec::new();
        }
        let len = unsafe { pg_sys::list_length(qual) };
        for i in 0..len {
            let expr = unsafe { pg_sys::list_nth(qual, i) } as *mut pg_sys::Expr;
            if expr.is_null() {
                continue;
            }
            if unsafe { subtree_is_unsafe_to_push(expr) } {
                continue;
            }
            let rewritten = unsafe { rewrite_not(expr) };
            let classification = unsafe { classifier.classify(rewritten) };
            accumulator.absorb(classification);
        }

        let pushed_exprs = accumulator.finish();
        let column_refs = unsafe {
            ColumnRefCollector::new(self.relation)
                .collect_exprs(pushed_exprs.iter().copied())
        };
        let mut predicates = Vec::with_capacity(pushed_exprs.len());
        let mut builder = PredicateBuilder::new(
            translator,
            &pushed_exprs,
            &column_refs,
            resolved_params,
            scan_relid,
        );
        for index in 0..pushed_exprs.len() {
            if let Ok(predicate) = unsafe { builder.build_one(index) } {
                predicates.push(predicate);
            }
        }
        predicates
    }
}

struct ConflictInputAccumulator {
    pushed_exprs: Vec<*mut pg_sys::Expr>,
}

impl ConflictInputAccumulator {
    #[inline]
    fn new() -> Self {
        Self {
            pushed_exprs: Vec::new(),
        }
    }

    fn absorb(&mut self, classification: ClauseClassification) {
        match classification {
            ClauseClassification::Pushable { parts, .. } => {
                self.extend_parts(parts);
            }
            ClauseClassification::PartialPush { pushed, .. } => {
                self.pushed_exprs.push(pushed);
            }
            ClauseClassification::Unsupported { .. } => {}
        }
    }

    fn extend_parts(&mut self, parts: Vec<ClassifiedPushedPart>) {
        self.pushed_exprs
            .extend(parts.into_iter().map(|part| part.expr));
    }

    #[inline]
    fn finish(self) -> Vec<*mut pg_sys::Expr> {
        self.pushed_exprs
    }
}
