//! DML row-delta conflict filter construction.
//!
//! Spark builds the RowDelta conflict filter from the scan's accepted Iceberg
//! filter expressions and only enables added-data-file validation at Iceberg
//! serializable isolation. PostgreSQL table-AM scans do not receive normal
//! `WHERE` clauses as `ScanKey`s, so this resolver obtains the live target
//! scan's `plan->qual` through the DML frame and runs it through the same
//! classification/translation path used by CustomScan pushdown.

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::access::dml::{DmlTargetPlan, with_current_dml_target_plan};
use pg_lakebase_core::expr::DmlConflictPredicateBuilder;
use pg_lakebase_core::expr::nodes::PgParamValue;
use pg_lakebase_core::expr::predicate::PlanPredicate;
use pg_lakebase_core::expr::split::QualPushdownDecision;
use pgrx::pg_sys;

use crate::predicate::{IcebergPredicateClassifier, IcebergPredicateTranslator};

/// Resolves the safest available Iceberg conflict filter for one DML target.
#[derive(Debug)]
pub(crate) struct DmlConflictFilterResolver {
    rel_oid: pg_sys::Oid,
}

impl DmlConflictFilterResolver {
    #[inline]
    pub(crate) fn new(rel_oid: pg_sys::Oid) -> Self {
        Self { rel_oid }
    }

    pub(crate) fn resolve(self) -> Predicate {
        with_current_dml_target_plan(self.rel_oid, |target_plan| {
            self.resolve_target(target_plan)
        })
        .unwrap_or(Predicate::AlwaysTrue)
    }

    fn resolve_target(&self, target_plan: DmlTargetPlan<'_>) -> Predicate {
        let classifier = IcebergPredicateClassifier::for_conflict_detection();
        let mut classify_leaf = |predicate: &PlanPredicate| -> QualPushdownDecision {
            classifier.classify(predicate)
        };
        let Some(mut builder) =
            DmlConflictPredicateBuilder::new(target_plan, &mut classify_leaf)
        else {
            return Predicate::AlwaysTrue;
        };
        // ConflictDetection classification rejects every dynamic operand, so
        // translation never relies on a missing-parameter error for safety.
        let resolved_params: [PgParamValue; 0] = [];
        let mut translator = IcebergPredicateTranslator::new();
        // SAFETY: `target_plan` is scoped by with_current_dml_target_plan;
        // build_predicates keeps every PostgreSQL expression pointer internal
        // and returns only provider-owned predicates.
        let predicates =
            unsafe { builder.build_predicates(&mut translator, &resolved_params) };
        predicates
            .into_iter()
            .fold(Predicate::AlwaysTrue, Predicate::and)
    }
}
