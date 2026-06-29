//! DML row-delta conflict scope construction.
//!
//! Spark builds the RowDelta conflict filter from the scan's accepted Iceberg
//! filter expressions and only enables added-data-file validation at Iceberg
//! serializable isolation. PostgreSQL table-AM scans do not receive normal
//! `WHERE` clauses as `ScanKey`s, so this resolver obtains the live target
//! scan's `plan->qual` through the DML frame and runs it through the same
//! classification/translation path used by CustomScan pushdown.
//!
//! Only static target predicates can narrow validation. A MERGE join against a
//! dynamic source cannot be represented as a bounded Iceberg predicate without
//! collecting source keys, which would make commit memory and validation cost
//! grow with source cardinality. Such plans deliberately resolve to
//! [`ConflictValidationScope::WholeTable`]. This is conservative serializable
//! validation, not primary-key or uniqueness enforcement.

use iceberg_lite::expr::Predicate;
use pg_lakebase_core::access::dml::{DmlTargetPlan, with_current_dml_target_plan};
use pg_lakebase_core::expr::DmlConflictPredicateBuilder;
use pg_lakebase_core::expr::nodes::PgParamValue;
use pg_lakebase_core::expr::predicate::PlanPredicate;
use pg_lakebase_core::expr::split::QualPushdownDecision;
use pgrx::pg_sys;

use crate::predicate::{IcebergPredicateClassifier, IcebergPredicateTranslator};

/// Scope against which concurrently added Iceberg files are validated.
#[derive(Debug)]
pub(crate) enum ConflictValidationScope {
    /// A static target predicate safely narrows the affected table region.
    StaticTarget(Predicate),
    /// No safe static target predicate exists; validate the whole table.
    WholeTable,
}

impl ConflictValidationScope {
    fn static_target(predicate: Predicate) -> Self {
        if predicate == Predicate::AlwaysTrue {
            Self::WholeTable
        } else {
            Self::StaticTarget(predicate)
        }
    }

    pub(crate) fn into_predicate(self) -> Predicate {
        match self {
            Self::StaticTarget(predicate) => predicate,
            Self::WholeTable => Predicate::AlwaysTrue,
        }
    }
}

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

    pub(crate) fn resolve(self) -> ConflictValidationScope {
        with_current_dml_target_plan(self.rel_oid, |target_plan| {
            self.resolve_target(target_plan)
        })
        .unwrap_or(ConflictValidationScope::WholeTable)
    }

    fn resolve_target(
        &self,
        target_plan: DmlTargetPlan<'_>,
    ) -> ConflictValidationScope {
        let classifier = IcebergPredicateClassifier::for_conflict_detection();
        let mut classify_leaf = |predicate: &PlanPredicate| -> QualPushdownDecision {
            classifier.classify(predicate)
        };
        let Some(mut builder) =
            DmlConflictPredicateBuilder::new(target_plan, &mut classify_leaf)
        else {
            return ConflictValidationScope::WholeTable;
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
        let mut predicates = predicates.into_iter();
        let Some(first) = predicates.next() else {
            return ConflictValidationScope::WholeTable;
        };
        ConflictValidationScope::static_target(predicates.fold(first, Predicate::and))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn true_static_predicate_is_whole_table_scope() {
        assert!(matches!(
            ConflictValidationScope::static_target(Predicate::AlwaysTrue),
            ConflictValidationScope::WholeTable
        ));
    }

    #[test]
    fn narrowing_static_predicate_remains_explicit() {
        assert!(matches!(
            ConflictValidationScope::static_target(Predicate::AlwaysFalse),
            ConflictValidationScope::StaticTarget(Predicate::AlwaysFalse)
        ));
    }
}
