//! Backend coverage for PostgreSQL-owned expression safety inspection.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pg_lakebase_core::expr::QualPushdownDecision;
    use pg_lakebase_core::expr::classify::{ClauseClassification, ClauseClassifier};
    use pg_lakebase_core::expr::predicate::{PlanPredicate, PlanPredicateContext};
    use pgrx::{pg_sys, pg_test};

    #[pg_test]
    fn contain_subplans_rejects_every_pg_subplan_representation() {
        unsafe {
            let context = PlanPredicateContext {
                rel_oid: pg_sys::Oid::INVALID,
                scan_relid: 1,
            };
            let mut classify_leaf = |_: &PlanPredicate| -> QualPushdownDecision {
                panic!("subplan safety gate must run before leaf classification")
            };
            let mut classifier = ClauseClassifier::new(&context, &mut classify_leaf);

            for (tag, size) in [
                (
                    pg_sys::NodeTag::T_SubPlan,
                    core::mem::size_of::<pg_sys::SubPlan>(),
                ),
                (
                    pg_sys::NodeTag::T_AlternativeSubPlan,
                    core::mem::size_of::<pg_sys::AlternativeSubPlan>(),
                ),
                (
                    pg_sys::NodeTag::T_SubLink,
                    core::mem::size_of::<pg_sys::SubLink>(),
                ),
            ] {
                let node = pg_sys::palloc0(size).cast::<pg_sys::Node>();
                (*node).type_ = tag;
                let expr = node.cast::<pg_sys::Expr>();

                match classifier.classify(expr) {
                    ClauseClassification::Unsupported { residual } => {
                        assert_eq!(
                            residual.as_ptr(),
                            expr,
                            "{tag:?} must preserve its residual",
                        );
                    }
                    other => panic!("{tag:?} must be rejected, got {other:?}"),
                }

                pg_sys::pfree(node.cast());
            }
        }
    }
}
