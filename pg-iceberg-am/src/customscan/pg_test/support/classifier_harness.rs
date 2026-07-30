//! Shared classifier observation harness for arbitrary test expressions.

use core::ffi::c_int;

use pg_lakebase_core::expr::QualPushdownDecision;
use pg_lakebase_core::expr::pg::PgExprRef;
use pg_lakebase_core::expr::predicate::PlanPredicateContext;
use pgrx::pg_sys;

use crate::customscan::pg_test::support::fixtures::SCAN_RELID;
use crate::predicate::IcebergPredicateClassifier;

/// Stateless backend harness for running the AM classifier over synthetic PG nodes.
pub(crate) struct ClassifierHarness;

pub(crate) const CLASSIFIER: ClassifierHarness = ClassifierHarness;

impl ClassifierHarness {
    /// Classify `expr` via core `parse_leaf` + Iceberg classifier dispatch.
    ///
    /// # Safety
    /// Traverses the raw PostgreSQL node pointed to by `expr`.
    pub(crate) unsafe fn classify(
        &self,
        expr: *mut pg_sys::Expr,
    ) -> QualPushdownDecision {
        unsafe {
            let predicate_ctx = PlanPredicateContext {
                rel_oid: pg_sys::Oid::INVALID,
                scan_relid: SCAN_RELID as c_int,
            };
            let leaf = PgExprRef::from_raw(expr);
            let predicate = match predicate_ctx.parse_leaf(leaf) {
                Ok(p) => p,
                Err(_) => return QualPushdownDecision::Unsupported,
            };
            IcebergPredicateClassifier.classify(&predicate)
        }
    }
}
